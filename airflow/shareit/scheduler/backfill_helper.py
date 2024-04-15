# -*- coding: utf-8 -*-
import json
import traceback
from collections import defaultdict

from airflow.configuration import conf

from airflow.shareit.utils.alarm_adapter import build_normal_backfill_notify_content, build_new_backfill_notify_content, \
    send_email_by_notifyAPI
from airflow.shareit.utils.state import State
from airflow.utils import timezone
from croniter import croniter_range

from airflow import LoggingMixin
from airflow.shareit.constant.trigger import TRI_BACKFILL
from airflow.shareit.models.backfill_job import BackfillJob
from airflow.shareit.models.event_depends_info import EventDependsInfo
from airflow.shareit.models.task_desc import TaskDesc
from airflow.shareit.models.trigger import Trigger
from airflow.shareit.service.task_service import TaskService
from airflow.shareit.utils.date_calculation_util import DateCalculationUtil
from airflow.shareit.utils.date_util import DateUtil
from airflow.shareit.workflow.workflow_state_sync import WorkflowStateSync
from airflow.utils.db import provide_session
from airflow.utils.timezone import utcnow, beijing, my_make_naive
from airflow.models.dag import DagModel

log = LoggingMixin().logger


class BackfillHelper(LoggingMixin):

    @provide_session
    def start_backfill_job(self, id, session=None):
        job = BackfillJob.find(id, session)
        if not job:
            log.warning('[BackfillJob] 找不到ID为 {} 的job'.format(id))
            BackfillJob.faild(id, session)
            return

        core_task = TaskDesc.get_task(task_name=job.core_task_name, session=session)
        if not core_task:
            log.warning('[BackfillJob] {} 任务不存在'.format(job.core_task_name))
            BackfillJob.faild(id, session)
            return

        task_desc_dict = {job.core_task_name: core_task}
        task_name_set = set(job.sub_task_name_list)
        task_name_set_copy = task_name_set.copy()
        instance_list = []
        trigger_list = []

        alert_dict = defaultdict(list)
        # 如果crontab不存在或者是 0 0 0 0 0 一律按照0 0 * * *处理
        core_task_crontab = core_task.crontab if core_task.crontab and core_task.crontab != '0 0 0 0 0' else '0 0 * * *'
        # coretask的所有待补实例日期
        times = croniter_range(job.start_date, job.end_date, core_task_crontab)

        if not times:
            log.warning('[BackfillJob] {task} {meta}:{et}'.format(task=job.core_task_name,
                                                                  meta="任务补数失败", et="输入的时间段应至少包含一个运行周期"))
            return

        for t in task_name_set_copy:
            td = TaskDesc.get_task(task_name=t, session=session)
            if td:
                task_desc_dict[t] = td
            else:
                # 任务不存在就移除
                task_name_set.remove(t)
        task_name_set.add(job.core_task_name)

        for exe_time in times:
            if DateUtil.date_compare(exe_time, utcnow(), eq=False):
                continue
            instance_list.append({"dag_id": job.core_task_name, "execution_date": exe_time})
            instance_list.extend(
                self._get_instance_list(job.core_task_name, task_name_set, exe_time, alert_dict, session=session))

        # 去重
        instance_list = [dict(t) for t in set([tuple(sorted(d.items())) for d in instance_list])]
        # 事务的
        try:
            # 处理上游依赖数据、下游生成数据状态
            for ins in instance_list:
                if DateUtil.date_compare(ins["execution_date"], utcnow(), eq=False):
                    continue
                trigger_list.append((ins["dag_id"], ins["execution_date"]))
                self._kill_running_ti_and_open_wi(ins["dag_id"], task_desc_dict, ins["execution_date"], session)

            instance_dict = defaultdict(list)
            for item in trigger_list:
                # 只有root节点可以忽略检查上游，否则深度补数没有意义
                ignore = False
                if item[0] == job.core_task_name:
                    ignore = job.ignore_check
                Trigger.sync_trigger_to_db(dag_id=item[0], execution_date=item[1], state=State.BF_WAIT_DEP, priority=5,
                                           trigger_type=TRI_BACKFILL, editor="DataPipeline_backfill", is_executed=True,
                                           start_check_time=timezone.utcnow(), ignore_check=ignore, transaction=True,
                                           backfill_label=job.label, session=session)
                instance_dict[item[0]].append(item[1].strftime("%Y-%m-%d %H:%M:%S"))

            BackfillJob.start(job.id, json.dumps(instance_dict), transaction=True, session=session)
            session.commit()
        except Exception:
            log.error(traceback.format_exc())
            session.rollback()
            raise
        # 发送补数消息
        if job.is_notify:
            self._notify(job.operator,core_task.ds_task_name,alert_dict)

    def _kill_running_ti_and_open_wi(self, task_name, task_desc_dict, execution_date, session):

        # 将实例都记为失败，也是为了kill掉正在补数的任务
        TaskService._set_task_instance_state(dag_id=task_name, execution_date=execution_date.replace(tzinfo=beijing),
                                             state=State.FAILED, transaction=True, session=session)
        execution_date = my_make_naive(execution_date)

        if task_desc_dict[task_name].workflow_name:
            # 打开任务归属的workflow
            WorkflowStateSync.open_wi(task_desc_dict[task_name].workflow_name, execution_date,
                                      transaction=True, session=session)

    def _get_instance_list(self, core_task_name, task_name_set, execution_date, alert_dict, session):
        instance_list = []
        work_queue = [(core_task_name,execution_date)]
        processed_set = {core_task_name}

        while work_queue:
            task,exe_date = work_queue.pop(0)
            res = self._get_down_stream_task_instance(core_task=task, execution_date=exe_date,
                                                      alert_dict=alert_dict, session=session)

            for ins in res:
                if ins["dag_id"] in task_name_set and ins["dag_id"] not in processed_set:
                    instance_list.append(ins)
                    processed_set.add(ins["dag_id"])
                    work_queue.append((ins["dag_id"],ins["execution_date"]))

            if len(processed_set) >= len(task_name_set):
                break

        return instance_list

    def _get_down_stream_task_instance(self,core_task, alert_dict, execution_date=None, session=None):
        # 判断任务是否是上线状态

        """
        获取当前实例的直接依赖实例或者直接下游实例
        """
        instances = []
        # 获取事件(任务成功)依赖的下游实例
        td = TaskDesc.get_task(task_name=core_task, session=session)
        e_depends = EventDependsInfo.get_downstream_event_depends(ds_task_id=td.ds_task_id, tenant_id=td.tenant_id,
                                                                  session=session)
        for e_depend in e_depends:
            downstream_dag_id = e_depend.dag_id
            if not DagModel.is_active_and_on(dag_id=downstream_dag_id, session=session):
                continue

            down_td = TaskDesc.get_task(task_name=downstream_dag_id, session=session)

            downstream_exe_dates = DateCalculationUtil.get_downstream_all_execution_dates(
                execution_date=execution_date,
                calculation_param=e_depend.date_calculation_param,
                downstream_task=downstream_dag_id,
                downstream_gra=down_td.granularity,
                downstream_crontab=down_td.crontab
            )

            if downstream_exe_dates is None or len(downstream_exe_dates) == 0:
                continue
            else:
                for exe_date in downstream_exe_dates:
                    # 由于从上游日期计算下游日期，在跨周期时，只能计算出一个粗略的日期，所以需要用下游日期再反推一下上游日期，判断到底需不需要
                    if self._is_execution_date_included(execution_date=execution_date,
                                                        calculation_param=e_depend.date_calculation_param,
                                                        up_crontab=td.crontab,
                                                        up_gra=td.granularity,
                                                        up_task_name=td.task_name,
                                                        cur_exe_date=exe_date):
                        instances.append({"dag_id": downstream_dag_id, "execution_date": exe_date})
                        # 记录补数消息，用于后续的通知
                        alert_dict[down_td.owner].append('{}_{}'.format(down_td.ds_task_name,exe_date.strftime('%Y-%m-%d %H:%M:%S')))

        return instances

    def _is_execution_date_included(self, execution_date, calculation_param, up_gra, cur_exe_date,up_task_name,up_crontab):
        '''
        判断某个日期的上游是否包含execution_date
        '''
        all_dates = DateCalculationUtil.get_upstream_all_execution_dates(execution_date=cur_exe_date,
                                                                         calculation_param=calculation_param,
                                                                         upstream_task=up_task_name,
                                                                         upstream_gra=up_gra,
                                                                         upstream_crontab=up_crontab)
        for dt in all_dates:
            if DateUtil.date_equals(execution_date, dt):
                return True

        return False

    def _notify(self,operator,core_ds_task_name,alert_dict):
        suffix = conf.get('email','email_suffix')
        # 发送消息
        for owner,ti_list in alert_dict.items():
            reciever = owner.strip()+suffix
            try:
                content = build_new_backfill_notify_content(task_name=core_ds_task_name,operator=operator,
                                                            tis='\n'.join(sorted(list(set(ti_list)))))
                send_email_by_notifyAPI(title='DataCake 补数通知',content=content.decode('utf-8').replace('\n', '<br>'), receiver=reciever)
            except Exception:
                log.error(traceback.format_exc())
                log.error('send backfill notify failed')
                continue
