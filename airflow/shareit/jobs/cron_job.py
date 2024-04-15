# -*- coding: utf-8 -*-
import multiprocessing
import traceback

from airflow.exceptions import AirflowTaskTimeout

from airflow.shareit.monitor.push_monitor import MonitorPush
from airflow.utils.timeout import timeout

from airflow.models.dag import DagModel

import time
import pendulum
from datetime import datetime,timedelta

from airflow.shareit.constant.taskInstance import PIPELINE
from airflow.shareit.models.dag_dataset_relation import DagDatasetRelation
from airflow.shareit.models.event_depends_info import EventDependsInfo
from airflow.shareit.models.trigger import Trigger
from airflow.shareit.models.workflow import Workflow
from airflow.shareit.models.workflow_instance import WorkflowInstance
from airflow.shareit.scheduler.state_recovery_helper import StateRecoveryProccess
from airflow.shareit.trigger.trigger_helper import TriggerHelper
from airflow.shareit.utils.constant import Constant
from airflow.shareit.utils.date_util import DateUtil
from airflow.shareit.utils.state import State
from airflow.shareit.utils.task_manager import get_dag
from airflow.utils import timezone
from airflow import LoggingMixin
from airflow.shareit.models.task_desc import TaskDesc
from croniter import croniter,croniter_range
from airflow.configuration import conf

from airflow.utils.db import provide_session
from airflow.utils.timezone import beijing


class DsCronJob(LoggingMixin):
    def __init__(
            self,
            timeout=-1,
            check_parallelism=3,
            *args, **kwargs):
        self.timeout = timeout
        self.check_parallelism = check_parallelism

    def run(self):
        try:
            self._execute()
        except Exception:
            raise

    def _execute(self):
        execute_start_time = timezone.utcnow()
        # 状态同步进程，保证pod异常回收时，恢复taskinstance实例状态
        # StateRecoveryProccess().start()
        monitor_push = MonitorPush()
        # a= 1
        while True:
            loop_start_time = time.time()
            try:
                self.log.info("[DataStudio Pipeline] Cronjob: start cronjob loop")
                try:
                    with timeout(3):
                        monitor_push.push_to_prometheus(job_name="cronjob", gauge_name="cronjob",
                                                        documentation="cronjob")
                except AirflowTaskTimeout as e:
                    self.log.error("[DataStudio Pipeline] push metrics timeout")
                except Exception as e:
                    self.log.error("[DataStudio Pipeline] cronjob push metrics failed")
                all_workflow = Workflow.get_all_online_workflow()
                for workflow in all_workflow:
                    nowDate = timezone.utcnow().replace(tzinfo=beijing)
                    # 校验crontab是否正确
                    if not croniter.is_valid(workflow.crontab):
                        self.log.error(
                            "[DataStudio Pipeline] Cronjob: workflow '{}' have wrong crontab '{}'".format(workflow.workflow_id, workflow.crontab))
                        continue
                    last_instance = WorkflowInstance.get_last_instance(workflow.workflow_id,workflow.tenant_id)
                    if last_instance is not None:
                        last_execution_date = last_instance.execution_date.replace(tzinfo=beijing)
                        # 如果当前日期比最后一次触发日期小
                        if DateUtil.date_compare(last_execution_date,nowDate):
                            self.log.warning(
                                "[DataStudio Pipeline] Cronjob: workflow '{}' , last execution date '{}' is greater than now date '{}' ".format(
                                    str(workflow.workflow_id),
                                    last_execution_date.strftime('%Y-%m-%d %H:%M:%S'), nowDate.strftime('%Y-%m-%d %H:%M:%S')))
                            continue
                        # 获取上一次调度日期到当前日期中间的所有cron日期
                        all_cron_dates = croniter_range(last_execution_date + timedelta(seconds=1), nowDate, workflow.crontab)
                        # 为这些日期注册工作流实例
                        for cron_date in all_cron_dates:
                            WorkflowInstance.add_wi(workflow_id=workflow.workflow_id, execution_date=cron_date,
                                                    version=workflow.version,tenant_id=workflow.tenant_id)
                            self.log.info("[DataStudio Pipeline] Cronjob: workflow '{}', add instance '{}'"
                                    .format(workflow.workflow_name,cron_date.strftime('%Y-%m-%d %H:%M:%S')))
                    else :
                        # 如果没有上一次的trigger,则获取当前时间前一次的cron日期
                        cron = croniter(workflow.crontab, nowDate)
                        cron.tzinfo = beijing
                        execution_date = cron.get_prev(datetime)
                        WorkflowInstance.add_wi(workflow_id=workflow.workflow_id, execution_date=execution_date,
                                                version=workflow.version,tenant_id=workflow.tenant_id)

                # task的cronjob模块
                all_cron_tasks = TaskDesc.get_all_cron_task()

                for task in all_cron_tasks:
                    nowDate = timezone.utcnow()

                    # 校验任务是否还在上线状态
                    if not DagModel.is_active_and_on(task.task_name):
                        # self.log.info(
                        #     "[DataStudio Pipeline] Cronjob: task '{}' is offline".format(task.task_name))
                        continue
                    try:
                        filter_task = conf.get('core', 'filter_task')
                        filter_tasks = filter_task.split(',')
                        if task.task_name in filter_tasks:
                            continue
                    except Exception as e:
                        self.log.warning("core - filter_task not exists")


                    if task.crontab == '0 0 0 0 0':
                        continue
                    # 校验crontab是否正确
                    if not croniter.is_valid(task.crontab):
                        self.log.error(
                            "[DataStudio Pipeline] Cronjob: task '{}' have wrong crontab '{}'".format(task.task_name, task.crontab))
                        continue

                    # 在任务给定的开始时间之前不触发
                    if task.start_date is not None and DateUtil.date_compare(task.start_date, timezone.utcnow(), eq=False):
                        continue
                    # 在任务给定的结束时间之后的不触发
                    if task.end_date is not None and DateUtil.date_compare(timezone.utcnow(), task.end_date, eq=False):
                        continue
                    '''
                    为防止上一次的轮询久，导致延迟， 每次应该从最后一次时间点恢复
                    但考虑到下线过久然后重新上线的场景,这种情况下其实不需要从最后一次时间点恢复
                    解决方法: 我们在处理任务上线/更新时,就立马生成一个最近日期的trigger,这样就避免了上述问题,所以这里不用针对这个场景做处理
                    '''
                    # 找到上一次trigger的日期
                    last_trigger = Trigger.get_last_trigger_data(dag_id=task.task_name)

                    if last_trigger is not None :
                        last_trigger_date = last_trigger.execution_date
                        # 如果当前日期比最后一次触发日期小
                        if nowDate.strftime('%Y%m%d%H:%M:%S') < last_trigger_date.strftime('%Y%m%d%H:%M:%S'):
                            self.log.warning(
                                "[DataStudio Pipeline] Cronjob: task '{}' , last trigger date '{}' is greater than now date '{}' ".format(
                                    task.task_name,
                                    last_trigger_date.strftime('%Y-%m-%d %H:%M:%S'), nowDate.strftime('%Y-%m-%d %H:%M:%S')))
                            continue

                        nowDate = nowDate.replace(tzinfo=beijing)
                        last_trigger_date = last_trigger_date.replace(tzinfo=beijing)
                        # 获取上一次trigger日期到当前日期中间的所有cron日期
                        all_cron_dates = croniter_range(last_trigger_date + timedelta(seconds=1), nowDate, task.crontab)
                        all_tri_dates = []
                        # 为这些日期注册trigger
                        for cron_date in all_cron_dates:
                            Trigger.sync_trigger_to_db(dag_id=task.task_name, execution_date=cron_date,
                                                       state=State.WAITING,priority=5,start_check_time=cron_date,
                                                       trigger_type="cronTrigger", editor="CronTrigger")
                            all_tri_dates.append(cron_date)
                            self.log.info("[DataStudio Pipeline] Cronjob: task '{}', add trigger '{}'"
                                    .format(task.task_name,cron_date.strftime('%Y-%m-%d %H:%M:%S')))
                        self.add_task_instance(task_name=task.task_name,execution_dates=all_tri_dates)
                    else :
                        # 如果没有上一次的trigger,则获取当前时间前一次的cron日期
                        cron = croniter(task.crontab, nowDate)
                        cron.tzinfo = beijing
                        trigger_execution_date = cron.get_prev(datetime)
                        Trigger.sync_trigger_to_db(dag_id=task.task_name, execution_date=trigger_execution_date,
                                                   state=State.WAITING,priority=5,start_check_time=trigger_execution_date,
                                                   trigger_type="cronTrigger", editor="CronTrigger")
                        self.log.info("[DataStudio Pipeline] Cronjob: task '{}', add trigger '{}'"
                                      .format(task.task_name, trigger_execution_date.strftime('%Y-%m-%d %H:%M:%S')))
                        self.add_task_instance(task_name=task.task_name,execution_dates=[trigger_execution_date])
            # self.log.info("[DataStudio Pipeline] cron task main_scheduler_process execute...")
            # data_pipeline_scheduler_helper = CronScheduelrManager()
            # data_pipeline_scheduler_helper.main_scheduler_process()
            # self.log.info("[DataStudio Pipeline] cron task main_scheduler_process successfully")
            except Exception as e:
                self.log.error(traceback.format_exc())
                self.log.error("[DataStudio Pipeline] Cronjob error")
                continue
            loop_end_time = time.time()
            loop_duration = loop_end_time - loop_start_time
            if loop_duration < 10:
                sleep_length = 10 - loop_duration
                self.log.info(
                    "[DataStudio Pipeline] Cronjob: sleeping for {0:.2f} seconds to prevent excessive logging"
                        .format(sleep_length))
                time.sleep(sleep_length)

    def add_task_instance(self,task_name,execution_dates):
        '''
        如果任务没有上游，直接加入到调度队列中去（创建taskInstance实例）
        '''
        if len(execution_dates) == 0:
            return
        ddr_inputs = DagDatasetRelation.get_inputs(task_name)
        event_inputs = EventDependsInfo.get_dag_event_depends(task_name, type=Constant.TASK_SUCCESS_EVENT)
        if len(ddr_inputs) == 0 and len(event_inputs) == 0:
            self.log.info(
                "[DataStudio Pipeline] Cronjob: task {} has no upstream,join the execution queue"
                    .format(task_name))
            dag = get_dag(task_name)
            for execution_date in execution_dates:
                TriggerHelper.create_taskinstance(dag.task_dict,execution_date,PIPELINE)

