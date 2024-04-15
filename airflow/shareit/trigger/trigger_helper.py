# -*- coding: utf-8 -*-
import copy
import traceback
from datetime import datetime, timedelta
import json
import logging
import time
import pendulum
import requests
import six
from croniter import croniter, croniter_range

from airflow.configuration import conf
from airflow.shareit.constant.trigger import TRI_FORCE


from airflow.shareit.models.event_depends_info import EventDependsInfo
from airflow.shareit.models.gray_test import GrayTest
from airflow.shareit.models.sensor_info import SensorInfo
from airflow.shareit.models.trigger import Trigger
from airflow.shareit.utils.constant import Constant
from airflow.shareit.utils.date_calculation_util import DateCalculationUtil
from airflow.shareit.utils.normal_util import NormalUtil
from airflow.shareit.utils.task_cron_util import TaskCronUtil
from airflow.utils import timezone
from airflow.utils.db import provide_session
from airflow.utils.timezone import utcnow, beijing,my_make_naive
from airflow.utils.log.logging_mixin import LoggingMixin
# from airflow.shareit.models.dataset import Dataset

from airflow.shareit.models.dag_dataset_relation import DagDatasetRelation
from airflow.shareit.models.dataset_partation_status import DatasetPartitionStatus
from airflow.shareit.models.task_desc import TaskDesc
from airflow.shareit.models.user_action import UserAction
from airflow.shareit.service.dataset_service import DatasetService
from airflow.shareit.utils.granularity import Granularity
from airflow.shareit.utils.state import State
from airflow.shareit.utils.task_manager import get_dag
from airflow.shareit.utils.date_util import DateUtil
from airflow.shareit.constant.taskInstance import *

log=LoggingMixin()
class TriggerHelper(LoggingMixin):
    def __init__(self):
        pass


    @staticmethod
    @provide_session
    def generate_trigger(dag_id=None, execution_date=None,down_task_id=None,is_force=False, task_desc=None,transaction=False, session=None):
        '''
        触发现在分为数据触发/事件触发
        '''
        if task_desc is None:
            task_desc = TaskDesc.get_task(task_name=dag_id)
        # 补数任务或已经执行过的任务不再触发下游
        triggers = Trigger.find(dag_id=dag_id, execution_date=execution_date,session=session)
        if len(triggers) > 0 and (triggers[0].trigger_type == 'backCalculation' or triggers[0].is_executed == True) and is_force == False:
            return
        # 事件触发 ----------------------------------
        e_depends = EventDependsInfo.get_downstream_event_depends(ds_task_id=task_desc.ds_task_id,
                                                                  tenant_id=task_desc.tenant_id,
                                                                  down_task_id=down_task_id, session=session)
        TriggerHelper.event_generate_trigger(event_depends=e_depends, execution_date=execution_date,
                                             transaction=transaction, session=session)


    @staticmethod
    def data_generate_trigger(ddrs,execution_date,transaction=False, session=None):
        # 整个循环的内容是待触发的下游任务
        for ddr in ddrs:
            downstream_dag_id = ddr.dag_id
            # 判断这个下游是否可以被事件触发
            if not TriggerHelper.can_be_triggered_by_data(downstream_dag_id):
                continue

            # 拿到当前任务的产出信息, 目的是需要产出粒度,以及产出offset, 因为要从数据日期反推出实例日期
            # downstream_ddr = DagDatasetRelation.get_outputs(dag_id=downstream_dag_id)
            # if len(downstream_ddr) > 0 :
            #     downstream_output_gra = downstream_ddr[0].granularity
            #     downstream_output_offset = downstream_ddr[0].offset
            # else :
            #     continue

            # 获取要触发的下游日期
            # downstream_exec_dates = DateCalculationUtil.get_downstream_calculated_date(execution_date,e_depend.date_calculation_param,downstream_dag_id)
            downstream_exec_dates = DateCalculationUtil.get_downstream_all_execution_dates(execution_date=execution_date,
                                                                                         calculation_param=ddr.date_calculation_param,downstream_task=downstream_dag_id)

            if downstream_exec_dates is None or len(downstream_exec_dates) == 0:
                continue
            else:
                for exec_date in downstream_exec_dates:
                    # # todo 这里不能反推出crontab表达式调度任务的日期,待做
                    # # 从数据日期反推出实例日期
                    # exe_date = Granularity.getNextTime(data_date,downstream_output_gra,downstream_output_offset)
                    # 生成trigger
                    Trigger.sync_trigger_to_db(dag_id=downstream_dag_id, execution_date=exec_date,
                                               state=State.WAITING,
                                               trigger_type="pipeline", editor="DataPipeline", priority=5,
                                               transaction=transaction,
                                               session=session)
                    print '调度数据触发: {},日期{}'.format(downstream_dag_id,exec_date.strftime("%Y%m%d %H"))
        return



    @staticmethod
    def old_data_generate_trigger(output_dataset,execution_date,transaction=False, session=None):
        '''
        数据触发下游
        '''
        output_offset = output_dataset.offset if output_dataset.offset else -1
        ddrs = DagDatasetRelation.get_downstream_dags(metadata_id=output_dataset.metadata_id)
        for ddr in ddrs:
            task = TaskDesc.get_task(task_name=ddr.dag_id)
            # 判断是否可以被数据触发
            if not TriggerHelper.can_be_triggered_by_data(ddr.dag_id):
                continue
            # 当下游任务的粒度和对应下游任务的input粒度对应时才生成trigger
            if Granularity.formatGranularity(ddr.granularity) == Granularity.formatGranularity(
                    task.get_granularity()):
                if output_dataset.granularity == ddr.granularity:
                    # offset 可为正或负
                    exe_time = Granularity.getNextTime(execution_date, ddr.granularity, ddr.offset - output_offset)

                elif output_dataset.granularity > ddr.granularity:
                    # 判断 当前execution_date 是小粒度的最后一次运行
                    data_date = Granularity.getPreTime(execution_date, output_dataset.granularity, output_offset)
                    target_time = Granularity.getNextTime(data_date, ddr.granularity, ddr.offset)
                    exe_time = Granularity.get_execution_date(target_time, ddr.granularity)
                    if ddr.detailed_dependency is not None and ddr.detailed_gra is not None:
                        dependency_sign = Granularity.getPreTime(exe_time, ddr.granularity, ddr.offset)
                        last_dependency_time = ddr.get_last_detailed_dependency(gra=output_dataset.granularity,
                                                                                data_partation_sign=dependency_sign)
                        if last_dependency_time is False or  last_dependency_time.strftime('%Y-%m-%d %H:%M:%S') != data_date.strftime('%Y-%m-%d %H:%M:%S'):
                            continue
                    else:
                        t2 = Granularity.getPreTime(exe_time, output_dataset.granularity, -1)
                        if t2.strftime('%Y-%m-%d %H:%M:%S') != data_date.strftime('%Y-%m-%d %H:%M:%S'):
                            continue
                else:
                    raise ValueError("[DataPipeline] Error dependency: can not depend on larger granular dataset")
            else:
                continue
            # 计算 trigger
            Trigger.sync_trigger_to_db(dag_id=ddr.dag_id, execution_date=exe_time, state=State.WAITING,
                                       trigger_type="pipeline", editor="DataPipeline", transaction=transaction,
                                       session=session)

        return


    @staticmethod
    def event_generate_trigger(event_depends,execution_date, transaction=False, session=None):
        '''
        事件触发下游
        '''
        for e_depend in event_depends:
            # 获取到下游的dag_id
            downstream_dag_id = e_depend.dag_id
            # 判断这个下游是否可以被事件触发
            if not TriggerHelper.can_be_triggered_by_event(downstream_dag_id,e_depend.depend_gra):
                continue
            # 获取要触发的下游日期
            # downstream_exe_date = DateCalculationUtil.get_downstream_calculated_date(execution_date,e_depend.date_calculation_param,downstream_dag_id)
            downstream_exe_dates = DateCalculationUtil.get_downstream_all_execution_dates(execution_date=execution_date,calculation_param=e_depend.date_calculation_param,downstream_task=downstream_dag_id)

            if downstream_exe_dates is None or len(downstream_exe_dates) == 0:
                continue
            else :
                for exe_date in downstream_exe_dates:
                 # 生成trigger
                 Trigger.sync_trigger_to_db(dag_id=downstream_dag_id, execution_date=exe_date, state=State.WAITING,
                                            trigger_type="pipeline", editor="DataPipeline", priority=5,
                                            transaction=transaction,
                                            session=session)
                 print '调度事件触发: 下游{},日期{}'.format(downstream_dag_id, exe_date.strftime("%Y%m%d %H"))
        return

    @staticmethod
    def can_be_triggered_by_event(dag_id,depend_event_gra):
        '''
        判断某个任务是否可以被事件触发
        条件1: 任务必须是上线状态
        条件2: 任务不能是crontab时间调度的任务
        条件3: 任务只有事件依赖,那么他可以被事件依赖触发(目前是这种情况,以后可能会修改)
        条件4: 任务依赖上游的粒度小于等于其本身的粒度,可以被事件触发
        '''
        from airflow.models.dag import DagModel
        # 判断任务是否是上线状态
        if not DagModel.is_active_and_on(dag_id=dag_id):
            return False

        # crontab时间调度的任务由crontab去触发
        if TaskDesc.check_is_crontab_task(task_name=dag_id):
            return False

        # # 判断有无数据依赖上游
        # inputs = DagDatasetRelation.get_inputs(dag_id=dag_id)
        # if len(inputs) > 0:
        #     return False

        outputs = DagDatasetRelation.get_outputs(dag_id=dag_id)
        # if len(outputs) > 0:
        #     downstream_gra = outputs[0].granularity
        #     # 如果下游依赖当前任务的粒度大于下游本身的粒度,那么不做触发
        #     if int(downstream_gra) > Granularity.gra_priority_map[depend_event_gra]:
        #         return False
        # else:
        #     return False

        return True

    @staticmethod
    def can_be_triggered_by_data(dag_id):
        '''
        判断某个任务是否可以被数据触发
        条件1: 任务必须是上线状态
        条件2: 一个任务不能是crontab时间调度的任务
        '''
        from airflow.models.dag import DagModel
        # 判断任务是否是上线状态
        if not DagModel.is_active_and_on(dag_id=dag_id):
            return False
        # crontab时间调度的任务由crontab去触发
        if TaskDesc.check_is_crontab_task(task_name=dag_id):
            return False

        return True

    @staticmethod
    @provide_session
    def generate_latest_cron_trigger(dag_id,crontab,start_date=None,end_date=None,transaction=False,session=None):
        '''
        规则：
        如果任务存在trigger（代表任务并非第一次上线），生成下一个周期的trigger
        如果任务不存在rigger（代表任务第一次上线），生成前一个周期的trigger
        '''

        # 校验crontab是否正确
        if not croniter.is_valid(crontab):
            log.log.error(
                "[DataStudio Pipeline] Cronjob: task '{}' have wrong crontab '{}'".format(dag_id, crontab))
            raise ValueError("[DataStudio Pipeline] Cronjob: task '{}' have wrong crontab '{}'".format(dag_id, crontab))
        nowDate = timezone.utcnow()
        nowDate = nowDate.replace(tzinfo=beijing)
        cnt = Trigger.count_with_date_range(dag_id=dag_id,session=session)
        exe_date = None
        if cnt is not None and cnt >0:
            '''
            为什么一定要为cronjob任务生成一个trigger呢？ 这个是为了防止一个下线很久的任务，突然上线，那么中间未调度的实例就会被全部创建出来。
            所以在这里生成一个trigger，就等于重新给定了一个截止日期，这样避免了上述的情况。
            '''
            cron = croniter(crontab, nowDate - timedelta(minutes=2))  # 为了防止时间误差，这里减去2分钟
            exe_date = cron.get_next(datetime)
            trigger_type = PIPELINE
            state = State.WAITING
        else :
            cron = croniter(crontab, nowDate)
            exe_date = cron.get_prev(datetime)
            trigger_type = FIRST
            # 这里给backfill_wait_dep状态，目的是为了在首次上线任务时，触发外部数据的检查
            state = State.BF_WAIT_DEP
        tris = Trigger.find(dag_id=dag_id, execution_date=exe_date,session=session)
        if tris is not None and len(tris) > 0:
            # 已有trigger存在就不在创建了
            # ps:这一步一定需要,因为后面创建的是'clear_backCalculation',相当于重算,会重新执行任务
            return

        # 在任务给定的开始时间之前的tri为终止状态
        if start_date and DateUtil.date_compare(pendulum.parse(start_date), exe_date, eq=False):
            state = State.TASK_STOP_SCHEDULER
        # 在任务给定的结束时间之后的tri为终止状态
        if end_date and DateUtil.date_compare(exe_date, pendulum.parse(end_date), eq=False):
            state = State.TASK_STOP_SCHEDULER

        start_check_time = max([my_make_naive(nowDate),my_make_naive(exe_date)])
        # 生成trigger
        Trigger.sync_trigger_to_db(dag_id=dag_id, execution_date=exe_date, state=state,start_check_time=start_check_time,
                                   trigger_type=trigger_type, editor="CronTrigger", transaction=transaction,priority=5,
                                   session=session)


    @staticmethod
    @provide_session
    def generate_latest_data_trigger(dag_id,gra,transaction=False,session=None):
        '''
        规则：
        如果任务存在trigger（代表任务并非第一次上线）直接退出
        如果任务不存在rigger（代表任务第一次上线），生成前一个周期的trigger
        '''
        nowDate = timezone.utcnow()
        nowDate = nowDate.replace(tzinfo=beijing)
        cnt = Trigger.count_with_date_range(dag_id=dag_id, session=session)
        if cnt is not None and cnt > 0:
            # 存在trigger
            return
        prev_exe_date = Granularity.get_latest_execution_date(nowDate,gra)
        # tris = Trigger.find(dag_id=dag_id, execution_date=prev_exe_date,session=session)
        # if tris is not None and len(tris) > 0:
        #     # 已有trigger存在就不在创建了
        #     # ps:这一步一定需要,因为后面创建的是'clear_backCalculation',相当于重算,会重新执行任务
        #     return
        # 生成trigger
        Trigger.sync_trigger_to_db(dag_id=dag_id, execution_date=prev_exe_date, state=State.BF_WAIT_DEP,
                                   trigger_type=FIRST, editor="DataPipeline", transaction=transaction,
                                   session=session)

    #
    # @staticmethod
    # def can_be_trigger(task_id,data_date_calculation_param,event_date_calculation_param,task_gra):
    #     task_gra = Granularity.getIntGra(task_gra)
    #
    #     for data_dcp in data_date_calculation_param:
    #
    @staticmethod
    @provide_session
    def create_taskinstance(task_dict,execution_date,task_type=PIPELINE,external_conf=None,is_debug=False,transaction=False,session=None):
        '''
        创建taskInstance，目的是生成一个待执行的任务
        返回taskInstance的实例

        params: task_dict
        params: execution_date
        '''
        from airflow.utils.state import State
        from airflow.models import TaskInstance
        if external_conf is not None:
            if isinstance(external_conf,dict):
                external_conf = json.dumps(external_conf)

        for task in six.itervalues(task_dict):
            ti = None
            res = session.query(TaskInstance).filter(TaskInstance.task_id == task.task_id).filter(
                TaskInstance.execution_date == execution_date).all()
            if res is not None and len(res) > 0:
                ti = res[0]
                ti.task_type = task_type
                # 状态为None的，更新为scheduled，None可能是clear造成`的
                if external_conf:
                    ti.external_conf = external_conf
                if ti.state is None:
                    ti.state = State.SCHEDULED
            else:
                if is_debug:
                    ti = TaskInstance(task,execution_date,state=None,task_type=task_type,external_conf=external_conf)
                else :
                    ti = TaskInstance(task,execution_date,state=State.SCHEDULED,task_type=task_type,external_conf=external_conf)

            session.merge(ti)
        if not transaction:
            session.commit()

    @staticmethod
    @provide_session
    def task_finish_trigger(ti,session=None):
        from airflow.shareit.models.trigger import Trigger
        from airflow.shareit.models.task_desc import TaskDesc
        from airflow.utils.state import State
        from airflow.shareit.utils.callback_util import CallbackUtil
        from airflow.shareit.workflow.workflow_state_sync import WorkflowStateSync

        ti.log.info("[DataStudio pipeline] start callback....")
        # 回调，把ti状态同步给其他平台
        ex_conf = ti.get_external_conf()
        if ex_conf is not None and isinstance(ex_conf, dict) and "callback_info" in ex_conf.keys():
            try:
                callback_info = ex_conf['callback_info']
                url = callback_info['callback_url']
                data = {"callback_id": callback_info['callback_id'], "state": ti.state,
                        "instance_id": ex_conf.get('batch_id',''),
                        "args":callback_info['extra_info']}
                CallbackUtil.callback_post_guarantee(url, data, ti.dag_id ,ti.execution_date,ex_conf.get('batch_id',''))
            except Exception as e:
                ti.log.error("[DataStudio pipeline] callback ti state request failed: " + str(e))

        ti.log.info("[DataStudio pipeline] record instance info ....")
        # # 增加执行记录
        ti.add_taskinstance_log()
        td = TaskDesc.get_task(task_name=ti.dag_id)
        if Trigger.check_is_forced(task_name=ti.dag_id,execution_date=ti.execution_date):
            ti.log.info("[DataStudio pipeline] forced tasks do not trigger downstream")
            try:
                NormalUtil.callback_task_state(td, State.FAILED if ti.state == State.UP_FOR_RETRY else ti.state)
                Trigger.sync_state(dag_id=ti.dag_id, execution_date=ti.execution_date,
                                   trigger_type=None, state=ti.state, transaction=False, session=session)
            except Exception:
                ti.log.error("[DataStudio pipeline] sync state to ds backend error")
            return
        # 同步workflow状态
        if td.workflow_name and td.workflow_name != '':
            ti.log.info("[DataStudio pipeline] sync state to workflow instance")
            try:
                WorkflowStateSync.ti_push_state_2_wi(td.workflow_name,ti,tenant_id=td.tenant_id)
            except Exception:
                ti.log.error(traceback.format_exc())

        ti.log.info("[DataStudio pipeline] start trigger downstream ....")
        # 在这里trigger下游
        if ti.state in (State.SUCCESS, State.FAILED):
            if ti.state == State.SUCCESS:
                ti.log.info('任务运行成功')
                ti.log.info('[STATUS INFO]--SUCCESS')
                # 打开灰度
                try:
                    ex_conf = ti.get_external_conf()
                    if ex_conf and 'is_kyuubi' not in ex_conf.keys():
                        GrayTest.open_gray_test(task_name=ti.dag_id)
                except Exception:
                    pass
            if ti.state == State.FAILED:
                ti.log.info('任务运行失败')
                ti.log.info('[STATUS INFO]--FAILED')
            from airflow.shareit.service.dataset_service import DatasetService
            try:
                # DataPipeline sync state of trigger and dataset
                Trigger.sync_state(dag_id=ti.dag_id, execution_date=ti.execution_date,
                                   trigger_type=None, state=ti.state, transaction=False, session=session)
                if ti.state == State.SUCCESS:
                    TriggerHelper.generate_trigger(dag_id=ti.dag_id, execution_date=ti.execution_date,
                                                   transaction=False,task_desc=td,
                                                   session=session)
                    # 执行成功后将trigger的trigger_type替换为正常pipeline，并且记录is_executed为true
                    Trigger.sync_trigger(dag_id=ti.dag_id, execution_date=ti.execution_date,
                                         # trigger_type="pipeline",
                                         is_executed=True, transaction=False, session=session)

                from airflow.models import TaskInstance
                # 将最近一次运行的状态返回给ds_task服务
                if TaskInstance.is_latest_runs(ti.dag_id, ti.execution_date, session=session):
                    ti.log.info("[DataStudio pipeline] start sync state to ds backend ...")
                    tmp_err = None
                    for _ in range(3):
                        try:
                            NormalUtil.callback_task_state(td, State.FAILED if ti.state == State.UP_FOR_RETRY else ti.state)
                            tmp_err= None
                        except Exception as e:
                            time.sleep(0.5)
                            tmp_err = e
                    if tmp_err:
                        ti.log.error("[DataStudio pipeline] sync state to ds backend error: " + str(tmp_err))
            except Exception as e:
                s = traceback.format_exc()
                ti.log.error(
                    "[DataStudio pipeline] trigger-downstream failed！！！,dag_id:{} ".format(ti.dag_id) + str(e))
                ti.log.error(s)
    @staticmethod
    def test_task_finish_trigger(ti):
        TriggerHelper.task_finish_trigger(ti)

    @staticmethod
    @provide_session
    def populate_trigger(task_id,gra,session=None):
        '''
        触发调度，如果依赖多个周期前的，由于上游已经执行过了，所以不会触发未来几个周期的。
        这个方法用来补充创建trigger
        '''
        from airflow.models import DagModel
        try:
            nowDate = utcnow()
            nowDate = nowDate.replace(tzinfo=beijing)
            # 获取当前task最近一次的execution_date
            prev_exe_date = Granularity.get_latest_execution_date(nowDate,gra)

            # 所有上游中，最大的execution_date及那个上游id
            latest_upstream_execution_date=None
            latest_upstream_id=None

            event_depends = EventDependsInfo.get_dag_event_depends(task_id, Constant.TASK_SUCCESS_EVENT)
            for e_depend in event_depends:
                upstream_dag_id = e_depend.depend_id
                # 上游下线判定失败
                if not DagModel.is_active_and_on(dag_id=upstream_dag_id):
                    return False
                # 获取要检查的任务的所有实例日期
                upstream_exe_dates = DateCalculationUtil.get_upstream_all_execution_dates(prev_exe_date,
                                                                                         e_depend.date_calculation_param,e_depend.depend_id)
                for u_exe_date in upstream_exe_dates:
                    if latest_upstream_execution_date is None or u_exe_date.strftime('%Y-%m-%d %H:%M:%S') > latest_upstream_execution_date:
                        latest_upstream_execution_date = u_exe_date.strftime('%Y-%m-%d %H:%M:%S')
                        latest_upstream_id = upstream_dag_id


            latest_upstream_data_date=None
            latest_metadata_id=None
            # 获取该任务的所有上游数据
            input_datasets = DagDatasetRelation.get_inputs(dag_id=task_id)
            if len(input_datasets) > 0:
                input_datasets = DateCalculationUtil.generate_ddr_date_calculation_param(input_datasets)
                # 用来判断是不是外部数据
                outer_datasets = [(out.dag_id, out.metadata_id) for out in DagDatasetRelation.get_outer_dataset(dag_id=task_id)]
                for i_dataset in input_datasets:
                    if (i_dataset.dag_id, i_dataset.metadata_id) in outer_datasets:
                        #外部数据过滤掉
                        continue
                    data_dates = DateCalculationUtil.get_upstream_all_data_dates(execution_date=prev_exe_date,
                                                                                 calculation_param=i_dataset.date_calculation_param,
                                                                                 gra=i_dataset.granularity)
                    for data_date in data_dates:
                        if latest_upstream_data_date is None or data_date.strftime('%Y-%m-%d %H:%M:%S') > latest_upstream_data_date:
                            latest_upstream_data_date = data_date.strftime('%Y-%m-%d %H:%M:%S')
                            latest_metadata_id = i_dataset.metadata_id


                ddr = DagDatasetRelation.get_upstream_dags(metadata_id=latest_metadata_id)

                tmp_exe = Granularity.getNextTime(pendulum.parse(latest_upstream_data_date), gra=ddr[0].granularity, offset=ddr[0].offset)
                execution_dates = TaskCronUtil.get_all_date_in_one_cycle(ddr[0].dag_id, tmp_exe)
                if latest_upstream_execution_date is None or execution_dates[0].strftime(
                        '%Y-%m-%d %H:%M:%S') > latest_upstream_execution_date:
                    latest_upstream_execution_date = execution_dates[0].strftime('%Y-%m-%d %H:%M:%S')
                    latest_upstream_id = ddr[0].dag_id


            # 获取到所有上游里最近一次的execution_date并且和它对应的dag_id,用它来trigger当前任务
            start_date = datetime.strptime(latest_upstream_execution_date,'%Y-%m-%d %H:%M:%S').replace(tzinfo=beijing)
            end_date = nowDate
            if DateUtil.date_compare(start_date,end_date):
                return
            contab = TaskCronUtil.get_task_cron(latest_upstream_id)
            all_cron_dates = croniter_range(start_date, end_date, contab)
            for dt in all_cron_dates:
                TriggerHelper.generate_trigger(dag_id=latest_upstream_id,execution_date=dt,down_task_id=task_id,is_force=True)
        except Exception as e:
            logging.error(traceback.format_exc())
            logging.error("populate_trigger failed !!! task_name:{}".format(task_id))

    @staticmethod
    @provide_session
    def reopen_future_trigger(task_name, start_date=None,transaction=False,session=None):
        if start_date is None:
            start_date = timezone.utcnow().replace(tzinfo=beijing)
        tris = session.query(Trigger).filter(Trigger.dag_id == task_name).filter(Trigger.execution_date > start_date).all()
        for tri in tris:
            if tri.state == State.TASK_STOP_SCHEDULER:
                tri.state = State.WAITING
                session.merge(tri)
        if not transaction:
            session.commit()


