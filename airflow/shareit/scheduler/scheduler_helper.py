# -*- coding: utf-8 -*-
"""
author: zhangtao
Copyright (c) 2021/5/10 Shareit.com Co., Ltd. All Rights Reserved.
"""
import logging
import multiprocessing
import os
import signal
import traceback
import time
from datetime import timedelta

import requests
import six

from airflow import AirflowException
from airflow.exceptions import AirflowTaskTimeout
from airflow.models import DagRun, TaskInstance, Pool, DagModel
from airflow.shareit.check.check_dependency_helper import CheckDependecyHelper
from airflow.shareit.models.alert import Alert
from airflow.shareit.models.backfill_job import BackfillJob
from airflow.shareit.models.dataset_partation_status import DatasetPartitionStatus
from airflow.shareit.models.event_trigger_mapping import EventTriggerMapping
from airflow.shareit.models.fast_execute_task import FastExecuteTask
from airflow.shareit.models.sensor_info import SensorInfo
from airflow.shareit.models.task_desc import TaskDesc
from airflow.shareit.models.task_life_cycle import TaskLifeCycle
from airflow.shareit.monitor.push_monitor import MonitorPush
from airflow.shareit.monitor.task_life_cycle_helper import TaskLifeCycleHelper
from airflow.shareit.scheduler.backfill_helper import BackfillHelper
from airflow.shareit.scheduler.push_scheduler import PushSchedulerHelper
from airflow.shareit.trigger.trigger_helper import TriggerHelper
from airflow.shareit.utils.date_calculation_util import DateCalculationUtil
from airflow.shareit.utils.date_util import DateUtil
from airflow.shareit.utils.normal_util import NormalUtil
from airflow.shareit.utils.task_cron_util import TaskCronUtil
from airflow.shareit.utils.task_util import TaskUtil
from airflow.utils import timezone
from airflow.utils.db import provide_session
from airflow.utils.log.logging_mixin import LoggingMixin, StreamLogWriter
from airflow.utils.state import State
from airflow.utils.helpers import reap_process_group
from airflow.configuration import conf

from airflow.shareit.utils.state import State as myState
from airflow.shareit.utils.task_manager import get_dag
from airflow.shareit.models.trigger import Trigger
from airflow.shareit.service.workflow_service import WorkflowService
from airflow.shareit.models.dag_dataset_relation import DagDatasetRelation
from airflow.utils.timeout import timeout
from airflow.utils.timezone import my_make_naive, utcnow, beijing
from airflow.shareit.constant.taskInstance import *

log = LoggingMixin().logger


def backfill_job_processor():
    '''
    启动所有的backfill_job,如果把每一次补数都当做一整个计划的话,这里就是启动的入口。 和backfill_processor是有区别的， 后者是用来将BF_WAIT_DEP状态的实例更新为WAITING状态。
    '''
    helper = BackfillHelper()
    while True:
        loop_start_time = time.time()
        log.info("[DataStudio Pipeline] Backfill_job_proccessor start...")

        try:
            jobs = BackfillJob.find_waiting_jobs()
            for job in jobs:
                helper.start_backfill_job(job.id)
            loop_end_time = time.time()
            loop_duration = loop_end_time - loop_start_time
            if loop_duration < 3:
                sleep_length = 3 - loop_duration
                log.debug(
                    "Sleeping for {0:.2f} seconds to prevent excessive logging"
                        .format(sleep_length))
                time.sleep(sleep_length)
        except Exception:
            s = traceback.format_exc()
            log.error("[DataStudio Pipeline] Backfill_job_proccessor error")
            log.error(s)

        log.info("[DataStudio Pipeline] Backfill_job_proccessor end...")

def push_fast_execute_proccessor():
    while True:
        loop_start_time = time.time()
        log.info("[DataStudio Pipeline] push_fast_execute_proccessor start...")

        execution_date_init = DateUtil.format_millisecond(timezone.utcnow().replace(tzinfo=beijing))
        try:
            task_list = FastExecuteTask.find_all_waiting_task()
            index = 1000
            for task in task_list:
                index += 1000
                # 日期每次增加1毫秒，主要是为了使得任务的execution_date不相同
                execution_date = execution_date_init + timedelta(microseconds=index)
                PushSchedulerHelper.push_task_execute(task,execution_date)

            etm_list = EventTriggerMapping.get_all_waiting()
            for etm in etm_list:
                index += 1000
                # 日期每次增加1毫秒，主要是为了使得任务的execution_date不相同
                execution_date = execution_date_init + timedelta(microseconds=index)
                PushSchedulerHelper.event_trigger_task_execute(etm,execution_date)
            loop_end_time = time.time()
            loop_duration = loop_end_time - loop_start_time
            if loop_duration < 2*60:
                sleep_length = 2*60 - loop_duration
                log.debug(
                    "Sleeping for {0:.2f} seconds to prevent excessive logging"
                        .format(sleep_length))
                time.sleep(sleep_length)
        except Exception:
            s = traceback.format_exc()
            log.error("[DataStudio Pipeline] push_fast_execute_proccessor error")
            log.error(s)

        log.info("[DataStudio Pipeline] push_fast_execute_proccessor end...")

def backfill_processor():
    '''
    与backfill_job_processor相比，负责的是不同的内容
    '''
    while True:
        loop_start_time = time.time()
        log.info("[DataStudio Pipeline] Backfill_proccessor start...")

        try:
            SchedulerManager().backfill_trigger_process()
            loop_end_time = time.time()
            loop_duration = loop_end_time - loop_start_time
            if loop_duration < 3:
                sleep_length = 3 - loop_duration
                log.debug(
                    "Sleeping for {0:.2f} seconds to prevent excessive logging"
                        .format(sleep_length))
                time.sleep(sleep_length)
        except Exception:
            s = traceback.format_exc()
            log.error("[DataStudio Pipeline] Backfill_proccessor error")
            log.error(s)

        log.info("[DataStudio Pipeline] Backfill_proccessor end...")

def quick_run_processor():
    # trigger的快速通道
    while True:
        loop_start_time = time.time()
        log.info("[DataStudio Pipeline] QuickScheduler start...")

        try:
            triggers = Trigger.get_untreated_triggers()
            now_date_str = utcnow().strftime('%Y-%m-%d %H:%M:%S')
            def filter_tri(node):
                if node.priority <= 1:
                    return False
                if node.execution_date.strftime('%Y-%m-%d %H:%M:%S') > now_date_str:
                    return False
                else:
                    return True

            # 过滤掉当前时间之后的trigger
            priority_tris = filter(lambda x: filter_tri(x), triggers)
            log.info("[DataStudio Pipeline] QuickScheduler trigger nums:{}".format(str(len(priority_tris))))
            scheduler(priority_tris)
            # # 降低优先级
            # Trigger.decrease_priority_by_tris(priority_tris)
            loop_end_time = time.time()
            loop_duration = loop_end_time - loop_start_time
            if loop_duration < 3:
                sleep_length = 3 - loop_duration
                log.debug(
                    "Sleeping for {0:.2f} seconds to prevent excessive logging"
                        .format(sleep_length))
                time.sleep(sleep_length)
        except Exception:
            s = traceback.format_exc()
            log.error("[DataStudio Pipeline] QuickScheduler error")
            log.error(s)

        log.info("[DataStudio Pipeline] QuickScheduler end...")

def scheduler(triggers):
    from airflow.utils import timezone
    from airflow.shareit.utils.state import State as State_new
    for trigger in triggers:
        if not DagModel.is_active_and_on(trigger.dag_id):
            continue
        # 判断任务依赖，将符合条件的任务加入列表
        # 刷新一下trigger信息
        trigger.refresh_from_db()
        if trigger.state in State_new.finished():
            continue

        is_backfill = True if trigger.trigger_type in [FIRST,BACKFILL,CLEAR] else False
        try:
            td = TaskDesc.get_task(task_name=trigger.dag_id)
            expire_time = td.get_check_expire_time()
            # 检查是不是已经超过超时时间了，超过就关闭
            if trigger.start_check_time and DateUtil.date_compare(timezone.utcnow(),
                                                                  trigger.start_check_time + timedelta(hours=expire_time)):
                # 超时的关闭trigger和sensor
                trigger.sync_state_to_db(state=State_new.TASK_STOP_SCHEDULER)
                SensorInfo.close_sensors(task_name=trigger.dag_id,execution_date=trigger.execution_date)
                log.info("[DataStudio Pipeline] tirgger check timed out, dag:{dag_id},time:{time}".format(
                    dag_id=trigger.dag_id, time=str(trigger.execution_date)))
                # 将终止的状态返回给ds_task服务
                try:
                    if TaskInstance.is_latest_runs(trigger.dag_id, trigger.execution_date.replace(tzinfo=timezone.beijing)):
                        NormalUtil.callback_task_state(td, State_new.FAILED)
                except Exception as e:
                    log.error("[DataStudio Pipeline] task:{} ,sync state to ds backend error: {}".format(trigger.dag_id,
                                                                                                         str(e)))
                continue

            if trigger.ignore_check:
                is_ready = True
            else:
                is_ready, ready_time = CheckDependecyHelper.check_dependency(trigger.dag_id, trigger.execution_date,is_backfill)
            if not is_ready:
                # 降低权重
                Trigger.decrease_priority_by_tris([trigger])
                continue
        except Exception:
            s = traceback.format_exc()
            log.error("check dependency failed !!!!!!!!!!!")
            log.error(s)
            continue
        try:
            exe_time = trigger.execution_date.replace(tzinfo=timezone.beijing)
            dag = get_dag(trigger.dag_id)
            if dag is None:
                log.warning("[DataStudio Pipeline] Main_scheduler_process: {dag_id} not exists!!!".format(dag_id=trigger.dag_id))
                continue

            can_tri = True
            # 正常调度和首次调度的实例需要判断是不是在开始时间之内,如果不是,则禁止被调起
            if trigger.trigger_type in [FIRST, PIPELINE]:
                # 在任务给定的开始时间之前的ti不生成
                if td.start_date is not None and DateUtil.date_compare(td.start_date, exe_time, eq=False):
                    can_tri = False
                # 在任务给定的结束时间之后的ti不生成
                if td.end_date is not None and DateUtil.date_compare(exe_time, td.end_date, eq=False):
                    can_tri = False

            if can_tri == False:
                # 将trigger更新为终止状态，避免下次再轮询也避免修改开始/结束日期后任务被调起一大堆
                trigger.sync_state_to_db(state=State_new.TASK_STOP_SCHEDULER)
                log.info("[DataStudio Pipeline] not in date range,cant be trigged, dag:{dag_id},time:{time}".format(
                    dag_id=trigger.dag_id, time=str(exe_time)))
                continue

            # dr = DagRun.find(dag_id=trigger.dag_id, execution_date=exe_time)
            tis = TaskInstance.find_by_execution_dates(dag_id=trigger.dag_id,execution_dates=[exe_time])

            if len(tis) > 0 and tis[0].state is not None:
                # 过滤掉createCheckFile任务
                tis = filter(lambda x:x.task_id != 'createCheckFile',tis)
                ti_state = None
                if len(tis) == 2:
                    # 特别处理两个实例任务这种情况，应该是sharestore任务
                    if tis[0].task_id == tis[1].task_id :
                        log.error("[DataStudio Pipeline] taskInstance repeated, dag:{dag_id},time:{time}".format(dag_id=trigger.dag_id,time=str(exe_time)))
                        continue
                    if tis[0].state == State.FAILED or tis[1].state == State.FAILED:
                        ti_state = State.FAILED
                    elif tis[0].state == State.SUCCESS and tis[1].state == State.SUCCESS:
                        ti_state = State.SUCCESS
                    else :
                        ti_state = State.RUNNING

                elif len(tis) == 1:
                    ti_state = tis[0].state
                else :
                    log.error("[DataStudio Pipeline] taskInstance nums:{num} , dag:{dag_id},time:{time}".format(num=len(tis),dag_id=trigger.dag_id,time=str(exe_time)))
                    continue
                if ti_state in State.finished():
                    if is_backfill:
                        for ti in tis:
                            ti.task_type = BACKFILL
                        TaskInstance.clear_task_instance_list(tis,task_type=trigger.trigger_type if trigger.trigger_type in [BACKFILL,CLEAR] else PIPELINE, dag=dag,backfill_label=trigger.backfill_label)
                        # 更新任务类型
                        trigger.sync_state_to_db(state=State.RUNNING)
                        # 如果有alert需求，这里重新打开
                        Alert.reopen_alert_job(task_name=trigger.dag_id,execution_date=trigger.execution_date)
                        log.info(
                            "[DataStudio Pipeline] Main_scheduler_process: clear dagrun,set trigger state {task} {time}...".format(
                                task=str(trigger.dag_id),
                                time=str(exe_time)))
                    else:
                        log.info(
                            "[DataStudio Pipeline] Main_scheduler_process: set trigger state {task} {time}...".format(
                                task=str(trigger.dag_id),
                                time=str(exe_time)))

                        trigger.sync_state_to_db(state=ti_state)
                else:
                    trigger.sync_state_to_db(state=State.RUNNING)
                if td.workflow_name:
                    log.info(
                        "[DataStudio Pipeline] Main_scheduler_process: open workflow instance {task} {time}...".format(
                            task=str(trigger.dag_id),
                            time=str(exe_time)))
                    from airflow.shareit.workflow.workflow_state_sync import WorkflowStateSync
                    WorkflowStateSync.open_wi(td.workflow_name, execution_date=exe_time)
                # 回调给dstask
                try:
                    if TaskInstance.is_latest_runs(trigger.dag_id, exe_time):
                        NormalUtil.callback_task_state(td, State.RUNNING)
                except Exception as e:
                    log.error("[DataStudio Pipeline] task:{} ,sync state to ds backend error: {}".format(trigger.dag_id,                                                                                       str(e)))
                continue

            TriggerHelper.create_taskinstance(dag.task_dict,exe_time,task_type=trigger.trigger_type if trigger.trigger_type in [BACKFILL,CLEAR] else PIPELINE)

            if td.workflow_name:
                log.info(
                    "[DataStudio Pipeline] Main_scheduler_process: open workflow instance {task} {time}...".format(
                        task=str(trigger.dag_id),
                        time=str(exe_time)))
                from airflow.shareit.workflow.workflow_state_sync import WorkflowStateSync
                WorkflowStateSync.open_wi(td.workflow_name,execution_date=exe_time)
            trigger.sync_state_to_db(state=myState.RUNNING)
            log.info(
                "[DataStudio Pipeline] Main_scheduler_process: trigger {task} {time}...".format(task=str(trigger.dag_id),
                                                                                                time=str(exe_time)))
            # 将开始运行的状态返回给ds_task服务
            try:
                if TaskInstance.is_latest_runs(trigger.dag_id,exe_time):
                    NormalUtil.callback_task_state(td,State.RUNNING)
            except Exception as e:
                log.error("[DataStudio Pipeline] task:{} ,sync state to ds backend error: {}".format(trigger.dag_id,str(e)))
        except Exception as e:
            log.error("[DataStudio Pipeline] clear or create dagrun failed !,dag_id :{}".format(trigger.dag_id), exc_info=True)




class SchedulerManager(LoggingMixin):
    def __init__(self):
        self._processors = {}
        # Pipe for communicating signals
        self._process = None
        self._done = False
        # Initialized as true so we do not deactivate w/o any actual DAG parsing.
        self._all_files_processed = True

        self._parent_signal_conn = None
        self._collected_dag_buffer = []

    @staticmethod
    def main_scheduler_process(check_parallelism):
        pool = multiprocessing.Pool(processes=check_parallelism)
        triggers = Trigger.get_untreated_triggers()
        now_date_str = utcnow().strftime('%Y-%m-%d %H:%M:%S')

        def filter_tri(node):
            if node.priority != 1:
                return False
            if node.execution_date.strftime('%Y-%m-%d %H:%M:%S') > now_date_str:
                return False
            else:
                return True
        # 过滤掉当前时间之后的trigger
        filter_triggers = filter(lambda x: filter_tri(x), triggers)
        # jobs = []
        log.info("[DataStudio Pipeline] check_parallelism : {}".format(str(check_parallelism)))
        log.info("[DataStudio Pipeline] Main_scheduler_process start..., {} trigger wait for check".format(len(filter_triggers)))
        tri_nums = len(filter_triggers)
        split_triggers = SchedulerManager.list_split(filter_triggers,tri_nums/check_parallelism + 1)
        for tris in split_triggers:
            pool.apply_async(scheduler, (tris,))

        # 实现join的功能但不关闭进程池.
        # for job in jobs:
        #     try:
        #         job.get()
        #     except Exception as e:
        #         log.warning("[DataStudio Pipeline] Main_scheduler_process get job failed,e:{} ".format(e.message))
        #         continue

        pool.close()
        pool.join()

        log.info("[DataStudio Pipeline] Main_scheduler_process end...".format())

    def terminate(self, sigkill=False):
        """
        Terminate (and then kill) the process launched to process the file.

        :param sigkill: whether to issue a SIGKILL if SIGTERM doesn't work.
        :type sigkill: bool
        """
        if self._process is None:
            raise AirflowException("Tried to call terminate before starting!")

        self._process.terminate()
        # Arbitrarily wait 5s for the process to die
        self._process.join(5)
        if sigkill:
            self._kill_process()
        self._parent_signal_conn.close()

    def _kill_process(self):
        if self._process.is_alive():
            self.log.warning("Killing PID %s", self._process.pid)
            os.kill(self._process.pid, signal.SIGKILL)

    def end(self):
        if not self._process:
            self.log.warning('Ending without manager process.')
            return
        reap_process_group(self._process.pid, log=self.log)
        self._parent_signal_conn.close()

    def backfill_trigger_process(self):
        triggers = Trigger.get_triggers(state=myState.BF_WAIT_DEP)
        now_date = utcnow().replace(tzinfo=beijing)
        for tri in triggers:
            try:
                rep = CheckDependecyHelper.list_data_dependency(dag_id=tri.dag_id,
                                                                        execution_date=tri.execution_date)
                self._process_dep(dag_id=tri.dag_id, execution_date=tri.execution_date, unready_dep_list=rep)
                tri.sync_state_to_db(state=myState.WAITING)
                TaskLifeCycleHelper.push_new_cycle(task_name=tri.dag_id,execution_date=tri.execution_date,tri_type=tri.trigger_type,time=DateUtil.get_timestamp(now_date))
            except Exception:
                s = traceback.format_exc()
                log.error("backfill trigger process failed, trigger :{} !!!!!!!!!!!".format(tri.dag_id))
                log.error(s)
                continue
    def _process_dep(self, dag_id, execution_date, unready_dep_list):
        """
        dag_id: 任务唯一标识
        execution_date:执行入参时间
        unready_dep_list： 未就绪的上游依赖列表 [(data_date,metadata_id)]

        """
        for dep in unready_dep_list:
            # 外部数据
            "dag_id,exe_date,metadata_id,meta_exe_date"
            ddr = DagDatasetRelation.get_relation(dag_id=dag_id,metadata_id=dep[1],dataset_type=1)

            #
            # datasets = DagDatasetRelation.get_active_downstream_dags(metadata_id=dep[1])
            # res = TaskUtil._merger_dataset_check_info(datasets=datasets)
            # ddr = None
            # for dataset in res[0]:
            #     if dataset.dag_id == dag_id:
            #         ddr = dataset
            if ddr:
                result = DateCalculationUtil.generate_ddr_date_calculation_param([ddr])
                ddr = result[0]
                if ddr.check_path and ddr.check_path.strip():
                    render_res = TaskUtil.render_datatime(content=ddr.check_path, execution_date=execution_date,
                                                          date_calculation_param=ddr.date_calculation_param,
                                                          gra=ddr.granularity,
                                                          dag_id=dag_id)
                    for render_re in render_res:
                        if my_make_naive(render_re["date"]) == my_make_naive(dep[0]):
                            SensorInfo.register_sensor(dag_id=dag_id, metadata_id=dep[1],
                                                       detailed_execution_date=dep[0],
                                                       execution_date=execution_date,
                                                       back_fill=True,
                                                       granularity=ddr.granularity, offset=ddr.offset,
                                                       check_path=render_re["path"],
                                                       cluster_tags="backfill_sensor")
                else:
                    if ddr.ready_time and len(ddr.ready_time) > 5:
                        dt = TaskCronUtil.get_all_date_in_one_cycle(task_name=dag_id,
                                                                    execution_date=execution_date,
                                                                    cron=ddr.ready_time,
                                                                    gra=ddr.granularity)
                        completion_time = dt[0]
                    else :
                        completion_time = DateUtil.date_format_ready_time_copy(execution_date, ddr.ready_time,ddr.granularity)

                    log.info("Backfill_proccessor dag_id:{},metadata_id:{},completion_time:{}".format(dag_id,dep[1],completion_time))
                    SensorInfo.register_sensor(dag_id=dag_id, metadata_id=dep[1],
                                               detailed_execution_date=dep[0],
                                               execution_date=execution_date,
                                               back_fill=True,
                                               granularity=ddr.granularity, offset=ddr.offset,
                                               cluster_tags="backfill_sensor",
                                               completion_time=completion_time)



    @staticmethod
    def list_split(items, n):
        return [items[i:i+n] for i in range(0, len(items), n)]





class QuickScheduler(LoggingMixin):
    def __init__(self):
        self.timeout = -1

    def start(self):
        process = multiprocessing.Process(
            target=quick_run_processor,
            args=(),
        )
        process.start()

class BackfillProccess(LoggingMixin):
    def __init__(self):
        self.timeout = -1

    def start(self):
        process = multiprocessing.Process(
            target=backfill_processor,
            args=(),
        )
        process.start()

class PushFastExecuteProccess(LoggingMixin):
    def __init__(self):
        self.timeout = -1

    def start(self):
        process = multiprocessing.Process(
            target=push_fast_execute_proccessor,
            args=(),
        )
        process.start()

class BackfillJobProccess(LoggingMixin):
    def __init__(self):
        self.timeout = -1

    def start(self):
        process = multiprocessing.Process(
            target=backfill_job_processor,
            args=(),
        )
        process.start()