# -*- coding: utf-8 -*-
import datetime
import multiprocessing
import time
import traceback

import pendulum
from airflow.configuration import conf
from airflow.exceptions import AirflowTaskTimeout

from airflow.hooks.genie_hook import GenieHook
from airflow.models.dag import DagModel

from airflow import LoggingMixin
from airflow.models import TaskInstance
from airflow.shareit.constant.kyuubi_state import KyuubiState
from airflow.shareit.hooks.kyuubi_hook import KyuubiHook
from airflow.shareit.models.task_desc import TaskDesc
from airflow.shareit.monitor.push_monitor import MonitorPush
from airflow.shareit.trigger.trigger_helper import TriggerHelper
from airflow.shareit.utils.alarm_adapter import ding_recovery_state
from airflow.shareit.utils.date_util import DateUtil
from airflow.shareit.utils.task_manager import get_dag
from airflow.utils import timezone
from airflow.utils.db import provide_session
from airflow.utils.state import State
from airflow.utils.timeout import timeout
from airflow.utils.timezone import beijing

log = LoggingMixin().logger


def recovery_process():
    state_recover = StateRecoveryHelper()
    # monitor_push = MonitorPush()
    # 恢复异常的状态
    while True:
        # try:
        #     with timeout(3):
        #         monitor_push.push_to_prometheus(job_name="recovery_process", gauge_name="recovery_process",
        #                                         documentation="recovery_process")
        # except AirflowTaskTimeout as e:
        #     log.error("[DataStudio Pipeline] push metrics timeout")
        # except Exception as e:
        #     log.error("[DataStudio Pipeline] cronjob push metrics failed")
        loop_start_time = time.time()
        log.info("[DataStudio Pipeline] Recovery state proccess start...")
        state_recover.recovery_state()
        loop_end_time = time.time()
        loop_duration = loop_end_time - loop_start_time
        # 五分钟同步一次
        if loop_duration < 60:
            sleep_length = 60 - loop_duration
            log.debug(
                "Sleeping for {0:.2f} seconds to prevent excessive logging"
                    .format(sleep_length))
            time.sleep(sleep_length)

        log.info("[DataStudio Pipeline] Recovery state proccess end...")


class StateRecoveryHelper(LoggingMixin):

    def recovery_state(self):
        running_tis = TaskInstance.find_running_ti()
        kh = KyuubiHook()
        for ti in running_tis:
            try:
                if not DagModel.is_active_and_on(ti.dag_id):
                    continue
                ex_conf = ti.get_external_conf()

                if not ex_conf:
                    continue
                now_date = timezone.utcnow().replace(tzinfo=beijing)
                finish_date = None
                now_state = ''
                if 'batch_id' in ex_conf.keys():
                    kh.batch_id = ex_conf['batch_id']
                    kh.header = {}
                    job_info = kh.get_job_info()
                    if job_info:
                        state = job_info['state']
                        appState = job_info['appState']
                        finish_time = job_info['endTime']
                        if not ex_conf.get('is_spark', True):
                            # 当state为FINISHED，appState不为FINISHED时，认为任务失败
                            if state == KyuubiState.KYUUBI_FINISHED and appState != KyuubiState.KYUUBI_FINISHED:
                                state = KyuubiState.KYUUBI_ERROR
                        finish_date = pendulum.from_timestamp(int(finish_time)/1000).in_timezone(beijing)

                        if state in KyuubiState.success_state:
                            now_state = 'success'
                        elif state in KyuubiState.failed_state:
                            now_state = 'failed'
                        else:
                            continue

                        # 任务完成时间与当前时间间隔超过10分钟没有同步的，我们认为状态丢失，否则跳过
                        if (now_date - finish_date).total_seconds() > 600:
                            try:
                                ding_recovery_state(task_name=ti.dag_id, batch_id=ex_conf['batch_id'],state=state,app_state=appState,end_time=finish_time,ti_state=ti.state)
                            except Exception as e:
                                self.log.error('send ding alert failed,{}'.format(str(e)))
                                pass
                        else:
                            continue


                if not now_state:
                    continue

                dag = get_dag(ti.dag_id)
                task = dag.get_task(ti.task_id)
                ti.task = task
                if now_state == 'success':
                    ti.state = State.SUCCESS
                    ti.end_date = finish_date
                    ti.set_duration()
                    StateRecoveryHelper.sync_and_trigger(ti)

                    if task.email_on_success and task.owner:
                        notify_type = conf.get('email', 'notify_type')
                        if self.is_send_notify(task, State.SUCCESS, "email") and "email" in notify_type:
                            try:
                                ti.email_notify_alert(State.SUCCESS)
                            except Exception as e:
                                self.log.error("任务开始执行钉钉发送通知失败")
                                pass
                        if self.is_send_notify(task, State.SUCCESS, "dingTalk") and "dingding" in notify_type:
                            try:
                                ti.ding_notify_alert(State.SUCCESS)
                            except Exception as e:
                                self.log.error(traceback.format_exc())
                                pass
                        if self.is_send_notify(task, State.SUCCESS, "phone") and "phone" in notify_type:
                            try:
                                ti.phone_notify_alert()
                            except Exception as e:
                                self.log.error(traceback.format_exc())
                                pass

                elif now_state == 'failed':
                    if ti.try_number <= ti.max_tries:
                        ti.state = State.UP_FOR_RETRY
                    else:
                        ti.state = State.FAILED
                    ti.end_date = finish_date
                    ti.set_duration()
                    StateRecoveryHelper.sync_and_trigger(ti)

                    if ti.state == State.UP_FOR_RETRY and task.owner and task.email_on_retry:
                        notify_type = conf.get('email', 'notify_type')
                        if self.is_send_notify(task, State.RETRY, "email") and "email" in notify_type:
                            try:
                                ti.email_notify_alert(State.FAILED)
                            except Exception as e:
                                self.log.error("任务开始执行钉钉发送通知失败")
                                pass
                        if self.is_send_notify(task, State.RETRY, "dingTalk") and "dingding" in notify_type:
                            try:
                                ti.ding_notify_alert(State.FAILED)
                            except Exception as e:
                                self.log.error(traceback.format_exc())
                                pass
                        if self.is_send_notify(task, State.RETRY, "phone") and "phone" in notify_type:
                            try:
                                ti.phone_notify_alert()
                            except Exception as e:
                                self.log.error(traceback.format_exc())
                                pass

                    if ti.state == State.FAILED and task.owner and task.email_on_failure:
                        notify_type = conf.get('email', 'notify_type')
                        if self.is_send_notify(task, State.FAILED, "email") and "email" in notify_type:
                            try:
                                ti.email_notify_alert(State.START)
                            except Exception as e:
                                self.log.error("任务开始执行钉钉发送通知失败")
                                pass
                        if self.is_send_notify(task, State.FAILED, "dingTalk") and "dingding" in notify_type:
                            try:
                                ti.ding_notify_alert(State.SUCCESS)
                            except Exception as e:
                                self.log.error(traceback.format_exc())
                                pass
                        if self.is_send_notify(task, State.FAILED, "phone") and "phone" in notify_type:
                            try:
                                ti.phone_notify_alert()
                            except Exception as e:
                                self.log.error(traceback.format_exc())
                                pass
            except Exception as e:
                s = traceback.format_exc()
                log.error("[DataStudio Pipeline] state recovery failed,task_name : {},execution_date : {}".format(ti.dag_id,ti.execution_date.strftime('%Y-%m-%d %H:%M:%S.%f')))
                log.error(s)
                continue

    @staticmethod
    def is_send_notify(task,state,type):
        cur_task_param = TaskDesc.get_task(task_name=task.task_id).extra_param
        if cur_task_param.has_key(state):
            type = cur_task_param[state]["alertType"]
            if type.contains(type):
                return True
        else:
            if type == "dingTalk":
                if cur_task_param.has_key("dingAlert"):
                    return True
            if type == "phone":
                if cur_task_param.has_key("phoneAlert"):
                    return True
        return False



    @staticmethod
    def sync_and_trigger(ti):
        log.info("[DataStudio Pipeline] The status of task {}/{} needs to be recovered ".format(ti.dag_id,ti.execution_date.strftime('%Y-%m-%d %H:%M:%S.%f')))
        TriggerHelper.task_finish_trigger(ti)
        TaskInstance.recovery_ti_state(ti.dag_id,ti.task_id,ti.execution_date,state=ti.state,duration=ti.duration,end_date=ti.end_date)


class StateRecoveryProccess(LoggingMixin):
    def __init__(self):
        self.timeout = -1

    def start(self):
        process = multiprocessing.Process(
            target=recovery_process,
            args=(),
        )
        process.start()