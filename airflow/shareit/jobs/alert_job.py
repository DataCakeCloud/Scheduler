# -*- coding: utf-8 -*-
import json
import multiprocessing
import threading
import time
import traceback
from collections import defaultdict
from datetime import timedelta

import requests
from datetime import datetime
from airflow.exceptions import AirflowTaskTimeout
from airflow.shareit.constant.taskInstance import PIPELINE_QUEUE, BACKFILL_QUEUE

from airflow.shareit.models.regular_alert import RegularAlert
from airflow.shareit.monitor.push_monitor import MonitorPush
from airflow.utils.timeout import timeout

from airflow.shareit.utils.date_util import DateUtil
from airflow.configuration import conf
from airflow.models import TaskInstance, DagModel
from airflow import LoggingMixin
from airflow.models import TaskInstance, DagModel
from airflow.shareit.models.alert import Alert
from airflow.shareit.models.alert_detail import AlertDetail
from airflow.shareit.models.callback import Callback
from airflow.shareit.models.task_desc import TaskDesc
from airflow.shareit.monitor.monitor_service import MonitorRun
from airflow.shareit.utils import task_manager, alarm_adapter
from airflow.shareit.utils.alarm_adapter import send_ding_notify, build_alert_content, get_receiver_list, \
    build_callback_failed_content, send_email_by_notifyAPI, build_import_sharestore_state_notify,  send_notify_by_alert_type
from airflow.shareit.utils.callback_util import CallbackUtil
from airflow.utils import timezone
from airflow.utils.state import State
from airflow.utils.timezone import beijing, make_naive

log = LoggingMixin().logger
def callback_processor():
    monitor_push = MonitorPush()
    while True :
        log.info("[DataStudio Pipeline] start callbackjob loop....")
        loop_start_time = time.time()
        try:
            with timeout(3):
                monitor_push.push_to_prometheus(job_name="alert_job", gauge_name="alert_job",
                                                documentation="alert job")
        except AirflowTaskTimeout as e:
            log.error("[DataStudio Pipeline] push metrics timeout")
        except Exception as e:
            log.error("[DataStudio Pipeline] alertjob push metrics failed")
        running_callbacks = Callback.list_running_callback()
        for callback in running_callbacks:
            url = callback.callback_url
            body = json.loads(callback.callback_body)
            state = callback.state
            nums = callback.check_nums
            try:
                nums += 1
                resp = CallbackUtil.callback_post(url,body)
                # 为push平台定制的内容...
                if "resultCode" in resp.keys() and resp['resultCode'] == 200:
                    state = 'success'
            except Exception as e:
                log.error("[DataStudio Pipeline] callback failed, id :{} ,error: {}".format(str(callback.id),str(e)))

            if nums > 10 and state != 'success':
                state = 'failed'
                notify_type = conf.get('email', 'notify_type')
                content = build_callback_failed_content(task_name=callback.relation_task_name, taskinstance_id=callback.taskinstance_id if callback.taskinstance_id else '')
                if "dingding" in notify_type:
                    # 回调超过10次失败，发送钉钉通知
                    owner = TaskDesc.get_owner(task_name=callback.relation_task_name)
                    receiver = get_receiver_list(receiver=owner)
                    send_ding_notify(title='DataCake回调失败通知', content=content, receiver=receiver)
                if "email" in notify_type:
                    emails = TaskDesc.get_email(task_name=callback.relation_task_name)
                    receiver = get_receiver_list(receiver=emails)
                    send_email_by_notifyAPI(title='DataCake回调失败通知', content=content, receiver=receiver)

            Callback.update_callback(id=callback.id,nums=nums,state=state)

        loop_end_time = time.time()
        loop_duration = loop_end_time - loop_start_time
        # 五分钟一次轮询
        if loop_duration < 300:
            sleep_length = 300 - loop_duration
            log.info(
                "[DataStudio Pipeline] alertjob: sleeping for {0:.2f} seconds to prevent excessive logging"
                    .format(sleep_length))
            time.sleep(sleep_length)

class AlertJob(LoggingMixin):
    def __init__(
            self,
            timeout=-1,
            check_parallelism=3,
            *args, **kwargs):
        self.timeout = timeout
        self.check_parallelism = check_parallelism
        self.scheduled_cache = {}
        self.scheduled_alert_interval = timedelta(minutes=20)

    def run(self):
        try:
            self._execute()
        except Exception:
            raise
    def _execute(self):
        execute_start_time = timezone.utcnow()
        # CallbackJob().start()
        # MonitorRun().start()
        while (timezone.utcnow() - execute_start_time).total_seconds() < self.timeout or self.timeout == -1:
        # if True:
            try:
                self.log.info("[DataStudio Pipeline] alert: start alert loop")
                loop_start_time = time.time()
                # now_date = timezone.utcnow().replace(tzinfo=beijing)
                #
                # # try:
                # #     self.scheduled_task_notify()
                # # except :
                # #     self.log.error(traceback.format_exc())
                # #     self.log.error("[DataStudio Pipeline] alert: scheduled_task_notify failed")
                # job_dict = {}
                # alert_jobs = Alert.list_all_open_alert_job()
                # for job in alert_jobs:
                #     if job.task_name in job_dict.keys():
                #         job_dict[job.task_name].append(job.execution_date.replace(tzinfo=beijing))
                #     else :
                #         job_dict[job.task_name] = [job.execution_date.replace(tzinfo=beijing)]
                #
                # for task_name,exe_dates in job_dict.items():
                #     tis = TaskInstance.find_by_execution_dates(dag_id=task_name, task_id=task_name,
                #                                                  execution_dates=exe_dates)
                #     owner = TaskDesc.get_owner(task_name=task_name)
                #     receiver = get_receiver_list(receiver=owner)
                #     emails = TaskDesc.get_email(task_name=task_name)
                #     email_receiver = get_receiver_list(receiver=emails)
                #     for ti in tis :
                #         if ti.state == State.RUNNING:
                #             if (now_date - ti.start_date).total_seconds() > 20*60:
                #                 content = build_alert_content(task_name=ti.dag_id,execution_date=ti.execution_date)
                #                 notify_type = conf.get('email', 'notify_type')
                #                 if "dingding" in notify_type:
                #                     send_ding_notify(title='DataCake任务超时通知',content=content,receiver=receiver)
                #                 if "email" in notify_type:
                #                     send_email_by_notifyAPI(title='DataCake任务超时通知',content=content,receiver=email_receiver)
                #                 Alert.sync_state(task_name=task_name,execution_date=ti.execution_date,state='finished')
                try:
                    self.import_sharestore_alert_notify()
                except Exception as e:
                    self.log.error("[DataStudio Pipeline] alertjob: import sharestore报警任务失败")
                    self.log.error(traceback.format_exc())

                try:
                    self.sync_data_to_regular_alert()
                    self.check_regualer_alert()
                except Exception as e:
                    self.log.error("[Datacake Pipeline] alertjob: 定时任务告警失败 {}".format(e.message))

                try:
                    self.send_phone_alert()
                except Exception as e:
                    self.log.error("[Datacake Pipeline] alertjob: 电话告警失败 {}".format(e.message))



                loop_end_time = time.time()
                loop_duration = loop_end_time - loop_start_time
                if loop_duration < 60:
                    sleep_length = 60 - loop_duration
                    self.log.info(
                        "[DataStudio Pipeline] alertjob: sleeping for {0:.2f} seconds to prevent excessive logging"
                            .format(sleep_length))
                    time.sleep(sleep_length)
            except Exception as e:
                self.log.error(traceback.format_exc())


    def check_is_notify(self,task_id,execution_date,try_number):
        ads = AlertDetail.get_alerts(task_name=task_id,execution_date=execution_date,try_number=try_number)
        return False if ads else True


    def import_sharestore_alert_notify(self):
        tis = TaskInstance.get_all_import_sharestore_tasks()
        for ti in tis:
            if (timezone.utcnow().replace(tzinfo=beijing) - ti.update_date.replace(tzinfo=beijing)).total_seconds() > 10 * 60 * 60 \
                    and self.check_is_notify(task_id=ti.task_id, execution_date=ti.execution_date, try_number=ti.try_number):
                dag_id = ti.task_id[:-18]
                owner = TaskDesc.get_owner_by_ds_task_name(ds_task_name=dag_id)
                content = build_import_sharestore_state_notify(dag_id=dag_id, task_name=ti.task_id,
                                                               execution_date=ti.execution_date)
                send_ding_notify(title="DataCake 提示信息", content=content, receiver=get_receiver_list(owner))
                alert_detail = AlertDetail()
                alert_detail.sync_alert_detail_to_db(task_name=ti.task_id,
                                                     execution_date=ti.execution_date,
                                                     owner=owner,
                                                     try_number=ti.try_number
                                                     )


    def count_task_types(self,task_list, task_types):
        return sum(ti.task_type in task_types for ti in task_list)


    def sync_data_to_regular_alert(self):
        from croniter import croniter
        from airflow.shareit.utils.state import State
        def get_value_from_dict(dict_obj, key):
            if isinstance(dict_obj,unicode):
                dict_obj = json.loads(dict_obj)
            if not isinstance(dict_obj, dict):
                return None
            return dict_obj[key] if key in dict_obj.keys() else None

        tasks = TaskDesc.get_regular_alert_all_task()
        for task in tasks:
            if not DagModel.is_active_and_on(dag_id=task.task_name):
                continue
            regular_alert = get_value_from_dict(task.extra_param,"regularAlert")
            if regular_alert:
                triggerCondition = get_value_from_dict(regular_alert,"triggerCondition")
                notifyCollaborator = get_value_from_dict(regular_alert,"notifyCollaborator")
                graularity = get_value_from_dict(regular_alert,"graularity")
                offset = get_value_from_dict(regular_alert,"offset")
                alertType = ",".join(get_value_from_dict(regular_alert,"alertType"))
                checkTime = get_value_from_dict(regular_alert, "checkTime")
            nowDate = timezone.utcnow()
            cron = croniter(task.crontab, nowDate)
            cron.tzinfo = beijing
            trigger_execution_date = cron.get_prev(datetime)
            check_time = DateUtil.get_check_time(trigger_execution_date,graularity,checkTime,offset)
            ra = RegularAlert()
            ra.sync_regular_alert_to_db(
                task_name = task.task_name,
                execution_date = make_naive(trigger_execution_date),
                alert_type = alertType,
                granularity = graularity,
                check_time = check_time,
                state = State.CHECKING,
                trigger_condition = triggerCondition,
                is_notify_collaborator = notifyCollaborator
            )

    def check_regualer_alert(self):
        from airflow.shareit.utils.state import State
        ras = RegularAlert.get_regular_alert_all_task()
        notStart = [State.TASK_UP_FOR_RETRY, State.FAILED, State.SUCCESS, State.RUNNING]
        for ra in ras:
            if not DagModel.is_active_and_on(dag_id=ra.task_name):
                continue
            nowDate = timezone.utcnow().replace(tzinfo=beijing)
            if DateUtil.date_compare(nowDate, ra.check_time):
                try:
                    tis = TaskInstance.find_by_execution_date(dag_id=ra.task_name, task_id=ra.task_name,
                                                              execution_date=ra.execution_date.replace(tzinfo=beijing))
                    ti = tis[0] if tis else None
                    owners = TaskDesc.get_owner_by_ds_task_name(
                        ra.task_name) if ra.is_notify_collaborator else TaskDesc.get_owner(task_name=ra.task_name)
                    if ra.trigger_condition == 'notStart':
                        if not ti or (ti.state not in notStart):
                            send_notify_by_alert_type(ra.task_name, ra.execution_date.isoformat(), ra.alert_type,
                                                      owners, "任务未开始运行")
                    elif ra.trigger_condition == 'notSuccess':
                        if not ti or ti.state != State.SUCCESS:
                            send_notify_by_alert_type(ra.task_name, ra.execution_date.isoformat(), ra.alert_type,
                                                      owners,
                                                      "任务未运行成功")
                    else:
                        self.log.error(
                            "没有当前告警类型{trigger_condition}".format(trigger_condition=ra.trigger_condition))
                    RegularAlert.update_ra_state(task_name=ra.task_name, execution_date=ra.execution_date,
                                                 state=State.SUCCESS, trigger_condition=ra.trigger_condition)
                except Exception as e:
                    self.log.error(traceback.format_exc())
                    self.log.error(
                        "[DataCake Pipeline] alertjob: send notify failed.")
                    RegularAlert.update_ra_state(task_name=ra.task_name, execution_date=ra.execution_date,
                                                 state=State.FAILED, trigger_condition=ra.trigger_condition)

    def send_phone_alert(self):
        ras = RegularAlert.get_task_alert_all_task()
        for ra in ras:
            owners = TaskDesc.get_owner_by_ds_task_name(
                ra.task_name) if ra.is_notify_collaborator else TaskDesc.get_owner(task_name=ra.task_name)
            if ra.trigger_condition == State.SUCCESS:
                alert_head = "任务执行成功"
            elif ra.trigger_condition == State.FAILED:
                alert_head = "任务执行失败"
            elif ra.trigger_condition == State.RETRY:
                alert_head = "任务正在重试"
            else:
                alert_head = "任务开始执行"
            phone_alert = threading.Thread(target=alarm_adapter.send_telephone_list, args=(
                get_receiver_list(owners),ra.task_name ,ra.execution_date,ra.trigger_condition,alert_head))
            phone_alert.start()






class CallbackJob(LoggingMixin):
    def __init__(
            self,
            timeout=-1,
            check_parallelism=3,
            *args, **kwargs):
        self.timeout = timeout
        self.check_parallelism = check_parallelism

    def start(self):
        process = multiprocessing.Process(
            target=callback_processor,
            args=(),
        )
        process.start()
