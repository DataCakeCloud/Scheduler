# -*- coding: utf-8 -*-
import traceback

from airflow import LoggingMixin
from airflow.shareit.models.alert import Alert
from airflow.shareit.models.event_trigger_mapping import EventTriggerMapping
from airflow.shareit.models.fast_execute_task import FastExecuteTask
from airflow.shareit.service.task_service import TaskService
from airflow.shareit.utils.task_manager import get_dag
from airflow.utils.db import provide_session

log = LoggingMixin().logger
class PushSchedulerHelper(LoggingMixin):
    @staticmethod
    @provide_session
    def push_task_execute(fast_execute_task,execution_date,session=None):
        transaction=True
        try:
            #     注意这里执行的仍是原有的任务名！虽然实际上是两个任务，但是对于用户来说只有一个任务
            dag = get_dag(fast_execute_task.real_task_name)
            FastExecuteTask.close_task(task_name=fast_execute_task.task_name,transaction=transaction,session=session)
            TaskService.task_execute(task_name=fast_execute_task.real_task_name, execution_date=execution_date,
                                     external_conf=fast_execute_task.external_conf,dag=dag,transaction=transaction,session=session)
            # raise Exception()
            # Alert.add_alert_job(task_name=fast_execute_task.real_task_name, execution_date=execution_date,transaction=transaction,session=session)
            # raise  Exception()
            session.commit()
        except Exception :
            session.rollback()
            s = traceback.format_exc()
            log.error("[DataStudio Pipeline] push_task_execute error")
            log.error(s)

    @staticmethod
    @provide_session
    def event_trigger_task_execute(etm,execution_date,session=None):
        transaction = True
        try:
            external_conf = {'uuid':etm.uuid}
            if etm.callback_info:
                external_conf["callback_info"] = etm.callback_info

            dag = get_dag(etm.task_name)

            EventTriggerMapping.update(uuid=etm.uuid,is_execute=True,execution_date=execution_date,session=session)
            TaskService.task_execute(task_name=etm.task_name, execution_date=execution_date,
                                     external_conf=external_conf,dag=dag,transaction=transaction,session=session)
        except :
            log.error("[DataStudio Pipeline] event trigger execute error")
            log.error(traceback.format_exc())