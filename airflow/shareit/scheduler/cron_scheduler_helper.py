# -*- coding: utf-8 -*-
import datetime
import logging
import os
import signal
import traceback

import jinja2
import requests
import six

from airflow import AirflowException
from airflow.models import DagRun, TaskInstance
from airflow.shareit.check.check_dependency_helper import CheckDependecyHelper
from airflow.shareit.models.dataset import Dataset
from airflow.shareit.models.dataset_partation_status import DatasetPartitionStatus
from airflow.shareit.models.event_depends_info import EventDependsInfo
from airflow.shareit.trigger.trigger_helper import TriggerHelper
from airflow.shareit.utils.constant import Constant
from airflow.shareit.utils.date_calculation_util import DateCalculationUtil
from airflow.shareit.utils.date_util import DateUtil
from airflow.shareit.utils.granularity import Granularity
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.shareit.utils.state import State
from airflow.utils.helpers import reap_process_group
from airflow.configuration import conf

from airflow.shareit.utils.state import State as myState
from airflow.shareit.utils.task_manager import get_dag
from airflow.shareit.models.trigger import Trigger
from airflow.shareit.service.dataset_service import DatasetService
from airflow.shareit.models.dag_dataset_relation import DagDatasetRelation


class CronScheduelrManager(LoggingMixin):
    def __init__(self):
        return

    def main_scheduler_process(self):
        return
    #     from airflow.utils import timezone
    #     from airflow.models.dag import DagModel
    #     from airflow.shareit.utils.state import State as State_new
    #     triggers = Trigger.get_untreated_cron_triggers()
    #     # 如果被backfill，其实就不属于时间触发了，所以不在这里处理
    #
    #     for trigger in triggers:
    #         if not DagModel.is_active_and_on(trigger.dag_id):
    #             continue
    #
    #         try:
    #             # 判断上游
    #             is_ready,ready_time = CheckDependecyHelper.check_dependency(trigger.dag_id, trigger.execution_date)
    #             if not is_ready:
    #                 continue
    #         except Exception:
    #             s = traceback.format_exc()
    #             self.logger.error("check dependency failed !!!!!!!!!!!")
    #             self.logger.error(s)
    #             continue
    #
    #         # todo 配置run_id,考虑后续执行队列时的问题
    #         run_id = "{type}__{date}".format(type=trigger.trigger_type, date=trigger.execution_date.isoformat())
    #         try:
    #             exe_time = trigger.execution_date.replace(tzinfo=timezone.beijing)
    #             dag = get_dag(trigger.dag_id)
    #             if dag is None:
    #                 self.log.warning("[DataStudio Pipeline] Main_scheduler_process: {dag_id} not exists!!!".format(
    #                     dag_id=trigger.dag_id))
    #                 continue
    #
    #             can_tri = True
    #             for task in six.itervalues(dag.task_dict):
    #                 # 在任务给定的开始时间之前的ti不生成
    #                 if task.start_date is not None and DateUtil.date_compare(task.start_date, exe_time, eq=False):
    #                     can_tri = False
    #                 # 在任务给定的结束时间之后的ti不生成
    #                 if task.end_date is not None and DateUtil.date_compare(exe_time, task.end_date, eq=False):
    #                     can_tri = False
    #
    #             if can_tri == False:
    #                 # 将trigger更新为终止状态，避免下次再轮询也避免修改开始/结束日期后任务被调起一大堆
    #                 trigger.sync_state_to_db(state=State_new.TASK_STOP_SCHEDULER)
    #                 self.log.info("[DataStudio Pipeline] not in date range,cant be trigged, dag:{dag_id},time:{time}".format(
    #                     dag_id=trigger.dag_id, time=str(exe_time)))
    #                 continue
    #
    #             tis = TaskInstance.find_by_execution_dates(dag_id=trigger.dag_id,task_id=trigger.dag_id,execution_dates=[exe_time])
    #
    #             if len(tis) > 0:
    #                 self.log.info("[DataStudio Pipeline] cronTrigger {task} {time} ,but taskInstance already exists"
    #                               .format(task=trigger.dag_id,time=exe_time.strftime('%Y-%m-%d %H:%M:%S')))
    #                 for ti in tis:
    #                     if ti.state in State.finished():
    #                         trigger.sync_state_to_db(state=ti.state)
    #                     else:
    #                         trigger.sync_state_to_db(state=State.RUNNING)
    #             # dr = DagRun.find(dag_id=trigger.dag_id, execution_date=exe_time)
    #             # if len(dr) > 0:
    #             #     # 理论上来说，cronTrigger只做首次触发，所以是绝对不可能存在dagrun的
    #             #     self.log.info("[DataStudio Pipeline] cronTrigger {task} {time} ,but dagrun already exists"
    #             #                   .format(task=trigger.dag_id,time=exe_time.strftime('%Y-%m-%d %H:%M:%S')))
    #             #     for tmp_dr in dr:
    #             #         if tmp_dr.state in State.finished():
    #             #             trigger.sync_state_to_db(state=tmp_dr.state)
    #             #         else:
    #             #             trigger.sync_state_to_db(state=State.RUNNING)
    #             #     continue
    #             # 创建taskInstance
    #             self.log.info("[DataStudio Pipeline] create taskinstance {task} {time}")
    #             TriggerHelper.create_taskinstance(dag.task_dict,exe_time)
    #             # dr = dag.create_dagrun(
    #             #     run_id=run_id,
    #             #     execution_date=exe_time,
    #             #     state=State.RUNNING,
    #             #     conf=None,
    #             #     external_trigger=False
    #             # )
    #             trigger.sync_state_to_db(state=State.RUNNING)
    #             # push running state to ds backend 由于效率问题这个地方状态不用重试
    #             # todo
    #             # if dr.is_latest_runs():
    #             #     url = conf.get("core", "default_task_url")
    #             #     data = {"taskName": trigger.dag_id, "status": State.RUNNING}
    #             #     requests.put(url=url, data=data)
    #         except Exception as e:
    #             s = traceback.format_exc()
    #             self.log.error(s)
    #             self.log.error("clear or create dagrun failed !", exc_info=True)
    #             continue