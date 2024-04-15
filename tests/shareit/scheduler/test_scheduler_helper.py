# -*- coding: utf-8 -*-
"""
author: zhangtao
Copyright (c) 2021/6/25 Shareit.com Co., Ltd. All Rights Reserved.
"""
# -*- coding: utf-8 -*-
import pendulum

from airflow.shareit.models.task_desc import TaskDesc
from airflow.utils import timezone

from airflow.shareit.models.trigger import Trigger
from airflow.shareit.utils.task_util import TaskUtil

"""
author: zhangtao
Copyright (c) 2021/5/24 Shareit.com Co., Ltd. All Rights Reserved.
"""
import time

from airflow.shareit.utils.task_manager import analysis_task, analysis_dag, format_dag_params
import unittest
from datetime import datetime, timedelta
from airflow.utils.timezone import beijing

from airflow.shareit.scheduler.scheduler_helper import SchedulerManager, scheduler, QuickScheduler, BackfillProccess, \
    push_fast_execute_proccessor


class SchedulerHelperTest(unittest.TestCase):

    def test_main_scheduler_process(self):
        SchedulerManager.main_scheduler_process(7)

    def test_check_dependency(self):
        SchedulerManager._check_dependency(dag_id="test_weekly", execution_date=datetime(2022, 03, 28, 0, 0, 0, tzinfo=beijing))

    def test_external_scheduler(self):
        TaskUtil().external_data_processing()

    def test_scheduler(self):
        dag_id= '1_17397'
        td = TaskDesc.get_task(task_name='1_17393')
        execution_date = pendulum.parse('2023/02/14 00:00:00.000').replace(tzinfo=timezone.beijing)
        triggers = Trigger.get_untreated_data_triggers()
        triggers = Trigger.find_by_execution_dates(dag_id=dag_id,execution_dates=[execution_date.replace(tzinfo=timezone.beijing)])
        scheduler(triggers)

    def test_quick_scheduler(self):
        QuickScheduler().start()
        # a=[1,2,3]
        # print filter(lambda x:x == 1,a)
    def test_backfill_proccess(self):
        BackfillProccess().start()
        # a=[1,2,3]
        # print filter(lambda x:x == 1,a)

    def test_backfill_trigger_process(self):
        SchedulerManager().backfill_trigger_process()

    def test_push_fast_execute_proccessor(self):
        push_fast_execute_proccessor()