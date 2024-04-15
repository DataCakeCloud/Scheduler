# -*- coding: utf-8 -*-

import unittest
from datetime import datetime

import pendulum

from airflow.models import DagRun, TaskInstance
from airflow.shareit.models.task_desc import TaskDesc
from airflow.shareit.utils.date_util import DateUtil
from airflow.utils import timezone
from airflow.utils.timezone import beijing


class TaskInstanceTest(unittest.TestCase):
    def test_get_taskinstance(self):
        res= TaskInstance.list_all_scheduled_task()
        print res

    def test_check_success(self):
        task_id = 'schedule_improve'
        dt = datetime(year=2022, month=8, day=10).replace(tzinfo=timezone.beijing)
        print TaskInstance.check_is_success(task_id=task_id,execution_date=dt)

    def test_refreh(self):
        task_id = '36_20496'
        execution_date = pendulum.parse("2023-02-14 08:00:00").replace(tzinfo=timezone.beijing)
        execution_date = datetime(year=2023, month=02, day=15, hour=0,minute=0,second=0).replace(tzinfo=timezone.beijing)
        # execution_date = datetime(year=2022, month=12, day=20, hour=18,minute=0,second=0).replace(tzinfo=timezone.beijing)
        tis = TaskInstance.find_by_execution_dates(dag_id=task_id, task_id=task_id, execution_dates=[execution_date])
        ti = tis[0]
        print ti
        # # ti.check_and_change_state_before_execution()
        # print ti
        # ti.add_taskinstance_log()
        # temp_ts = DateUtil.get_timestamp(timezone.utcnow().replace(tzinfo=beijing))
        # print "1" + str(temp_ts)

    def test_is_latest_runs(self):
        task_id = 'test_update_and_execute'
        execution_date = datetime(year=2022, month=8, day=12, hour=13,minute=0,second=0).replace(tzinfo=timezone.beijing)
        TaskInstance.is_latest_runs(task_id,execution_date)

    def test_find_taskinstance_by_sql(self):
        print TaskInstance.find_taskinstance_by_sql()

    def test_ding_group_chat_notify_alert(self):
        task_id = '1_22560'
        execution_date = pendulum.parse("2023-04-04 00:00:00").replace(tzinfo=timezone.beijing)
        # execution_date = datetime(year=2023, month=02, day=15, hour=0,minute=0,second=0).replace(tzinfo=timezone.beijing)
        # execution_date = datetime(year=2022, month=12, day=20, hour=18,minute=0,second=0).replace(tzinfo=timezone.beijing)
        tis = TaskInstance.find_by_execution_dates(dag_id=task_id, task_id=task_id, execution_dates=[execution_date])
        ti = tis[0]
        print ti
        ti.ding_group_chat_notify_alert(batch_id='111')

    def test_add_taskinstance_log(self):
        task_id = '32_58005'
        execution_date = pendulum.parse("2024-03-28 00:00:00").replace(tzinfo=timezone.beijing)
        tis = TaskInstance.find_by_execution_dates(dag_id=task_id, task_id=task_id, execution_dates=[execution_date])
        tis[0].add_taskinstance_log()