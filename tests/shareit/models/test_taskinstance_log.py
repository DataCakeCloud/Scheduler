# -*- coding: utf-8 -*-

import unittest
from datetime import datetime

import pendulum

from airflow.models import DagRun, TaskInstance
from airflow.shareit.constant.taskInstance import MANUALLY
from airflow.shareit.models.task_desc import TaskDesc
from airflow.shareit.models.taskinstance_log import TaskInstanceLog
from airflow.utils import timezone


class TaskInstanceLogTest(unittest.TestCase):
    def test_get_taskinstance(self):
        task_name='schedule_improve'
        execution_date = '2022-08-12 13:00:00'
        execution_date=pendulum.parse(execution_date)
        res= TaskInstanceLog.find_by_execution_date(task_name=task_name,execution_date=execution_date)
        for r in res:
            print r


    def test_update_name(self):
        old_name = 'taskinstance_log'
        new_name = 'taskinstance_log_1'
        TaskInstanceLog.update_name(old_name,new_name)

    def test_find_one_hour_ti(self):
        start, end = timezone.before_hour()
        tis = TaskInstanceLog.find_one_hour_ti(start)
        failed_tis = [ti for ti in tis if ti.state == 'failed' and ti.task_type != MANUALLY]
        distinct_tis = set([ti.dag_id for ti in tis])
        distinct_failed_tis = set([ti.dag_id for ti in failed_tis])
        print round(len(failed_tis) * 1.0 * 100 / len(tis), 2)
        print round(len(distinct_failed_tis) * 1.0 * 100 / len(distinct_tis), 2)
