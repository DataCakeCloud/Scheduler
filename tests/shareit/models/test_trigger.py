# -*- coding: utf-8 -*-

import unittest
from datetime import datetime

import pendulum

from airflow.models import DagRun, TaskInstance
from airflow.shareit.models.task_desc import TaskDesc
from airflow.shareit.models.taskinstance_log import TaskInstanceLog
from airflow.shareit.models.trigger import Trigger
from airflow.utils import timezone
from airflow.utils.timezone import beijing


class TriggerTest(unittest.TestCase):
    def test_delete_tri(self):
        task_name='task12091140'
        now_date = timezone.utcnow().replace(tzinfo=beijing)
        Trigger.delete_by_execution_date_range(task_name=task_name,start_date=now_date)
        # res= TaskInstanceLog.find_by_execution_date(task_name=task_name,execution_date=execution_date)
        # for r in res:
        #     print r

