# -*- coding: utf-8 -*-
import unittest

from airflow.utils import timezone

from airflow.shareit.jobs.cron_job import DsCronJob
from airflow.shareit.models.fast_execute_task import FastExecuteTask
from airflow.shareit.scheduler.cron_scheduler_helper import CronScheduelrManager
from airflow.shareit.scheduler.push_scheduler import PushSchedulerHelper
from airflow.shareit.utils.date_util import DateUtil
from airflow.utils.timezone import beijing


class PushSchedulerTest(unittest.TestCase):
    def test_push_task_execute(self):
        task_list = FastExecuteTask.find_all_waiting_task()
        execution_date_init = DateUtil.format_millisecond(timezone.utcnow().replace(tzinfo=beijing))
        PushSchedulerHelper.push_task_execute(task_list[0],execution_date_init)