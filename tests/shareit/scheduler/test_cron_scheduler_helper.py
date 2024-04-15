# -*- coding: utf-8 -*-
import unittest

from airflow.shareit.jobs.cron_job import DsCronJob
from airflow.shareit.scheduler.cron_scheduler_helper import CronScheduelrManager


class CronSchedulerHelperTest(unittest.TestCase):
    def test_execute(self):
        CronScheduelrManager().main_scheduler_process()