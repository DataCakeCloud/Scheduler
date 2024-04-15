# -*- coding: utf-8 -*-
import unittest

from airflow.shareit.jobs.cron_job import DsCronJob


class CronJobTest(unittest.TestCase):
    def test_execute(self):
        DsCronJob().run()