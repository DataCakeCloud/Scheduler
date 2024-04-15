# -*- coding: utf-8 -*-
import unittest

from airflow.shareit.jobs.cron_job import DsCronJob
from airflow.shareit.scheduler.cron_scheduler_helper import CronScheduelrManager
from airflow.shareit.scheduler.state_recovery_helper import StateRecoveryHelper


class stateRecoveryHelperTest(unittest.TestCase):
    def test_recovery_state(self):
        StateRecoveryHelper().recovery_state()