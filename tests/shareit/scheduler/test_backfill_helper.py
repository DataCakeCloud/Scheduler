# -*- coding: utf-8 -*-
import pendulum

from airflow.shareit.models.backfill_job import BackfillJob
from airflow.shareit.models.task_desc import TaskDesc
from airflow.shareit.scheduler.backfill_helper import BackfillHelper
from airflow.utils import timezone

from airflow.shareit.models.trigger import Trigger
from airflow.shareit.utils.task_util import TaskUtil

from airflow.shareit.utils.task_manager import analysis_task, analysis_dag, format_dag_params
import unittest
from datetime import datetime, timedelta
from airflow.utils.timezone import beijing

from airflow.shareit.scheduler.scheduler_helper import SchedulerManager, scheduler, QuickScheduler, BackfillProccess, \
    push_fast_execute_proccessor


class BackfillHelperTest(unittest.TestCase):
    def test_start_backfill_job(self):
        jobs = BackfillJob.find_waiting_jobs()
        BackfillHelper().start_backfill_job(48)