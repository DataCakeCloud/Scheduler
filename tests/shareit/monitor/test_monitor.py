# -*- coding: utf-8 -*-
import json
import time

import unittest
from datetime import datetime, timedelta

import pendulum
from airflow import dag
from airflow.shareit.check.check_dependency_helper import CheckDependecyHelper
from airflow.shareit.monitor.monitor import Monitor
from airflow.shareit.monitor.monitor_service import MonitorRun
from airflow.shareit.scheduler.external_helper import ExternalManager
from airflow.shareit.scheduler.sensor_job_helper import ReadyTimeSensenJob, sensor_job_readytime_processor
from airflow.shareit.utils.date_calculation_util import DateCalculationUtil
from airflow.shareit.utils.date_util import DateUtil
from airflow.shareit.utils.granularity import Granularity
from airflow.shareit.utils.state import State
from airflow.shareit.utils.task_cron_util import TaskCronUtil

from airflow.shareit.utils.task_util import TaskUtil
from airflow.utils import timezone
from croniter import croniter, croniter_range

from airflow.shareit.models.sensor_info import SensorInfo
from airflow.shareit.service.task_service import TaskService
from airflow.utils.timezone import beijing, utcnow
from airflow.utils.db import provide_session
from airflow.shareit.models.dag_dataset_relation import DagDatasetRelation
from airflow.shareit.models.trigger import Trigger
from airflow.shareit.models.task_desc import TaskDesc
from airflow.shareit.service.dataset_service import DatasetService


class MonitorTest(unittest.TestCase):

    def test_mointor_run(self):
        # MonitorRun.run(5000)
        print 1

    def test_push(self):
        monitor = Monitor()
        # monitor.set_task_runtime('task_life_cycle','2022-12-15 00:00:00.000','1',1000)
        # monitor.push_prometheus_metrics()