# -*- coding: utf-8 -*-
import json
import time

import unittest
from datetime import datetime, timedelta

import pendulum
from airflow import dag
from airflow.models import TaskInstance
from airflow.shareit.check.check_dependency_helper import CheckDependecyHelper
from airflow.shareit.constant.taskInstance import PIPELINE
from airflow.shareit.scheduler.external_helper import ExternalManager
from airflow.shareit.scheduler.sensor_job_helper import ReadyTimeSensenJob, sensor_job_readytime_processor
from airflow.shareit.trigger.trigger_helper import TriggerHelper
from airflow.shareit.utils.date_calculation_util import DateCalculationUtil
from airflow.shareit.utils.date_util import DateUtil
from airflow.shareit.utils.granularity import Granularity
from airflow.shareit.utils.state import State
from airflow.shareit.utils.task_cron_util import TaskCronUtil
from airflow.shareit.utils.task_manager import get_dag

from airflow.shareit.utils.task_util import TaskUtil
from airflow.shareit.workflow.workflow_state_sync import WorkflowStateSync
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


class DatasetServiceTest(unittest.TestCase):

    def test_ti_push_state_2_wi(self):
        execution_date = pendulum.parse('2023-02-27 13:00:00').replace(tzinfo=timezone.beijing)
        ti = TaskInstance.find_by_execution_date(dag_id='1_22333',execution_date=execution_date)
        if ti:
            WorkflowStateSync.ti_push_state_2_wi(workflow_id=10050,cur_ti=ti[0])

    def test_open_wi(self):
        execution_date = pendulum.parse('2022-12-06 00:00:00').replace(tzinfo=timezone.beijing)
        WorkflowStateSync.open_wi(workflow_id='14',execution_date=execution_date)
