# -*- coding: utf-8 -*-
"""
author: zhangtao
Copyright (c) 2021/6/30 Shareit.com Co., Ltd. All Rights Reserved.
"""
import json
import time

import unittest
from datetime import datetime

from croniter import croniter

from airflow import macros

from airflow.shareit.jobs.unified_sensor_job import UnifiedSensorJob
from airflow.shareit.operators.script_operator import ScriptOperator
from airflow.shareit.service.diagnosis_service import DiagnosisService
from airflow.shareit.service.taskinstance_service import TaskinstanceService
from airflow.shareit.utils import task_manager
from airflow.shareit.utils.task_util import TaskUtil
from airflow.shareit.www import pipeline
from airflow.utils import timezone

import pendulum

from airflow.shareit.check.check_dependency_helper import CheckDependecyHelper
from airflow.shareit.models.sensor_info import SensorInfo
from airflow.shareit.service.dataset_service import DatasetService
from airflow.shareit.service.task_service import TaskService
from airflow.shareit.trigger.trigger_helper import TriggerHelper
from airflow.shareit.utils.granularity import Granularity
from airflow.shareit.utils.state import State
from airflow.utils.timezone import beijing, my_make_naive, utcnow
from airflow.utils.db import provide_session
from airflow.models.dag import DagModel
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.shareit.models.dag_dataset_relation import DagDatasetRelation
from airflow.shareit.models.trigger import Trigger
from airflow.shareit.models.task_desc import TaskDesc


class TaskinstanceServiceTest(unittest.TestCase):

    @provide_session
    def test_get_record(self, session=None):
        task_name='schedule_improve'
        execution_date = '2022-08-12 13:00:00'
        execution_date=pendulum.parse(execution_date)
        res = TaskinstanceService.get_taskinstance_record(task_name=task_name,execution_date=execution_date)
        for i in res :
            print i

    def test_get_ti_state_by_uuid(self):
        uuid = 'cdc07214-fed4-11ed-afdc-f620b3153039'
        state = TaskinstanceService.get_ti_state_by_uuid(uuid)
        print state

    def test_get_page(self):
        end_date=None
        task_filter = {
            "ds_task_name": 'test_select_1',
            "cycle": 'daily',
            # "template_code": template_code,
            "group_uuid": 'groupe6e2pyci'
        }
        ti_filter = {
            # "state": 'success',
            # "start_date": pendulum.parse(start_date),
            "end_date": pendulum.parse(end_date) if end_date else utcnow(),
            # "schedule_type": 'backCalculation'
            "backfill_label":"ceshi"
        }
        page_filter = {
            "page": 1,
            "size": 10,
        }

        result = TaskinstanceService.get_ti_page(task_filter=task_filter,ti_filter=ti_filter,page_filter=page_filter,tenant_id=32)
        print json.dumps(result)
