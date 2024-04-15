# -*- coding: utf-8 -*-
import unittest
from datetime import datetime

import pendulum

from airflow.models import DagRun
from airflow.shareit.models.task_desc import TaskDesc
from airflow.shareit.models.task_life_cycle import TaskLifeCycle
from airflow.utils import timezone


class FindExportCycleTest(unittest.TestCase):

    def test_find_export_cycle(self):
        print TaskLifeCycle.find_export_cycle()

    def test_add_cycle(self):
        task_name = 'shareit_workflow_n2_task1'
        execution_date = pendulum.parse('2022-12-09 00:00:00')
        TaskLifeCycle.add_cycle(task_name,execution_date)
        TaskLifeCycle.add_cycle(task_name, execution_date,backfill_time = 1670522157722,start_time=1670522157722)
        # TaskLifeCycle.add_cycle(task_name, execution_date, clear_time=1670522157722)
        # TaskLifeCycle.add_cycle(task_name, execution_date, first_pipeline_time=1670522157722)
        # TaskLifeCycle.add_cycle(task_name, execution_date, ready_time=1670522157722)
        # TaskLifeCycle.add_cycle(task_name, execution_date, start_time=1670522157722)

    def test_update_cycle_first(self):
        task_name = 'shareit_workflow_n2_task1'
        execution_date = pendulum.parse('2022-12-09 00:00:00')
        TaskLifeCycle.update_cycle_first(task_name,execution_date,start_time = 1670522303721)
        TaskLifeCycle.update_cycle_first(task_name, execution_date, ready_time=1670522303721)
