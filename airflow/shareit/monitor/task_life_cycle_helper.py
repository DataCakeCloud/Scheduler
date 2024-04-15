# -*- coding: utf-8 -*-
import pendulum

from airflow.shareit.constant.taskInstance import BACKFILL, CLEAR, PIPELINE, FIRST
from airflow.shareit.models.task_life_cycle import TaskLifeCycle
from airflow.shareit.utils.date_util import DateUtil
from airflow.utils import timezone
from prometheus_client import Gauge, CollectorRegistry, Summary, Histogram
from prometheus_client.exposition import choose_encoder

from airflow import LoggingMixin
from airflow.utils.timezone import beijing


class TaskLifeCycleHelper(LoggingMixin):

    @staticmethod
    def push_new_cycle(task_name,execution_date,tri_type,time=None):
        if tri_type == BACKFILL:
            TaskLifeCycle.add_cycle(task_name,execution_date,backfill_time=time)
        if tri_type == CLEAR:
            TaskLifeCycle.add_cycle(task_name, execution_date, clear_time=time)
        if tri_type == FIRST:
            TaskLifeCycle.add_cycle(task_name,execution_date,first_pipeline_time=time)
        if tri_type == PIPELINE:
            TaskLifeCycle.add_cycle(task_name, execution_date)

    @staticmethod
    def update_ready_time(task_name,execution_date,time):
        cycles = TaskLifeCycle.find_cycle(task_name,execution_date)
        if cycles:
            TaskLifeCycle.update_cycle_first(task_name,execution_date,ready_time=time)
        else:
            TaskLifeCycle.add_cycle(task_name,execution_date,ready_time=time)

    @staticmethod
    def update_start_time(task_name,execution_date,time):
        cycles = TaskLifeCycle.find_cycle(task_name,execution_date)
        if cycles:
            TaskLifeCycle.update_cycle_first(task_name,execution_date,start_time=time)
        else:
            TaskLifeCycle.add_cycle(task_name,execution_date,start_time=time)