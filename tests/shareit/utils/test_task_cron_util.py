import json
import re
import unittest
from datetime import datetime,timedelta

from airflow import macros
from airflow.models import TaskInstance
from airflow.shareit.utils.task_cron_util import TaskCronUtil

from airflow.models import DagModel
import pendulum
from croniter import croniter,croniter_range
import time
from airflow.shareit.service.diagnosis_service import DiagnosisService
from airflow.shareit.utils.task_util import TaskUtil


class TaskCronUtilTest(unittest.TestCase):

    def test_get_prev(self):
        beijing = pendulum.timezone("Asia/Shanghai")
        dag_id = 'test_python_shell_optimize'
        execution_date = datetime(year=2022, month=7, day=18, hour=0, minute=0, second=0, tzinfo=beijing)
        # end_date  = datetime(year=2022, month=5, day=19, hour=20, minute=0, second=0, tzinfo=beijing)
        # print  TaskCronUtil.get_pre_execution_date(dag_id,execution_date)
        # DagModel.get_dag()
        # ti =  TaskInstance.find_with_date_range(dag_id=dag_id,start_date=execution_date)
        content = "{{ lastmonth_date }}"
        re =  TaskUtil.get_template_context(dag_id=dag_id,execution_date=execution_date,context={})
        print re["lastmonth_date"]
        print re["lastmonth_date_ds_nodash"]
        print re["lastmonth_date_utc0"]
        print re["lastmonth_date_ds_nodash_utc0"]
    def test_get_version(self):
        from airflow.shareit.models.task_desc import TaskDesc
        td = TaskDesc()
        version = td.get_task_version(task_name="test_redi")
        print version