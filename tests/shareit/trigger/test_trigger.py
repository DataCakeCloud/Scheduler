import json
import re
import unittest
from datetime import datetime,timedelta

import requests

from airflow.models import TaskInstance
from airflow.shareit.service.task_service import TaskService
from airflow.shareit.trigger.trigger_helper import TriggerHelper
from airflow.utils import timezone
import pendulum
from croniter import croniter, croniter_range
import time
from airflow.shareit.service.diagnosis_service import DiagnosisService
from airflow.shareit.utils.task_manager import get_dag


class TriggerTest(unittest.TestCase):
    def test_generate_latest_cron_trigger(self):
        # TriggerHelper.generate_latest_cron_trigger('schedule_improve_down','00 00-23/1 * * *')
        TriggerHelper.generate_latest_cron_trigger('32_56241','0 0 * * * ')
        # TriggerHelper.generate_latest_data_trigger('test_cron_job_copy',5)

    def test_trigger(self):
        exe_date =  datetime(year=2022,month=6,day=29,hour=0)
        dag_id = 'aaaaaa'
        TriggerHelper.generate_trigger(dag_id=dag_id,execution_date=exe_date,transaction=True)


    def test_create_taskinstance(self):
        dag = get_dag(dag_id='dws_sa_push_user_world_df_2obs')
        dt = pendulum.parse('2023/02/20 00:00:00.000').replace(tzinfo=timezone.beijing)
        # dt = datetime(year=2022, month=8, day=14, hour=20).replace(tzinfo=timezone.beijing)
        TriggerHelper.create_taskinstance(task_dict=dag.task_dict, execution_date=dt)

    def test_tri(self):
        execution_date = datetime(year=2022,month=8,day=17,hour=17,tzinfo=timezone.beijing)
        tis = TaskInstance.find_by_execution_dates(dag_id='schedule_improve_down',task_id='schedule_improve_down',execution_dates=[execution_date])
        TriggerHelper.test_task_finish_trigger(tis[0])

    def test_populate_trigger(self):
        task_id = 'populate_trigger_2'
        gra = 6
        TriggerHelper.populate_trigger(task_id=task_id,gra=gra)
        # TaskService.test_trigger(task_id,gra)


    def test_reopen_future_trigger(self):
        task_name = 'scheduler_improve_external_down_copy_copy'
        TriggerHelper.reopen_future_trigger(task_name=task_name)

    def test_requests(self):
        url='https://ds-api-gateway.ushareit.org/ds_task/task'
        headers = {"Authentication": "eyJhbGciOiJIUzI1NiJ9.eyJqdGkiOiI5YzBjNDQ1ZjYyMjM0N2U1YTg4OWExNjhkZTNhOGNkZSIsImlhdCI6MTY3NjIxNzUzNCwic3ViIjoidG9rZW4gYnkgdXNoYXJlaXQiLCJleHAiOjE2NzYzMDM5MzR9.3pXIIcfQN9hJXXqHEoGzx6jOlZ61kTjX93yOPFa58FA"}
        params= {'id':18335}
        resp = requests.get(url=url,params=params,headers=headers)
        print resp.json()
