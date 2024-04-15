import json
import re
import unittest
from datetime import datetime,timedelta

from airflow import macros
from airflow.shareit.utils.airflow_date_param_transform import AirlowDateParamTransform
from airflow.shareit.utils.task_util import TaskUtil

from airflow.utils import timezone
import pendulum
from croniter import croniter,croniter_range
import time
from airflow.shareit.service.diagnosis_service import DiagnosisService



class AirlowDateParamTransformTest(unittest.TestCase):
    def test_date_transform(self):
        content = '''
                {{ (execution_date  - macros.timedelta(hours=9)  - macros.timedelta(hours=9)  - macros.timedelta(hours=9) ).strftime("%Y%m%d") }}
                {{ (next_execution_date  - macros.timedelta(hours=9)  - macros.timedelta(hours=9)  - macros.timedelta(hours=9) ).strftime("%Y%m%d") }}
        {{ execution_date.strftime("%Y%m%d") }}
                {{ prev_execution_date.strftime("%Y%m%d") }}
                {{ (prev_execution_date  - macros.timedelta(hours=9)  - macros.timedelta(hours=9)  - macros.timedelta(hours=9) ).strftime("%Y%m%d") }}
                { { next_ds}} 
                {{next_ds_nodash}} 
                {{prev_ds}} 
                {{prev_ds_nodash}} 
                {{yesterday_ds}}
                
                
                11111
        '''

        print AirlowDateParamTransform.date_transform(is_data_trigger=True,airflow_crontab='0 10,11 * * *',task_gra='daily',airflow_content=content)

    def test_unified_format(self):
        content = "{ { next_ds}} {{next_ds_nodash}} {{prev_ds}} {{prev_ds_nodash}} {{yesterday_ds}}"

        print AirlowDateParamTransform.unified_format(content)

    def test_utc_format(self):
        content = '''
        {{ (execution_date  - macros.timedelta(hours=9)  - macros.timedelta(hours=9)  - macros.timedelta(hours=9) ).strftime("%Y%m%d") }}
        {{ (next_execution_date  - macros.timedelta(hours=9)  - macros.timedelta(hours=9)  - macros.timedelta(hours=9) ).strftime("%Y%m%d") }}
{{ execution_date.strftime("%Y%m%d") }}
        {{ prev_execution_date.strftime("%Y%m%d") }}
        {{ (prev_execution_date  - macros.timedelta(hours=9)  - macros.timedelta(hours=9)  - macros.timedelta(hours=9) ).strftime("%Y%m%d") }}
        { { next_ds}} {{next_ds_nodash}} {{prev_ds}} {{prev_ds_nodash}} {{yesterday_ds}}
'''
        AirlowDateParamTransform.utc_format(content,'macros.timedelta(hours=8)')

    def test_date_transform2(self):
        content = '''

alter table ads_dmp.dws_ads_thirdparty_inventory_day add if not exists partition(dt='{{ (execution_date - macros.timedelta(days=1)).strftime("%Y%m%d") }}',platform='applovin') 
location 's3://shareit.ads.ap-southeast-1/table/ads/dws_ads_thirdparty_inventory_day/dt={{ (execution_date - macros.timedelta(days=1)).strftime("%Y%m%d") }}/platform=applovin/';
        '''

        print AirlowDateParamTransform.date_transform(is_data_trigger=False, airflow_crontab='0 6,11 * * *',
                                                      task_gra='hourly', airflow_content=content)

    def test_render_time(self):
        from airflow import macros
        from datetime import datetime, timedelta
        beijing = pendulum.timezone("Asia/Shanghai")
        # crontab
        execution_date = datetime(year=2022, month=5, day=16, hour=0, minute=0, second=0, tzinfo=beijing)
        content = '''

        alter table ads_dmp.dws_ads_thirdparty_inventory_day add if not exists partition(dt='{{ (prev_2_execution_date - macros.timedelta(days=1)).strftime("%Y%m%d %H") }}',platform='applovin') 
        location 's3://shareit.ads.ap-southeast-1/table/ads/dws_ads_thirdparty_inventory_day/dt={{ (prev_execution_date - macros.timedelta(days=1)).strftime("%Y%m%d %H") }}/platform=applovin/';
                '''

        res= TaskUtil.render_datatime(content=content,execution_date=execution_date,depend_gra='5',offset=-1,detailed_gra=None,
                                 detailed_dependency=None,dag_id='test_location')
        for i in res:
            print i['path']