# -*- coding: utf-8 -*-

import re
from datetime import datetime, timedelta

import jinja2
import pendulum
import six
from croniter import croniter, croniter_range

from airflow.models.dag import DagModel
from airflow.shareit.models.dag_dataset_relation import DagDatasetRelation
from airflow.shareit.models.dataset_partation_status import DatasetPartitionStatus
from airflow.shareit.models.sensor_info import SensorInfo
from airflow.shareit.models.task_desc import TaskDesc
from airflow.shareit.models.trigger import Trigger
from airflow.shareit.utils.granularity import Granularity
from airflow.shareit.utils.state import State
from airflow.shareit.service.dataset_service import DatasetService
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.timezone import beijing, utcnow


class AirlowDateParamTransform():
    @staticmethod
    def date_transform(is_data_trigger=True,airflow_crontab=None,task_gra=None,airflow_content=None,is_sql=False):
        format_content = AirlowDateParamTransform.unified_format(airflow_content,is_sql)
        # 校验crontab是否正确
        if not croniter.is_valid(airflow_crontab):
            raise  ValueError("crontab错误，无法解析")
        if is_data_trigger:
            units = airflow_crontab.strip().split(' ')
            hour = units[1]

            if task_gra not in ['weekly','monthly','minutely','daily','hourly'] or task_gra is None:
                raise ValueError('不能识别任务的调度周期')

            if task_gra in ['weekly','monthly','minutely'] :
                raise ValueError('周、月粒度的数据触发调度任务不支持直接替换,周月任务可以切换成时间调度任务,支持参数直接替换')

            if task_gra == 'daily':
                # crontab
                start = datetime(year=2022, month=5, day=16, hour=0, minute=0, second=0, tzinfo=beijing)
                end = datetime(year=2022, month=5, day=16, hour=23, minute=59, second=59, tzinfo=beijing)
                i = 0
                for c in croniter_range(start, end, airflow_crontab):
                    if i > 0:
                        raise ValueError('当前为数据触发,且任务周期为daily.但airflow中调度此任务.一天有多次执行,怀疑配置有误,不支持参数直接替换')
                    i = i + 1
                hour_num = int(hour)
                mod = (hour_num + 8) % 24
                add_time = "macros.timedelta(hours=-8+{})".format(str(mod))
            else :
                add_time = "macros.timedelta(hours=-8)"

            ds_content = AirlowDateParamTransform.utc_format(content=format_content, add_time=add_time)

        else :
            add_time = "macros.timedelta(hours=-8)"
            ds_content = AirlowDateParamTransform.utc_format(content=format_content, add_time=add_time)
        return ds_content



    @staticmethod
    def utc_format(content,add_time=None):
        result = content

        execution_date_pattern = re.compile(r'{\s*{\s*[\(\s]*execution_date[\)\s]*[^}]+}\s*}')
        execution_date_pattern_no_paren = re.compile(r'{\s*{\s*execution_date[^}]+}\s*}')
        next_execution_date_pattern = re.compile(r'{\s*{\s*[\(\s]*next_execution_date[\)\s]*[^}]+}\s*}')
        next_execution_date_pattern_no_paren = re.compile(r'{\s*{\s*next_execution_date[^}]+}\s*}')
        prev_execution_date_pattern = re.compile(r'{\s*{\s*[\(\s]*prev_execution_date[\)\s]*[^}]+}\s*}')
        prev_execution_date_pattern_no_paren = re.compile(r'{\s*{\s*prev_execution_date[^}]+}\s*}')
        execution_all = execution_date_pattern.findall(content)
        for e in execution_all:
            if re.match(execution_date_pattern_no_paren,e):
                add_e = e.replace("execution_date", "(prev_execution_date + {})".format(add_time))
            else :
                add_e = e.replace("execution_date", "prev_execution_date + {} ".format(add_time))
            result = result.replace(e, add_e)

        prev_execution_all = prev_execution_date_pattern.findall(content)
        for e in prev_execution_all:
            if re.match(prev_execution_date_pattern_no_paren,e):
                add_e = e.replace("prev_execution_date", "(prev_2_execution_date + {})".format(add_time))
            else :
                add_e = e.replace("prev_execution_date", "prev_2_execution_date + {} ".format(add_time))
            result = result.replace(e, add_e)

        next_execution_all = next_execution_date_pattern.findall(content)
        for e in next_execution_all:
            if re.match(next_execution_date_pattern_no_paren,e):
                add_e = e.replace("next_execution_date", "(execution_date + {})".format(add_time))
            else :
                add_e = e.replace("next_execution_date", "execution_date + {} ".format(add_time))
            result = result.replace(e, add_e)

        return result

    @staticmethod
    def unified_format(content,is_sql=False):
        '''
        next_ds
        next_ds_nodash
        prev_ds
        prev_ds_nodash

{{ (execution_date  - macros.timedelta(hours=9) ).strftime("%Y%m%d") }}
{{ execution_date.strftime("%Y%m%d") }}
        '''



        ds = "{\\s*{\\s*ds\\s*}\\s*}"
        next_ds = "{\\s*{\\s*next_ds\\s*}\\s*}"
        next_ds_nodash = "{\\s*{\\s*next_ds_nodash\\s*}\\s*}"
        prev_ds = "{\\s*{\\s*prev_ds\\s*}\\s*}"
        prev_ds_nodash = "{\\s*{\\s*prev_ds_nodash\\s*}\\s*}"
        yesterday_ds = "{\\s*{\\s*yesterday_ds\\s*}\\s*}"
        yesterday_ds_nodash  = "{\\s*{\\s*yesterday_ds_nodash \\s*}\\s*}"
        tomorrow_ds  = "{\\s*{\\s*tomorrow_ds\\s*}\\s*}"
        tomorrow_ds_nodash  = "{\\s*{\\s*tomorrow_ds_nodash\\s*}\\s*}"

        # sql中使用双引号
        new_ds = "{{ execution_date.strftime('%Y-%m-%d') }}" if not is_sql else '{{ execution_date.strftime("%Y-%m-%d") }}'
        new_next_ds = "{{ next_execution_date.strftime('%Y-%m-%d') }}" if not is_sql else '{{ next_execution_date.strftime("%Y-%m-%d") }}'
        new_next_ds_nodash = "{{ next_execution_date.strftime('%Y%m%d') }}" if not is_sql else '{{ next_execution_date.strftime("%Y%m%d") }}'
        new_prev_ds = "{{ prev_execution_date.strftime('%Y-%m-%d') }}" if not is_sql else '{{ prev_execution_date.strftime("%Y-%m-%d") }}'
        new_prev_ds_nodash = "{{ prev_execution_date.strftime('%Y%m%d') }}" if not is_sql else '{{ prev_execution_date.strftime("%Y%m%d") }}'
        new_yesterday_ds = "{{ (execution_date  - macros.timedelta(days=1) ).strftime('%Y-%m-%d') }}" if not is_sql else '{{ (execution_date  - macros.timedelta(days=1) ).strftime("%Y-%m-%d") }}'
        new_yesterday_ds_nodash = "{{ (execution_date  - macros.timedelta(days=1) ).strftime('%Y%m%d') }}" if not is_sql else '{{ (execution_date  - macros.timedelta(days=1) ).strftime("%Y%m%d") }}'
        new_tomorrow_ds = "{{ (execution_date  + macros.timedelta(days=1) ).strftime('%Y-%m-%d') }}" if not is_sql else '{{ (execution_date  + macros.timedelta(days=1) ).strftime("%Y-%m-%d") }}'
        new_tomorrow_ds_nodash = "{{ (execution_date  + macros.timedelta(days=1) ).strftime('%Y%m%d') }}" if not is_sql else '{{ (execution_date  + macros.timedelta(days=1) ).strftime("%Y%m%d") }}'

        content = re.sub(ds, new_ds, content)
        content = re.sub(next_ds, new_next_ds, content)
        content = re.sub(next_ds_nodash, new_next_ds_nodash, content)
        content = re.sub(prev_ds, new_prev_ds, content)
        content = re.sub(prev_ds_nodash, new_prev_ds_nodash, content)
        content = re.sub(yesterday_ds, new_yesterday_ds, content)
        content = re.sub(yesterday_ds_nodash, new_yesterday_ds_nodash, content)
        content = re.sub(tomorrow_ds, new_tomorrow_ds, content)
        content = re.sub(tomorrow_ds_nodash, new_tomorrow_ds_nodash, content)

        return content