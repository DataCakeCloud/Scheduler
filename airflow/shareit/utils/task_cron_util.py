# -*- coding: utf-8 -*-

from datetime import datetime, timedelta

import jinja2
import six
from croniter import croniter, croniter_range
import pendulum

from airflow.shareit.models.dag_dataset_relation import DagDatasetRelation
from airflow.shareit.models.task_desc import TaskDesc
from airflow.shareit.utils.granularity import Granularity
from airflow.utils.timezone import beijing, utcnow


class TaskCronUtil():
    @staticmethod
    def get_task_cron(task_name,gra=None):
        task = TaskDesc.get_task(task_name=task_name)
        if task is not None and task.crontab is not None and task.crontab != '':
            return task.crontab
        if gra is None:
            gra = DagDatasetRelation.get_task_granularity(task_name)
        return TaskCronUtil.get_cron_by_gra(gra)

    @staticmethod
    def get_cron_by_gra(gra):
        gra = int(gra)
        gra_dict = {
            6: '0 * * * *',
            5: '0 0 * * *',
            4: '0 0 * * 1',
            3: '0 0 1 * *',
        }
        if gra not in [3, 4, 5, 6]:
            return ''
        return gra_dict[gra]

    @staticmethod
    def get_pre_execution_date(task_name,execution_date):
        cron = TaskCronUtil.get_task_cron(task_name)
        if cron == '':
            return  None
        execution_date = pendulum.instance(execution_date)
        cron = croniter(cron, execution_date.replace(tzinfo=beijing))
        cron.tzinfo = beijing
        return cron.get_prev(datetime)

    @staticmethod
    def get_pre_execution_date_by_crontab(crontab,execution_date):
        cron = croniter(crontab, execution_date)
        cron.tzinfo = beijing
        return cron.get_prev(datetime)

    @staticmethod
    def get_next_execution_date(task_name,execution_date):
        cron = TaskCronUtil.get_task_cron(task_name)
        if cron == '':
            return  None
        execution_date = pendulum.instance(execution_date)
        cron = croniter(cron, execution_date)
        cron.tzinfo = beijing
        return cron.get_next(datetime)

    @staticmethod
    def get_all_date_in_one_cycle(task_name,execution_date,max_gra=7,gra=None,cron=None):
        if not gra:
            gra = DagDatasetRelation.get_task_granularity(task_name)
        else :
            gra = Granularity.getIntGra(gra)
        if cron is None:
            cron = TaskCronUtil.get_task_cron(task_name=task_name, gra=gra)
        if gra > max_gra :
            return []
        result_date = []
        if gra == 6:
            # 小时周期补充
            startDate = datetime(year=execution_date.year, month=execution_date.month, day=execution_date.day,hour=execution_date.hour,tzinfo=beijing)
            endDate = startDate.replace(minute=59,second=59)
            for dt in croniter_range(startDate, endDate, cron):
                return [execution_date.replace(minute=dt.minute)]
            # 如果没有,返回自己本身
            return [execution_date]
        if gra == 5:
            # 天周期补充
            startDate = datetime(year=execution_date.year, month=execution_date.month, day=execution_date.day,
                                 tzinfo=beijing)
            endDate = startDate.replace(hour=23,minute=59, second=59)
            for dt in croniter_range(startDate, endDate, cron):
                return [execution_date.replace(hour=dt.hour,minute=dt.minute)]
            # 如果没有,返回自己本身
            return [execution_date]
        if gra == 4:
            # 周周期补充
            startDate = (execution_date - timedelta(days=execution_date.weekday())).replace(hour=0,minute=0,tzinfo=beijing)
            endDate = (startDate + timedelta(days=6)).replace(hour=23,minute=59)
            for dt in croniter_range(startDate, endDate, cron):
                result_date.append(execution_date.replace(month=dt.month,day=dt.day,hour=dt.hour,minute=dt.minute))
            if len(result_date) == 0:
                return [execution_date]
            else:
                return result_date
        if gra == 3:
            from airflow.shareit.utils.date_calculation_util import DateCalculationUtil
            # 月周期补充
            startDate = datetime(year=execution_date.year, month=execution_date.month, day=1,
                                 tzinfo=beijing)
            endDate = DateCalculationUtil.datetime_month_caculation(startDate,1) - timedelta(minutes=1)
            for dt in croniter_range(startDate, endDate, cron):
                result_date.append(execution_date.replace(day=dt.day, hour=dt.hour, minute=dt.minute))
            if len(result_date) == 0:
                return [execution_date]
            else:
                return result_date

        return [execution_date]

    @staticmethod
    def get_all_date_in_one_cycle_range(task_name,execution_date, max_gra=7,gra=None,time_zone=8):
        if not gra:
            gra = DagDatasetRelation.get_task_granularity(task_name)
        if gra > max_gra:
            return []
        result_date = []
        if gra == 6:
            # 小时周期补充
            startDate = datetime(year=execution_date.year, month=execution_date.month, day=execution_date.day,
                                 hour=execution_date.hour, tzinfo=beijing)
            endDate = startDate.replace(minute=59, second=59)
            # 如果没有,返回自己本身
            result_date = [startDate,endDate]
        if gra == 5:
            # 天周期补充
            startDate = datetime(year=execution_date.year, month=execution_date.month, day=execution_date.day,
                                 tzinfo=beijing)
            endDate = startDate.replace(hour=23, minute=59, second=59)
            # 如果没有,返回自己本身
            result_date = [startDate, endDate]
        if gra == 4:
            # 周周期补充
            startDate = (execution_date - timedelta(days=execution_date.weekday())).replace(hour=0, minute=0,
                                                                                            tzinfo=beijing)
            endDate = (startDate + timedelta(days=6)).replace(hour=23, minute=59)
            # 如果没有,返回自己本身
            result_date = [startDate, endDate]
        if gra == 3:
            from airflow.shareit.utils.date_calculation_util import DateCalculationUtil
            # 月周期补充
            startDate = datetime(year=execution_date.year, month=execution_date.month, day=1,
                                 tzinfo=beijing)
            endDate = DateCalculationUtil.datetime_month_caculation(startDate, 1) - timedelta(minutes=1)
            # 如果没有,返回自己本身
            result_date = [startDate, endDate]

        if len(result_date) > 1:
            return result_date[0] - timedelta(hours=time_zone-8), result_date[1] - timedelta(hours=time_zone-8)
        else:
            return execution_date,execution_date

    @staticmethod
    def ger_prev_execution_date_by_task(task_name, cur_date, task_desc=None):
        if task_desc is not None and task_desc.crontab is not None and task_desc.crontab != '':
            crontab = task_desc.crontab
        else:
            gra = DagDatasetRelation.get_task_granularity(task_name)
            crontab = TaskCronUtil.get_cron_by_gra(gra)

        if crontab == '':
            return crontab
        cur_date = cur_date.replace(tzinfo=beijing)
        cron = croniter(crontab, cur_date)
        return cron.get_prev(datetime)
