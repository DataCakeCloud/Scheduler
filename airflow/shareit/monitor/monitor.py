# -*- coding: utf-8 -*-
import copy
import traceback

import pendulum
from airflow.configuration import conf
from airflow.shareit.constant.taskInstance import MANUALLY

from airflow.shareit.models.task_life_cycle import TaskLifeCycle
from airflow.shareit.utils.date_util import DateUtil
from airflow.utils import timezone
from prometheus_client import Gauge, CollectorRegistry, Summary, Histogram, push_to_gateway
from prometheus_client.exposition import choose_encoder

from airflow import LoggingMixin
from airflow.models import TaskInstance
from airflow.shareit.models.taskinstance_log import TaskInstanceLog
from airflow.utils.timezone import beijing


class Monitor(LoggingMixin):
    def __init__(self):
        self.gateway_host = conf.get('monitor', 'gateway_host')

        # 注册收集器&最大耗时map
        self.collector_registry = CollectorRegistry(auto_describe=False)
        self.running_task = set()

        # 接口调用summary统计
        self.check_dispatch = Histogram(name="scheduler_check_dispatch",
                                        documentation="scheduler check dispatch",
                                        labelnames=["task_type"],
                                        buckets=(5, 10, 15, 20, 25, 30, 60, 120, 240),
                                        registry=self.collector_registry)
        # 正在运行任务的运行时长
        self.scheduler_task_runtime = Gauge(name="scheduler_task_runtime",
                                           documentation="scheduler task run time",
                                           labelnames=("task_name","execution_date","try_number"),
                                           registry=self.collector_registry)

        # 正在运行的任务数
        self.scheduler_task_running_num = Gauge(name="scheduler_task_running_num",
                                           documentation="scheduler task running num",
                                           registry=self.collector_registry)

        # queued挂起的任务数
        self.scheduler_task_queued_num = Gauge(name="scheduler_task_queued_num",
                                           documentation="scheduler task queued num",
                                           registry=self.collector_registry)

        # 当前时间一小时内实例失败率
        self.scheduler_task_failed = Gauge(name='scheduler_task_failed',
                                           documentation='scheduler ti failed',
                                           registry=self.collector_registry)

        # 当前时间一小时内任务失败率
        self.scheduler_distinct_task_failed = Gauge(name='scheduler_distinct_task_failed',
                                           documentation='scheduler task failed',
                                           registry=self.collector_registry)

    # # push指标
    # def push_prometheus_metrics(self):
    #     push_to_gateway(self.gateway_host,job='schedulerMetrics',registry=self.collector_registry)

    # 获取/metrics结果
    def get_prometheus_metrics(self):
        self.get_task_runtime_metrics()
        self.get_check_dispatch_metrics()
        self.get_task_running_num_metrics()
        self.get_task_queued_num_metrics()
        self.get_task_failed_metrics()

    # summary统计
    def set_check_dispatch(self, task_type, value):
        self.check_dispatch.labels(task_type).observe(value)

    def set_task_runtime(self,task_name,execution_date,try_number,value):
        self.scheduler_task_runtime.labels(task_name,execution_date,try_number).set(value)

    def set_task_running_num(self,value):
        self.scheduler_task_running_num.set(value)

    def set_task_queued_num(self,value):
        self.scheduler_task_queued_num.set(value)

    def set_task_failed(self, rate):
        self.scheduler_task_failed.set(rate)

    def set_distinct_task_failed(self, rate):
        self.scheduler_distinct_task_failed.set(rate)

    def get_check_dispatch_metrics(self):
        cycles = TaskLifeCycle.find_export_cycle()
        for cycle in cycles:
            exe_ts = DateUtil.get_timestamp(cycle.execution_date)
            value = min(cycle.start_time - exe_ts,
                        cycle.start_time - cycle.ready_time,
                        cycle.start_time - cycle.backfill_time,
                        cycle.start_time - cycle.clear_time)/1000
            if value <= 0:
                self.log.error('[Monitor Service] get wrong value : {},task_name :{},execution_date:{}'.format(value,
                                                                                                                 cycle.task_name,
                                                                                                                 cycle.execution_date.strftime(
                                                                                                                     '%Y-%m-%d %H:%M:%S.%f')))
                continue

            task_type = 'pipeline'
            if cycle.backfill_time > 0:
                task_type = 'backfill'
            if cycle.clear_time > 0:
                task_type = 'clear'
            self.set_check_dispatch(task_type, value)
            TaskLifeCycle.close_export(cycle.id)


    def get_task_runtime_metrics(self):
        nowDate = timezone.utcnow()
        running_tis = TaskInstance.find_running_ti()
        # temp_ti_set = set()
        for ti in running_tis:
            try :
                # temp_ti_set.add((ti.task_id,ti.execution_date.strftime('%Y-%m-%dT%H:%M:%S.%f')))
                duration = (DateUtil.get_timestamp(nowDate) - DateUtil.get_timestamp(ti.start_date)) / 1000
                try_number = ti.try_number - 1 if ti.try_number > 0 else 0
                self.set_task_runtime(ti.task_id,ti.execution_date.strftime('%Y-%m-%dT%H:%M:%S.%f'),try_number,duration)
            except Exception:
                self.log.error(traceback.format_exc())
                continue
        # copy_runnig_task = copy.copy(self.running_task)
        # for task in copy_runnig_task:
        #     if task not in temp_ti_set:
        #         # 如果不在,可能已经执行结束了
        #         try:
        #             tis = TaskInstance.find_by_execution_date(task_id=task[0],execution_date=pendulum.parse(task[1]).replace(tzinfo=beijing))
        #             if tis:
        #                 ti = tis[0]
        #                 end_date = ti.end_date if ti.end_date else nowDate
        #                 duration = (DateUtil.get_timestamp(end_date) - DateUtil.get_timestamp(ti.start_date)) / 1000
        #                 try_number = ti.try_number - 1 if ti.try_number > 0 else 0
        #                 self.set_task_runtime(ti.task_id, ti.execution_date.strftime('%Y-%m-%dT%H:%M:%S.%f'), try_number,duration)
        #             self.running_task.remove(task)
        #         except Exception as e:
        #             self.log.error(traceback.format_exc())
        #             continue
        #
        # self.running_task = set.union(self.running_task,temp_ti_set)
    def get_task_running_num_metrics(self):
        running_tis = TaskInstance.find_running_ti()
        self.set_task_running_num(len(running_tis))

    def get_task_queued_num_metrics(self):
        queued_tis = TaskInstance.find_queued_ti()
        self.set_task_queued_num(len(queued_tis))

    def get_task_failed_metrics(self):
        try:
            start, end = timezone.before_hour()
            tis = TaskInstanceLog.find_one_hour_ti(start)
            failed_tis = [ti for ti in tis if ti.state == 'failed' and ti.start_date and ti.end_date and ti.start_date != ti.end_date]
            distinct_tis = set([ti.dag_id for ti in tis])
            distinct_failed_tis = set([ti.dag_id for ti in failed_tis])
            # 计算实例失败率
            self.set_task_failed(round(len(failed_tis) * 1.0 * 100 / len(tis), 2))
            # 计算任务失败率
            self.set_distinct_task_failed(round(len(distinct_failed_tis) * 1.0 * 100 / len(distinct_tis), 2))
        except Exception as ex:
            self.log.error(traceback.format_exc())

