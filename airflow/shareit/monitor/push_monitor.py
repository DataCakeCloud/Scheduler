# -*- coding: utf-8 -*-
from airflow.exceptions import AirflowTaskTimeout
from prometheus_client import Gauge, push_to_gateway,CollectorRegistry
import time
from airflow.configuration import conf
from airflow.utils.timeout import timeout
from airflow import LoggingMixin
class MonitorPush(LoggingMixin):
    def __init__(self):
        pass
        # try:
        #     self.gateway_host = conf.get('monitor', 'gateway_host')
        # except Exception:
        #     self.gateway_host = ''


    def push_to_prometheus(self,job_name,gauge_name,documentation):
        return
        # if not self.gateway_host:
        #     return
        # collector_registry = CollectorRegistry(auto_describe=False)
        # # 创建一个Gauge指标，用于跟踪当前时间
        # current_time = Gauge(name=gauge_name,documentation=documentation,registry=collector_registry)
        #
        # # 设置Gauge指标的值
        # now = int(time.time())
        # current_time.set(now)
        #
        # # 将指标推送到Prometheus Pushgateway
        # push_to_gateway(self.gateway_host, job=job_name,registry=collector_registry)


