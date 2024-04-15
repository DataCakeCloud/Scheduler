# -*- coding: utf-8 -*-
"""
author: zhangtao
Copyright (c) 2021/8/6 Shareit.com Co., Ltd. All Rights Reserved.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import os
import signal
import sys
import time
import traceback
import uuid

import requests
import six
import boto3
import multiprocessing

from airflow.exceptions import AirflowTaskTimeout

from airflow.shareit.monitor.push_monitor import MonitorPush
from airflow.shareit.utils.cloud.cloud_utli import CloudUtil
from airflow.utils.timeout import timeout

from airflow.configuration import conf
from obs import ObsClient
from datetime import datetime
from gcloud import storage

from airflow.shareit.models.trigger import Trigger
from airflow.shareit.scheduler.sensor_job_helper import ReadyTimeSensenJob
from airflow.shareit.utils.normal_util import NormalUtil
from airflow.utils import timezone
from airflow.shareit.utils.state import State
from airflow.shareit.utils.granularity import Granularity
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.shareit.models.sensor_info import SensorInfo
from airflow.shareit.service.dataset_service import DatasetService
from airflow.shareit.utils.alarm_adapter import get_receiver_list, send_ding_notify,send_email_by_notifyAPI
from airflow.shareit.models.task_desc import TaskDesc


def _is_dir(path=None):
    if path.endswith("/"):
        return True
    return False


def generate_task_id():
    return "ds_check_" + str(uuid.uuid4())


def _get_genie_job_name(metadata_id, execution_date):
    from datetime import datetime
    if isinstance(execution_date, six.string_types):
        exec_date = execution_date
    elif isinstance(execution_date, datetime):
        exec_date = execution_date.strftime("%Y-%m-%dT%H:%M:%S")
    else:
        exec_date = ""
    return "ds_check_" + metadata_id + "_" + exec_date


def check_path_process(dag_id, metadata_id, execution_date, granularity, offset,
                       check_path, cluster_tags, detailed_execution_date):
    # from airflow import macros
    usJob = UnifiedSensorJob()
    usJob.log.debug("[DataStudio Sensor] Check {metadata_id} Check path:{path}".format(metadata_id=str(metadata_id),
                                                                                       path=str(check_path)))
    try:
        # contest = {"execution_date": execution_date, "macros": macros}
        # cpath = usJob.render_check_path(content=check_path, context=contest)
        usJob.log.debug("[DataStudio Sensor] Check path: {path}".format(path=check_path))
        if usJob.check_poke_by_sdk(check_path=check_path,task_name=dag_id):
            # if usJob.check_poke(genie_conn_id="dev-datastudio-big-authority", check_path=check_path,
            #                     cluster_tags=cluster_tags, command_tags="type:hadoop",
            #                     job_name=_get_genie_job_name(metadata_id, execution_date)):
            SensorInfo.update_si_state(metadata_id=metadata_id, execution_date=execution_date,
                                       detailed_execution_date=detailed_execution_date,
                                       granularity=granularity, offset=offset, state=State.SUCCESS,dag_id=dag_id)
            # 数据集状态写入
            dataset_time = Granularity.getPreTime(baseDate=execution_date, gra=granularity,
                                                  offset=offset) if detailed_execution_date is None else detailed_execution_date
            # DatasetService.add_metadata_sign(metadata_id=metadata_id, dataset_partition_sign=dataset_time,
            #                                  state="temp", run_id="Datastudio_Sensor")
            # 如果成功创建对应的下游的trigger
            if cluster_tags != "backfill_sensor":
                usJob.log.debug("[DataStudio Sensor] trigger downstream task")
                DatasetService.trigger_downstream_task_by_dataset_info(metadata_id=metadata_id,
                                                                       execution_date=dataset_time,
                                                                       granularity=granularity)
    except Exception as e:
        usJob.log.error("[DataStudio Sensor] Check {metadata_id} Failed! Check path:{path}".format(
            metadata_id=metadata_id, path=check_path), exc_info=True)
    finally:
        if execution_date.utcoffset() is None:
            execution_date.replace(tzinfo=timezone.beijing)
        SensorInfo.update_si_checked_time(dag_id=dag_id, metadata_id=metadata_id,
                                          execution_date=execution_date,
                                          detailed_execution_date=detailed_execution_date,
                                          granularity=granularity, offset=offset)


class UnifiedSensorJob(LoggingMixin):
    def __init__(
            self,
            timeout=-1,
            check_parallelism=6,
            *args, **kwargs):
        self.timeout = timeout
        self.check_parallelism = check_parallelism
        signal.signal(signal.SIGINT, self._exit_gracefully)
        signal.signal(signal.SIGTERM, self._exit_gracefully)

    def _exit_gracefully(self, signum, frame):
        """
        Helper method to clean up processor_agent to avoid leaving orphan processes.
        """
        self.log.info("Exiting gracefully upon receiving signal %s", signum)
        sys.exit(os.EX_OK)

    def run(self):
        try:
            self._execute()
        except Exception:
            raise

    def _execute(self):
        ReadyTimeSensenJob().start()
        execute_start_time = timezone.utcnow()
        monitor_push = MonitorPush()
        while (timezone.utcnow() - execute_start_time).total_seconds() < self.timeout or self.timeout == -1:
            # 获取当前待检查的条目 状态为checking 心跳超过间隔
            self.log.debug("Starting Loop...")
            try:
                with timeout(3):
                    monitor_push.push_to_prometheus(job_name="sensor_job_check_path", gauge_name="sensor_job_check_path",
                                                    documentation="sensor_job_check_path")
            except AirflowTaskTimeout as e:
                self.log.error("[DataStudio Pipeline] push metrics timeout")
            except Exception as e:
                self.log.error("[DataStudio Pipeline] sensor_job_check_path push metrics failed")
            loop_start_time = time.time()
            try :
                res = SensorInfo.get_checking_info(sensor_type=State.CHECK_PATH)
            except Exception as e :
                s = traceback.format_exc()
                self.log.error(s)
                continue
            # unique_res = self._merge_redundant_sensor_info(si_list=res)
            check_si = []
            for si in res:
                try:
                    total_cost = (
                            timezone.make_naive(timezone.utcnow()) - si.first_check).total_seconds() if isinstance(
                        si.first_check, datetime) else 0
                    last_cost = (timezone.make_naive(timezone.utcnow()) - si.last_check).total_seconds() if isinstance(
                        si.last_check, datetime) else 0
                    if si.last_check is None:
                        check_si.append(si)
                    # elif total_cost > si.timeout_seconds:
                    #     si.update_state(State.FAILED)
                    #     from airflow.models import TaskInstance
                    #     # 将终止状态回传给ds-task后端
                    #     if TaskInstance.is_latest_runs(si.dag_id, si.execution_date):
                    #         self.log.info("[DataStudio pipeline] start sync state to ds backend ...")
                    #         tmp_err = None
                    #         for _ in range(3):
                    #             try:
                    #                 url = conf.get("core", "default_task_url")
                    #                 data = {"taskName": si.dag_id,
                    #                         "status": State.FAILED}
                    #                 requests.put(url=url, data=data)
                    #                 tmp_err = None
                    #             except Exception as e:
                    #                 time.sleep(0.5)
                    #                 tmp_err = e
                    #         if tmp_err:
                    #             self.log.error("[DataStudio pipeline] task:{} ,sync state to ds backend error:{} ".format(si.dag_id,str(tmp_err)))
                    elif last_cost > si.poke_interval:
                        check_si.append(si)
                        if total_cost > 86400:
                            new_poke = int(si.poke_interval * 1.1)
                            new_poke = new_poke if new_poke < 86400 else 86400
                            si.update_poke(new_poke_interval=new_poke)
                    else:
                        self.log.debug("[DataStudio Sensor] {metadata} {time} not in poke_interval ".format(
                            metadata=str(si.metadata_id),
                            time=str(si.execution_date)))
                except Exception:
                    self.log.error("[DataStudio Sensor] {metadata} {time} excute sensor Failed!".format(
                        metadata=str(si.metadata_id),
                        time=str(si.execution_date)), exc_info=True)

            self.log.info("[DataStudio Sensor] {nums} inspection tasks are being processed"
                          .format(nums=str(len(check_si))))
            # 检查每个的状态
            pool = multiprocessing.Pool(processes=self.check_parallelism)
            for si in check_si:
                pool.apply_async(check_path_process, (si.dag_id, si.metadata_id,
                                                      si.execution_date, si.granularity,
                                                      si.offset, si.check_path, si.cluster_tags,
                                                      si.detailed_execution_date,))
            pool.close()
            pool.join()
            self.log.debug("[DataStudio Sensor] now time" + str(time.time()))
            loop_end_time = time.time()
            loop_duration = loop_end_time - loop_start_time
            if loop_duration < 5:
                sleep_length = 5 - loop_duration
                self.log.debug(
                    "Sleeping for {0:.2f} seconds to prevent excessive logging"
                        .format(sleep_length))
                time.sleep(sleep_length)


    def render_check_path(self, content, context):
        import jinja2
        jinja_env = jinja2.Environment(cache_size=0)
        return jinja_env.from_string(content).render(**context)

    def check_poke(self, genie_conn_id, check_path, cluster_tags, command_tags, job_name=None):
        from airflow.hooks.genie_hook import GenieHook
        if _is_dir(path=check_path):
            command = 'fs -test -d ' + check_path
        else:
            command = 'fs -test -e ' + check_path
        genie_hook = GenieHook(genie_conn_id=genie_conn_id)
        self.log.info('Poking for key : %s', check_path)
        if job_name is None:
            job_name = generate_task_id()
        status = genie_hook.submit_job(cluster_tags=cluster_tags,
                                       command_tags=command_tags,
                                       command=command,
                                       job_name=job_name
                                       )
        if status.upper() != 'SUCCEEDED':
            return False
        else:
            return True

    def check_poke_by_sdk(self, check_path,task_name):
        if check_path is None:
            self.log.info("check path id None !")
            return False
        try:
            tenant_id,cluster_region,provider = TaskDesc.get_cluster_manager_info(task_name=task_name)
            cloud = CloudUtil(provider=provider,region=cluster_region,tenant_id=tenant_id)
            # cloud = CloudUtil()
            return cloud.check_file_or_dir(check_path)
        except Exception as e:
            self.log.error("检查路径不通过:{} ".format(check_path) + str(e), exc_info=True)
            self.log.error(traceback.format_exc())
            return False


    def send_check_failed_notify(self, check_path, dag_id, execution_date):
        base_content = """__[DataStudio 数据检查失败通知]__  
            __任务: {task_name}__  
            __执行入参时间: {execution_date}__    
            __检查路径: {check_path}__    
            路径检测超时，如需重新启动检查，请转至Datastudio实例页进行操作"""
        if isinstance(execution_date, datetime):
            execution_date = execution_date.strftime('%Y-%m-%dT%H:%M:%S')
        task = TaskDesc.get_task(task_name=dag_id)
        if task:
            notify_type = conf.get('email', 'notify_type')
            content = base_content.format(task_name=dag_id, execution_date=execution_date, check_path=check_path)
            if "dingding" in notify_type:
                receiver = get_receiver_list(task.owner)
                send_ding_notify(title="DataStudio 数据检查状态通知", content=content, receiver=receiver)
            if "email" in notify_type:
                receiver = get_receiver_list(task.email)
                send_email_by_notifyAPI(title="DataStudio 数据检查状态通知", content=content, receiver=receiver)
