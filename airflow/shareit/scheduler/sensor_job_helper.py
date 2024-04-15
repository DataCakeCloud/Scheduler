# -*- coding: utf-8 -*-

import multiprocessing
import time
import traceback

from airflow.utils.timeout import timeout

from airflow.exceptions import AirflowTaskTimeout

from airflow.shareit.models.sensor_info import SensorInfo
from airflow.shareit.monitor.push_monitor import MonitorPush
from airflow.shareit.service.dataset_service import DatasetService
from airflow.shareit.utils.date_util import DateUtil
from airflow.shareit.utils.granularity import Granularity
from airflow.shareit.utils.state import State
from airflow.utils import timezone

from airflow.utils.log.logging_mixin import LoggingMixin

from datetime import datetime

from airflow.utils.timezone import beijing

log = LoggingMixin().logger


def sensor_job_readytime_processor():
    monitor_push = MonitorPush()
    while True:
        loop_start_time = time.time()
        log.info("[DataStudio Pipeline] ready_time_proccessor start...")
        try:
            with timeout(3):
                monitor_push.push_to_prometheus(job_name="sensor_job_ready_time", gauge_name="sensor_job_ready_time",
                                                documentation="sensor_job_ready_time")
        except AirflowTaskTimeout as e:
            log.error("[DataStudio Pipeline] push metrics timeout")
        except Exception as e:
            log.error("[DataStudio Pipeline] sensor_job_ready_time push metrics failed")

        try:
            sensor_job_readytime_processor_main()
            loop_end_time = time.time()
            loop_duration = loop_end_time - loop_start_time
            if loop_duration < 5:
                sleep_length = 5 - loop_duration
                log.debug(
                    "Sleeping for {0:.2f} seconds to prevent excessive logging"
                        .format(sleep_length))
                time.sleep(sleep_length)
        except Exception:
            s = traceback.format_exc()
            log.error("[DataStudio Pipeline] ready_time_proccessor error")
            log.error(s)

        log.info("[DataStudio Pipeline] ready_time_proccessor end...")


def sensor_job_readytime_processor_main():
        # 获取当前待检查的条目 状态为checking 心跳超过间隔
        log.debug("Starting ready_time Loop...")
        loop_start_time = time.time()
        res = SensorInfo.get_checking_info(sensor_type=State.READY_TIME)
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

                elif last_cost > si.poke_interval:
                    check_si.append(si)
                    if total_cost > 86400:
                        new_poke = int(si.poke_interval * 1.1)
                        new_poke = new_poke if new_poke < 86400 else 86400
                        si.update_poke(new_poke_interval=new_poke)
                else:
                    log.debug("[DataStudio Sensor ready_time] {metadata} {time} not in poke_interval ".format(
                        metadata=str(si.metadata_id),
                        time=str(si.execution_date)))

            except Exception:
                log.error("[DataStudio Sensor ready_time] {metadata} {time} excute sensor Failed!".format(
                    metadata=str(si.metadata_id),
                    time=str(si.execution_date)), exc_info=True)
                log.info("[DataStudio Sensor ready_time] {nums} inspection tasks are being processed"
                         .format(nums=str(len(check_si))))

        for check in check_si:
            try:
                if DateUtil.date_compare(timezone.utcnow().replace(tzinfo=beijing),check.completion_time):
                    SensorInfo.update_si_state(metadata_id=check.metadata_id, execution_date=check.execution_date,
                                               detailed_execution_date=check.detailed_execution_date,
                                               granularity=check.granularity, offset=check.offset, state=State.SUCCESS,
                                               dag_id=check.dag_id)

                # 数据集状态写入
                dataset_time = Granularity.getPreTime(baseDate=check.execution_date, gra=check.granularity,
                                                          offset=-1) if check.detailed_execution_date is None else check.detailed_execution_date
                # 如果成功创建对应的下游的trigger
                if check.cluster_tags != "backfill_sensor":
                        log.debug("[DataStudio Sensor ready_time] trigger downstream task")
                        DatasetService.trigger_downstream_task_by_dataset_info(metadata_id=check.metadata_id,
                                                                               execution_date=dataset_time,
                                                                               granularity=check.granularity)
            except Exception as e:
                log.error("[DataStudio Sensor ready_time] Check {metadata_id} Failed! Check path:{path}".format(
                    metadata_id=check.metadata_id, path=check.check_path), exc_info=True)
            finally:
                if check.execution_date.utcoffset() is None:
                    check.execution_date.replace(tzinfo=timezone.beijing)
                SensorInfo.update_si_checked_time(dag_id=check.dag_id, metadata_id=check.metadata_id,
                                                  execution_date=check.execution_date,
                                                  detailed_execution_date=check.detailed_execution_date,
                                                  granularity=check.granularity, offset=check.offset)




class ReadyTimeSensenJob(LoggingMixin):
    def __init__(self):
        self.timeout = -1

    def start(self):
        process = multiprocessing.Process(
            target=sensor_job_readytime_processor,
            args=(),
        )
        process.start()