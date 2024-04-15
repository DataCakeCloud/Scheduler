# -*- coding: utf-8 -*-

import multiprocessing
import os
import signal
import sys
import time

from airflow.exceptions import AirflowTaskTimeout

from airflow.shareit.monitor.push_monitor import MonitorPush
from airflow.utils.timeout import timeout

from airflow.shareit.models.trigger import Trigger
from airflow.shareit.scheduler.external_helper import ExternalManager
from airflow.shareit.scheduler.scheduler_helper import SchedulerManager, QuickScheduler
from airflow.utils import timezone
from airflow.utils.log.logging_mixin import LoggingMixin


log=LoggingMixin().logger

class DSExternalJob(LoggingMixin):
    def __init__(
            self,
            timeout=-1,
            check_parallelism=6,
            *args, **kwargs):
        self.timeout = timeout
        self.check_parallelism = check_parallelism
        self.pool = multiprocessing.Pool(processes=2)
        signal.signal(signal.SIGINT, self._exit_gracefully)
        signal.signal(signal.SIGTERM, self._exit_gracefully)

    def _exit_gracefully(self, signum, frame):
        """
        Helper method to clean up processor_agent to avoid leaving orphan processes.
        """
        self.log.info("Exiting gracefully upon receiving signal %s", signum)
        self.pool.close()
        self.pool.join()
        sys.exit(os.EX_OK)


    def run(self):
        try:
            self._execute()
        except Exception:
            raise

    def _execute(self):
        execute_start_time = timezone.utcnow()
        monitor_push = MonitorPush()
        while (timezone.utcnow() - execute_start_time).total_seconds() < self.timeout or self.timeout == -1:
            # 获取当前待检查的条目 状态为checking 心跳超过间隔
            self.log.debug("Starting ds external scheduler loop...")
            loop_start_time = time.time()

            try:
                with timeout(3):
                    monitor_push.push_to_prometheus(job_name="ds_external_job", gauge_name="ds_external_job",
                                                    documentation="ds_external_job")
            except AirflowTaskTimeout as e:
                self.log.error("[DataStudio Pipeline] push metrics timeout")
            except Exception as e:
                self.log.error("[DataStudio Pipeline] ds_external_job push metrics failed")

            try:
                self.log.info("[DataStudio Pipeline] main_scheduler_process execute...")
                ExternalManager().main_scheduler_process(check_parallelism=self.check_parallelism)

            except Exception as e:
                self.log.error("[DataStudio Pipeline] scheduler loop error: {err}".format(err=str(e)), exc_info=True)


            loop_end_time = time.time()
            loop_duration = loop_end_time - loop_start_time
            if loop_duration < 5:
                sleep_length = 5 - loop_duration
                self.log.debug(
                    "Sleeping for {0:.2f} seconds to prevent excessive logging"
                        .format(sleep_length))
                time.sleep(sleep_length)


