# -*- coding: utf-8 -*-
"""
Author: Tao Zhang
Date: 2021/12/3

       ,--.                              ,--.
,-----.|  ,---.  ,--,--.,--,--,  ,---. ,-'  '-. ,--,--. ,---.
`-.  / |  .-.  |' ,-.  ||      \| .-. |'-.  .-'' ,-.  || .-. |
 /  `-.|  | |  |\ '-'  ||  ||  |' '-' '  |  |  \ '-'  |' '-' '
`-----'`--' `--' `--`--'`--''--'.`-  /   `--'   `--`--' `---'
                                `---'
If you are doing your best,you will not have to worry about failure.
"""
import multiprocessing
import os
import signal
import sys
import time

from airflow.exceptions import AirflowTaskTimeout

from airflow.shareit.monitor.push_monitor import MonitorPush
from airflow.utils.timeout import timeout

from airflow.shareit.models.trigger import Trigger
from airflow.shareit.scheduler.scheduler_helper import SchedulerManager, QuickScheduler, BackfillProccess, \
    PushFastExecuteProccess, BackfillJobProccess
from airflow.utils import timezone
from airflow.utils.log.logging_mixin import LoggingMixin


log=LoggingMixin().logger


def external_and_reliance(lock):
    lock.acquire()  # 加锁，只允许一个进程运行
    from airflow.shareit.utils.task_util import TaskUtil
    try :
        tu = TaskUtil()
        # tu.check_outer_dataset()
        # log.info("[DataStudio Pipeline] external_data_processing execute...")
        # tu.external_data_processing()
        log.info("[DataStudio Pipeline] trigger_self_reliance_task execute...")
        tu.trigger_self_reliance_task()
        lock.release() # 释放锁
    except Exception as e:
        lock.release() # 释放锁
        log.error("[DataStudio Pipeline] external_and_reliance loop  error: {err}".format(err=str(e)),
                       exc_info=True)


class DSSchedulerJob(LoggingMixin):
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
        last_check_external_data_time = timezone.utcnow()


        manager = multiprocessing.Manager()
        # 多进程共享变量,记录正在被其他进程处理的trigger
        tri_dict = manager.dict()
        # 外部数据和自依赖检测的锁,不并发执行
        external_and_reliance_lock = manager.Lock()

        QuickScheduler().start()
        BackfillProccess().start()
        BackfillJobProccess().start()
        # PushFastExecuteProccess().start()
        while (timezone.utcnow() - execute_start_time).total_seconds() < self.timeout or self.timeout == -1:
            # 获取当前待检查的条目 状态为checking 心跳超过间隔
            self.log.debug("Starting DS Scheduler Loop...")
            loop_start_time = time.time()
            if (timezone.utcnow() - last_check_external_data_time).total_seconds() > 60:
                from airflow.shareit.utils.task_util import TaskUtil
                self.pool.apply_async(external_and_reliance, (external_and_reliance_lock,))
                last_check_external_data_time = timezone.utcnow()
            try:
                self.log.info("[DataStudio Pipeline] main_scheduler_process execute...")
                SchedulerManager().main_scheduler_process(check_parallelism=self.check_parallelism)

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

    def list_split(self,items, n):
        return [items[i:i + n] for i in range(0, len(items), n)]
