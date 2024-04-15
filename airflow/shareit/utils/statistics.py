# -*- coding: utf-8 -*-
"""
Author: zhangtao
CreateDate: 2020/9/7
"""
import datetime as dt
import re
from datetime import datetime

import pendulum
import six
from croniter import croniter

from airflow.models import DagModel
from airflow.models.dagbag import DagBag
from airflow.utils.alarm_adapter import send_alarm
from airflow.utils.db import provide_session
from airflow.utils.state import State

TIMEZONE = pendulum.timezone("UTC")

cron_presets = {
    '@hourly': '0 * * * *',
    '@daily': '0 0 * * *',
    '@weekly': '0 0 * * 0',
    '@monthly': '0 0 1 * *',
    '@quarterly': '0 0 1 */3 *',
    '@yearly': '0 0 1 1 *',
}


def is_naive(value):
    """
    Determine if a given datetime.datetime is naive.
    The concept is defined in Python's docs:
    http://docs.python.org/library/datetime.html#datetime.tzinfo
    Assuming value.tzinfo is either None or a proper datetime.tzinfo,
    value.utcoffset() implements the appropriate logic.
    """
    return value.utcoffset() is None


def make_naive(value, timezone=None):
    """
    Make an aware datetime.datetime naive in a given time zone.

    :param value: datetime
    :param timezone: timezone
    :return: naive datetime
    """
    if timezone is None:
        timezone = TIMEZONE

    # Emulate the behavior of astimezone() on Python < 3.6.
    if is_naive(value):
        raise ValueError("make_naive() cannot be applied to a naive datetime")

    # o = value.astimezone(timezone)
    o = value.replace(tzinfo=TIMEZONE)
    # cross library compatibility
    naive = dt.datetime(o.year,
                        o.month,
                        o.day,
                        o.hour,
                        o.minute,
                        o.second,
                        o.microsecond)

    return naive


class StatisticsUtil(object):
    def __init__(self):
        self.total = 0
        self.success = 0
        self.failed = 0
        self.running = 0
        self.unready = 0
        self.failedList = []
        self.unreadyList = []

    def _statistics_result(self, isDetail=True):
        if self._check_statistics():
            if isDetail:
                return {"Total": self.total,
                        "Success": self.success,
                        "Failed": self.failed,
                        "Running": self.running,
                        "Unready": self.unready,
                        "FailedTaskList": self.failedList,
                        "UnreadyTaskList": self.unreadyList}
            else:
                return {"Total": self.total,
                        "Success": self.success,
                        "Failed": self.failed,
                        "Running": self.running,
                        "Unready": self.unready}
        else:
            raise ValueError("Statistics error!!")

    def _normalized_schedule_interval(self, schedule_interval=None):
        if isinstance(schedule_interval, six.string_types) and schedule_interval in cron_presets:
            _schedule_interval = cron_presets.get(schedule_interval)
        elif schedule_interval == '@once':
            _schedule_interval = None
        else:
            _schedule_interval = schedule_interval
        return _schedule_interval

    def _check_statistics(self):
        if len(self.failedList) != self.failed:
            return False
        if len(self.unreadyList) != self.unready:
            return False
        if self.total != (self.success + self.failed + self.running + self.unready):
            return False
        return True

    def _is_send_alarm_default(self):
        if isinstance(self.failed, int) and self.failed > 0:
            return True
        elif isinstance(self.unready, int) and self.unready > 0:
            return True
        else:
            return False

    def get_dag(self, dag_id=None, fileloc=None, store_serialized_dags=False):
        dag = DagBag(
            dag_folder=fileloc, store_serialized_dags=store_serialized_dags).get_dag(dag_id)
        if store_serialized_dags and dag is None:
            dag = self.get_dag()
        return dag

    @provide_session
    def get_statistics(self, session=None, dag_id=None, task_id=None, skipList=None, **kwargs):
        if (dag_id is not None and not (isinstance(dag_id, str) or isinstance(dag_id, unicode))) or dag_id is None:
            raise ValueError("Have input wrong dag_id!!")
        if skipList is not None and not isinstance(skipList,list):
            raise ValueError("Argument skipList must be a list !!")
        # Filter out the dag to be counted according to dag_id
        dags = session.query(DagModel).all()
        statistics_dag = []
        for dag in dags:
            if dag.is_paused or not dag.is_active:
                continue
            if skipList is not None and dag.dag_id in skipList:
                continue
            if dag_id == "all":
                statistics_dag.append(dag)
            else:
                regex = re.match(dag_id, dag.dag_id, re.I)
                if regex and regex.span() == (0, len(dag.dag_id)):
                    statistics_dag.append(dag)
        # Find out the most recent dag_run according to dag
        dagrun_list = []
        dag_info_list = []
        for dag in statistics_dag:
            dagrun = dag.get_last_dagrun()
            if dagrun:
                dagrun_list.append(dagrun)
                dag_info_list.append({"schedule_interval": self._normalized_schedule_interval(dag.schedule_interval),
                                      "fileLoc": dag.fileloc})
            else:
                # Handle the situation without dagrun
                print("Dag:{dag_id} no statistics required!".format(dag_id=dag.dag_id))

        dttm = pendulum.instance(datetime.utcnow())
        naive = make_naive(dttm, pendulum.timezone("UTC"))
        for i, dagrun in enumerate(dagrun_list):
            tasks = dagrun.get_task_instances()
            # Remove dag_run that should run at the current time but have not yet created task_instance,
            # and add all its tasks to the unready list
            print("Count task {id} status:{dag_id} {execution_date} ..."
                  .format(id=i, dag_id=dagrun.dag_id, execution_date=str(dagrun.execution_date)))
            if dag_info_list[i]["schedule_interval"] is None:
                continue
            cron = croniter(dag_info_list[i]["schedule_interval"], naive)
            cur = cron.get_prev(datetime)
            pre = cron.get_prev(datetime)
            delta = naive - cur
            cron_delta = cur - pre
            previous = dttm.in_timezone(TIMEZONE).subtract_timedelta(delta).subtract_timedelta(cron_delta)
            if tasks:
                if pendulum.instance(dagrun.execution_date) >= previous:
                    for task in tasks:
                        is_statistics = False
                        if task_id is None:
                            is_statistics = True
                        else:
                            regex = re.match(task_id, task.task_id, re.I)
                            if regex and regex.span() == (0, len(task.task_id)):
                                is_statistics = True
                        # 根据每个dag_run 统计对应的任务 success、failed、running
                        if is_statistics:
                            if task.state == State.RUNNING or \
                                task.state == State.UP_FOR_RETRY or \
                                task.state == State.UP_FOR_RESCHEDULE or \
                                task.state == State.QUEUED or \
                                task.state == State.SCHEDULED:
                                self.running += 1
                            elif task.state == State.SUCCESS or task.state == State.SKIPPED:
                                self.success += 1
                            elif task.state == State.FAILED:
                                self.failed += 1
                                self.failedList.append(str(dagrun.dag_id) + "." + str(task.task_id))
                            else:
                                self.unready += 1
                                self.unreadyList.append(str(dagrun.dag_id) + "." + str(task.task_id))
                            self.total += 1
                else:
                    for task in tasks:
                        is_statistics = False
                        if task_id is None:
                            is_statistics = True
                        else:
                            regex = re.match(task_id, task.task_id, re.I)
                            if regex and regex.span() == (0, len(task.task_id)):
                                is_statistics = True
                        if is_statistics:
                            self.unready += 1
                            self.unreadyList.append(str(dagrun.dag_id) + "." + str(task.task_id))
                            self.total += 1
            else:
                # With dagrun without task_instance, all tasks will be counted as unready
                dag = self.get_dag(dag_id=dagrun.dag_id, fileloc=dag_info_list[i]["fileLoc"],
                                   store_serialized_dags=True)
                task_ids = dag.task_ids
                for id in task_ids:
                    self.unready += 1
                    self.unreadyList.append(str(dagrun.dag_id) + "." + str(id))
                    self.total += 1
        # Return the result as a dictionary
        return self._statistics_result()

    def send_SCMP_alarm(self,
                        func=None,
                        isDetail=None,
                        group=None,
                        severity=None,
                        subject=None,
                        appendMessage=None):
        if func is None:
            if not self._is_send_alarm_default():
                return False
        elif not func(total=self.total,
                      success=self.success,
                      failed=self.failed,
                      running=self.running,
                      unready=self.unready):
            return False

        if subject is None:
            subject = "Airflow statistics alarm"
        if severity is None:
            severity = "warning"
        message = []
        statistics_res = self._statistics_result(isDetail)
        for key in statistics_res:
            if 'List' in key:
                message.append(str(key) + ":" + ";".join(statistics_res[key]))
            else:
                message.append(str(key) + ":" + str(statistics_res[key]))
        alarm_message = str(appendMessage) + "   ".join(message)
        send_alarm(groups=group, subject=subject, severity=severity, message=alarm_message)
