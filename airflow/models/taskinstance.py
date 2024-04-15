# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import copy
import functools
import getpass
import hashlib
import logging
import math
import os
import signal
import time
import json
import traceback
from datetime import timedelta
from typing import Optional
from urllib.parse import quote

import dill
import lazy_object_proxy
import pendulum
import six
from six.moves.urllib.parse import quote_plus
from jinja2 import TemplateAssertionError, UndefinedError
from sqlalchemy import Column, Float, Index, Integer, PickleType, String, func, DateTime
from sqlalchemy.orm import reconstructor
from sqlalchemy.orm.session import Session

from airflow import settings
from airflow.configuration import conf
from airflow.exceptions import (
    AirflowException, AirflowTaskTimeout, AirflowSkipException, AirflowRescheduleException
)
from airflow.models.base import Base, ID_LEN
from airflow.models.log import Log
from airflow.models.pool import Pool
from airflow.models.taskfail import TaskFail
from airflow.models.taskreschedule import TaskReschedule
from airflow.models.variable import Variable
from airflow.models.xcom import XCom, XCOM_RETURN_KEY
from airflow.sentry import Sentry
from airflow.settings import Stats
from airflow.shareit.models.event_trigger_mapping import EventTriggerMapping
from airflow.shareit.models.task_desc import TaskDesc
from airflow.shareit.models.taskinstance_log import TaskInstanceLog
from airflow.shareit.trigger.trigger_helper import TriggerHelper
from airflow.shareit.utils import task_manager
from airflow.shareit.utils.alarm_adapter import  build_notify_content_email
from airflow.shareit.utils.date_calculation_util import DateCalculationUtil
from airflow.shareit.constant.taskInstance import *
from airflow.shareit.utils.date_util import DateUtil
from airflow.shareit.utils.normal_util import NormalUtil
from airflow.ti_deps.dep_context import DepContext, REQUEUEABLE_DEPS, RUNNING_DEPS
from airflow.settings import STORE_SERIALIZED_DAGS
from airflow.utils import timezone
from airflow.utils.alarm_adapter import send_alarm
from airflow.utils.db import provide_session
from airflow.utils.email import send_email
from airflow.utils.helpers import is_container
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.net import get_hostname
from airflow.utils.sqlalchemy import UtcDateTime
from airflow.utils.state import State
from airflow.utils.timeout import timeout
from airflow.utils.timezone import utcnow, beijing, make_naive
from airflow.shareit.models.gray_test import GrayTest

def clear_task_instances(tis,
                         session,
                         activate_dag_runs=True,
                         dag=None,
                         ):
    """
    Clears a set of task instances, but makes sure the running ones
    get killed.

    :param tis: a list of task instances
    :param session: current session
    :param activate_dag_runs: flag to check for active dag run
    :param dag: DAG object
    """
    job_ids = []
    for ti in tis:
        if ti.state == State.RUNNING:
            if ti.job_id:
                ti.state = State.SHUTDOWN
                job_ids.append(ti.job_id)
        else:
            task_id = ti.task_id
            if dag and dag.has_task(task_id):
                task = dag.get_task(task_id)
                ti.refresh_from_task(task)
                task_retries = task.retries
                ti.max_tries = ti.try_number + task_retries - 1
            else:
                # Ignore errors when updating max_tries if dag is None or
                # task not found in dag since database records could be
                # outdated. We make max_tries the maximum value of its
                # original max_tries or the last attempted try number.
                ti.max_tries = max(ti.max_tries, ti.prev_attempted_tries)
            ti.state = State.SCHEDULED
            session.merge(ti)
        # Clear all reschedules related to the ti to clear
        TR = TaskReschedule
        session.query(TR).filter(
            TR.dag_id == ti.dag_id,
            TR.task_id == ti.task_id,
            TR.execution_date == ti.execution_date,
            TR.try_number == ti.try_number
        ).delete()

    if job_ids:
        from airflow.jobs import BaseJob as BJ
        for job in session.query(BJ).filter(BJ.id.in_(job_ids)).all():
            job.state = State.SHUTDOWN

    if activate_dag_runs and tis:
        from airflow.models.dagrun import DagRun  # Avoid circular import
        drs = session.query(DagRun).filter(
            DagRun.dag_id.in_({ti.dag_id for ti in tis}),
            DagRun.execution_date.in_({ti.execution_date for ti in tis}),
        ).all()
        for dr in drs:
            dr.state = State.RUNNING
            dr.start_date = timezone.utcnow()


class TaskInstance(Base, LoggingMixin):
    """
    Task instances store the state of a task instance. This table is the
    authority and single source of truth around what tasks have run and the
    state they are in.

    The SqlAlchemy model doesn't have a SqlAlchemy foreign key to the task or
    dag model deliberately to have more control over transactions.

    Database transactions on this table should insure double triggers and
    any confusion around what task instances are or aren't ready to run
    even while multiple schedulers may be firing task instances.
    """

    __tablename__ = "task_instance"

    task_id = Column(String(ID_LEN), primary_key=True)
    dag_id = Column(String(ID_LEN), primary_key=True)
    execution_date = Column(UtcDateTime, primary_key=True)
    start_date = Column(UtcDateTime)
    end_date = Column(UtcDateTime)
    duration = Column(Float)
    state = Column(String(20))
    _try_number = Column('try_number', Integer, default=0)
    max_tries = Column(Integer)
    hostname = Column(String(1000))
    unixname = Column(String(1000))
    job_id = Column(Integer)
    pool = Column(String(50), nullable=False)
    pool_slots = Column(Integer, default=1)
    queue = Column(String(256))
    priority_weight = Column(Integer)
    operator = Column(String(1000))
    queued_dttm = Column(UtcDateTime)
    pid = Column(Integer)
    executor_config = Column(PickleType(pickler=dill))
    external_conf = Column(String(500))
    task_type = Column(String(50), default='pipeline')  # backCalculation,pipeline
    create_date = Column(DateTime, server_default=func.now())
    update_date = Column(DateTime, server_default=func.now(), onupdate=func.now())
    backfill_label = Column(String)
    # If adding new fields here then remember to add them to
    # refresh_from_db() or they wont display in the UI correctly

    __table_args__ = (
        Index('ti_dag_state', dag_id, state),
        Index('ti_dag_date', dag_id, execution_date),
        Index('ti_state', state),
        Index('ti_state_lkp', dag_id, task_id, execution_date, state),
        Index('ti_pool', pool, state, priority_weight),
        Index('ti_job_id', job_id),
    )

    def __init__(self, task, execution_date, state=None,task_type=PIPELINE,start_date=None,end_date=None,external_conf=None):
        self.dag_id = task.dag_id
        self.task_id = task.task_id
        self.task = task
        self.refresh_from_task(task)
        self._log = logging.getLogger("airflow.task")

        # make sure we have a localized execution_date stored in UTC
        if execution_date and not timezone.is_localized(execution_date):
            self.log.warning("execution date %s has no timezone information. Using "
                             "default from dag or system", execution_date)
            if self.task.has_dag():
                execution_date = timezone.make_aware(execution_date,
                                                     self.task.dag.timezone)
            else:
                execution_date = timezone.make_aware(execution_date)

            execution_date = timezone.convert_to_utc(execution_date)

        self.execution_date = execution_date

        self.try_number = 0
        self.unixname = getpass.getuser()
        if state:
            self.state = state
        self.task_type = task_type
        self.hostname = ''
        self.init_on_load()
        # Is this TaskInstance being currently running within `airflow run --raw`.
        # Not persisted to the database so only valid for the current process
        self.raw = False
        if start_date:
            self.start_date = start_date
        if end_date:
            self.end_date = end_date
        if external_conf:
            self.external_conf = external_conf

    @reconstructor
    def init_on_load(self):
        """ Initialize the attributes that aren't stored in the DB. """
        self.test_mode = False  # can be changed when calling 'run'

    @property
    def try_number(self):
        """
        Return the try number that this task number will be when it is actually
        run.

        If the TI is currently running, this will match the column in the
        database, in all other cases this will be incremented.
        """
        # This is designed so that task logs end up in the right file.
        if self.state == State.RUNNING:
            return self._try_number
        return self._try_number + 1

    @try_number.setter
    def try_number(self, value):
        self._try_number = value

    @property
    def prev_attempted_tries(self):
        """
        Based on this instance's try_number, this will calculate
        the number of previously attempted tries, defaulting to 0.
        """
        # Expose this for the Task Tries and Gantt graph views.
        # Using `try_number` throws off the counts for non-running tasks.
        # Also useful in error logging contexts to get
        # the try number for the last try that was attempted.
        # https://issues.apache.org/jira/browse/AIRFLOW-2143

        return self._try_number

    @property
    def next_try_number(self):
        return self._try_number + 1

    def command(
            self,
            mark_success=False,
            ignore_all_deps=False,
            ignore_depends_on_past=False,
            ignore_task_deps=False,
            ignore_ti_state=False,
            local=False,
            pickle_id=None,
            raw=False,
            job_id=None,
            pool=None,
            cfg_path=None):
        """
        Returns a command that can be executed anywhere where airflow is
        installed. This command is part of the message sent to executors by
        the orchestrator.
        """
        return " ".join(self.command_as_list(
            mark_success=mark_success,
            ignore_all_deps=ignore_all_deps,
            ignore_depends_on_past=ignore_depends_on_past,
            ignore_task_deps=ignore_task_deps,
            ignore_ti_state=ignore_ti_state,
            local=local,
            pickle_id=pickle_id,
            raw=raw,
            job_id=job_id,
            pool=pool,
            cfg_path=cfg_path))

    def command_as_list(
            self,
            mark_success=False,
            ignore_all_deps=False,
            ignore_task_deps=False,
            ignore_depends_on_past=False,
            ignore_ti_state=False,
            local=False,
            pickle_id=None,
            raw=False,
            job_id=None,
            pool=None,
            cfg_path=None):
        """
        Returns a command that can be executed anywhere where airflow is
        installed. This command is part of the message sent to executors by
        the orchestrator.
        """
        dag = self.task.dag

        should_pass_filepath = not pickle_id and dag
        if should_pass_filepath and dag.full_filepath != dag.filepath:
            path = "DAGS_FOLDER/{}".format(dag.filepath)
        elif should_pass_filepath and dag.full_filepath:
            path = dag.full_filepath
        else:
            path = None

        return TaskInstance.generate_command(
            self.dag_id,
            self.task_id,
            self.execution_date,
            mark_success=mark_success,
            ignore_all_deps=ignore_all_deps,
            ignore_task_deps=ignore_task_deps,
            ignore_depends_on_past=ignore_depends_on_past,
            ignore_ti_state=ignore_ti_state,
            local=local,
            pickle_id=pickle_id,
            file_path=path,
            raw=raw,
            job_id=job_id,
            pool=pool,
            cfg_path=cfg_path)

    @staticmethod
    def generate_command(dag_id,
                         task_id,
                         execution_date,
                         mark_success=False,
                         ignore_all_deps=False,
                         ignore_depends_on_past=False,
                         ignore_task_deps=False,
                         ignore_ti_state=False,
                         local=False,
                         pickle_id=None,
                         file_path=None,
                         raw=False,
                         job_id=None,
                         pool=None,
                         cfg_path=None
                         ):
        """
        Generates the shell command required to execute this task instance.

        :param dag_id: DAG ID
        :type dag_id: unicode
        :param task_id: Task ID
        :type task_id: unicode
        :param execution_date: Execution date for the task
        :type execution_date: datetime.datetime
        :param mark_success: Whether to mark the task as successful
        :type mark_success: bool
        :param ignore_all_deps: Ignore all ignorable dependencies.
            Overrides the other ignore_* parameters.
        :type ignore_all_deps: bool
        :param ignore_depends_on_past: Ignore depends_on_past parameter of DAGs
            (e.g. for Backfills)
        :type ignore_depends_on_past: bool
        :param ignore_task_deps: Ignore task-specific dependencies such as depends_on_past
            and trigger rule
        :type ignore_task_deps: bool
        :param ignore_ti_state: Ignore the task instance's previous failure/success
        :type ignore_ti_state: bool
        :param local: Whether to run the task locally
        :type local: bool
        :param pickle_id: If the DAG was serialized to the DB, the ID
            associated with the pickled DAG
        :type pickle_id: unicode
        :param file_path: path to the file containing the DAG definition
        :param raw: raw mode (needs more details)
        :param job_id: job ID (needs more details)
        :param pool: the Airflow pool that the task should run in
        :type pool: unicode
        :param cfg_path: the Path to the configuration file
        :type cfg_path: basestring
        :return: shell command that can be used to run the task instance
        """
        iso = execution_date.isoformat()
        cmd = ["airflow", "run", str(dag_id), str(task_id), str(iso)]
        cmd.extend(["--mark_success"]) if mark_success else None
        cmd.extend(["--pickle", str(pickle_id)]) if pickle_id else None
        cmd.extend(["--job_id", str(job_id)]) if job_id else None
        cmd.extend(["-A"]) if ignore_all_deps else None
        cmd.extend(["-i"]) if ignore_task_deps else None
        cmd.extend(["-I"]) if ignore_depends_on_past else None
        cmd.extend(["--force"]) if ignore_ti_state else None
        cmd.extend(["--local"]) if local else None
        cmd.extend(["--pool", pool]) if pool else None
        cmd.extend(["--raw"]) if raw else None
        cmd.extend(["-sd", file_path]) if file_path else None
        cmd.extend(["--cfg_path", cfg_path]) if cfg_path else None
        return cmd

    @property
    def log_filepath(self):
        iso = self.execution_date.isoformat()
        log = os.path.expanduser(conf.get('core', 'BASE_LOG_FOLDER'))
        return ("{log}/{dag_id}/{task_id}/{iso}.log".format(
            log=log, dag_id=self.dag_id, task_id=self.task_id, iso=iso))

    @property
    def log_url(self):
        iso = self.execution_date.isoformat()
        base_url = conf.get('webserver', 'BASE_URL')
        relative_url = '/log?execution_date={iso}&task_id={task_id}&dag_id={dag_id}'.format(
            iso=quote_plus(iso), task_id=quote_plus(self.task_id), dag_id=quote_plus(self.dag_id))

        if conf.getboolean('webserver', 'rbac'):
            return '{base_url}{relative_url}'.format(base_url=base_url, relative_url=relative_url)
        return '{base_url}/admin/airflow{relative_url}'.format(base_url=base_url, relative_url=relative_url)

    @property
    def mark_success_url(self):
        iso = quote(self.execution_date.isoformat())
        base_url = conf.get('webserver', 'BASE_URL')
        return base_url + (
            "/success"
            "?task_id={task_id}"
            "&dag_id={dag_id}"
            "&execution_date={iso}"
            "&upstream=false"
            "&downstream=false"
        ).format(task_id=self.task_id, dag_id=self.dag_id, iso=iso)

    @provide_session
    def current_state(self, session=None):
        """
        Get the very latest state from the database, if a session is passed,
        we use and looking up the state becomes part of the session, otherwise
        a new session is used.
        """
        TI = TaskInstance
        ti = session.query(TI).filter(
            TI.dag_id == self.dag_id,
            TI.task_id == self.task_id,
            TI.execution_date == self.execution_date,
        ).all()
        if ti:
            state = ti[0].state
        else:
            state = None
        return state

    @provide_session
    def error(self, session=None):
        """
        Forces the task instance's state to FAILED in the database.
        """
        self.log.error("Recording the task instance as FAILED")
        self.state = State.FAILED
        session.merge(self)
        session.commit()

    @provide_session
    def refresh_from_db(self, session=None, lock_for_update=False):
        """
        Refreshes the task instance from the database based on the primary key

        :param lock_for_update: if True, indicates that the database should
            lock the TaskInstance (issuing a FOR UPDATE clause) until the
            session is committed.
        """
        TI = TaskInstance

        qry = session.query(TI).filter(
            TI.dag_id == self.dag_id,
            TI.task_id == self.task_id,
            TI.execution_date == self.execution_date)

        if lock_for_update:
            ti = qry.with_for_update().first()
        else:
            ti = qry.first()
        if ti:
            # Fields ordered per model definition
            self.start_date = ti.start_date
            self.end_date = ti.end_date
            self.duration = ti.duration
            self.state = ti.state
            # Get the raw value of try_number column, don't read through the
            # accessor here otherwise it will be incremeneted by one already.
            self.try_number = ti._try_number
            self.max_tries = ti.max_tries
            self.hostname = ti.hostname
            self.unixname = ti.unixname
            self.job_id = ti.job_id
            self.pool = ti.pool
            self.pool_slots = ti.pool_slots or 1
            self.queue = ti.queue
            self.priority_weight = ti.priority_weight
            self.operator = ti.operator
            self.queued_dttm = ti.queued_dttm
            self.pid = ti.pid
            self.task_type = ti.task_type
            self.external_conf = ti.external_conf
            self.backfill_label = ti.backfill_label
        else:
            self.state = None

    def refresh_from_task(self, task, pool_override=None):
        """
        Copy common attributes from the given task.

        :param task: The task object to copy from
        :type task: airflow.models.BaseOperator
        :param pool_override: Use the pool_override instead of task's pool
        :type pool_override: str
        """
        self.queue = task.queue
        self.pool = pool_override or task.pool
        self.pool_slots = task.pool_slots
        self.priority_weight = task.priority_weight_total
        self.run_as_user = task.run_as_user
        self.max_tries = task.retries
        self.executor_config = task.executor_config
        self.operator = task.__class__.__name__

    @provide_session
    def clear_xcom_data(self, session=None):
        """
        Clears all XCom data from the database for the task instance
        """
        session.query(XCom).filter(
            XCom.dag_id == self.dag_id,
            XCom.task_id == self.task_id,
            XCom.execution_date == self.execution_date
        ).delete()
        session.commit()

    @property
    def key(self):
        """
        Returns a tuple that identifies the task instance uniquely
        """
        return self.dag_id, self.task_id, self.execution_date, self.try_number

    @provide_session
    def set_state(self, state, session=None, commit=True):
        self.state = state
        self.start_date = timezone.utcnow()
        self.end_date = timezone.utcnow()
        session.merge(self)
        if commit:
            session.commit()

    @property
    def is_premature(self):
        """
        Returns whether a task is in UP_FOR_RETRY state and its retry interval
        has elapsed.
        """
        # is the task still in the retry waiting period?
        return self.state == State.UP_FOR_RETRY and not self.ready_for_retry()

    @provide_session
    def save_external_conf(self, ex_conf=None, session=None):
        if conf:
            from airflow.shareit.models.task_desc import TaskDesc
            td = TaskDesc()
            task_version = td.get_task_version(task_name=self.task_id)
            if not task_version:
                task_version = 0
            ex_conf["version"] = task_version
            self.external_conf = json.dumps(ex_conf)
        session.merge(self)
        session.commit()

    def get_external_conf(self):
        try:
            res = json.loads(self.external_conf) if self.external_conf else None
        except Exception as e:
            res = self.external_conf
        return res

    @provide_session
    def are_dependents_done(self, session=None):
        """
        Checks whether the dependents of this task instance have all succeeded.
        This is meant to be used by wait_for_downstream.

        This is useful when you do not want to start processing the next
        schedule of a task until the dependents are done. For instance,
        if the task DROPs and recreates a table.
        """
        task = self.task

        if not task.downstream_task_ids:
            return True

        ti = session.query(func.count(TaskInstance.task_id)).filter(
            TaskInstance.dag_id == self.dag_id,
            TaskInstance.task_id.in_(task.downstream_task_ids),
            TaskInstance.execution_date == self.execution_date,
            TaskInstance.state == State.SUCCESS,
        )
        count = ti[0][0]
        return count == len(task.downstream_task_ids)

    @provide_session
    def _get_previous_ti(self, state=None, session=None):
        # type: (Optional[str], Session) -> Optional['TaskInstance']
        dag = self.task.dag
        if dag:
            dr = self.get_dagrun(session=session)

            # LEGACY: most likely running from unit tests
            if not dr:
                # Means that this TI is NOT being run from a DR, but from a catchup
                previous_scheduled_date = dag.previous_schedule(self.execution_date)
                if not previous_scheduled_date:
                    return None

                return TaskInstance(task=self.task, execution_date=previous_scheduled_date)

            dr.dag = dag

            # We always ignore schedule in dagrun lookup when `state` is given or `schedule_interval is None`.
            # For legacy reasons, when `catchup=True`, we use `get_previous_scheduled_dagrun` unless
            # `ignore_schedule` is `True`.
            ignore_schedule = state is not None or dag.schedule_interval is None
            if dag.catchup is True and not ignore_schedule:
                last_dagrun = dr.get_previous_scheduled_dagrun(session=session)
            else:
                last_dagrun = dr.get_previous_dagrun(session=session, state=state)

            if last_dagrun:
                return last_dagrun.get_task_instance(self.task_id, session=session)

        return None

    @property
    def previous_ti(self):  # type: () -> Optional['TaskInstance']
        """The task instance for the task that ran before this task instance."""
        return self._get_previous_ti()

    @property
    def previous_ti_success(self):  # type: () -> Optional['TaskInstance']
        """The ti from prior succesful dag run for this task, by execution date."""
        return self._get_previous_ti(state=State.SUCCESS)

    @property
    def previous_execution_date_success(self):  # type: () -> Optional[pendulum.datetime]
        """The execution date from property previous_ti_success."""
        self.log.debug("previous_execution_date_success was called")
        prev_ti = self._get_previous_ti(state=State.SUCCESS)
        return prev_ti and prev_ti.execution_date

    @property
    def previous_start_date_success(self):  # type: () -> Optional[pendulum.datetime]
        """The start date from property previous_ti_success."""
        self.log.debug("previous_start_date_success was called")
        prev_ti = self._get_previous_ti(state=State.SUCCESS)
        return prev_ti and prev_ti.start_date

    @provide_session
    def are_dependencies_met(
            self,
            dep_context=None,
            session=None,
            verbose=False):
        """
        Returns whether or not all the conditions are met for this task instance to be run
        given the context for the dependencies (e.g. a task instance being force run from
        the UI will ignore some dependencies).

        :param dep_context: The execution context that determines the dependencies that
            should be evaluated.
        :type dep_context: DepContext
        :param session: database session
        :type session: sqlalchemy.orm.session.Session
        :param verbose: whether log details on failed dependencies on
            info or debug log level
        :type verbose: bool
        """
        dep_context = dep_context or DepContext()
        failed = False
        verbose_aware_logger = self.log.info if verbose else self.log.debug
        for dep_status in self.get_failed_dep_statuses(
                dep_context=dep_context,
                session=session):
            failed = True

            verbose_aware_logger(
                "Dependencies not met for %s, dependency '%s' FAILED: %s",
                self, dep_status.dep_name, dep_status.reason
            )

        if failed:
            return False

        verbose_aware_logger("Dependencies all met for %s", self)
        return True

    @provide_session
    def get_failed_dep_statuses(
            self,
            dep_context=None,
            session=None):
        dep_context = dep_context or DepContext()
        for dep in dep_context.deps | self.task.deps:
            for dep_status in dep.get_dep_statuses(
                    self,
                    session,
                    dep_context):

                self.log.debug(
                    "%s dependency '%s' PASSED: %s, %s",
                    self, dep_status.dep_name, dep_status.passed, dep_status.reason
                )

                if not dep_status.passed:
                    yield dep_status

    def __repr__(self):
        return (
            "<TaskInstance: {ti.dag_id}.{ti.task_id} "
            "{ti.execution_date} [{ti.state}]>"
        ).format(ti=self)

    def next_retry_datetime(self):
        """
        Get datetime of the next retry if the task instance fails. For exponential
        backoff, retry_delay is used as base and will be converted to seconds.
        """
        delay = self.task.retry_delay
        if self.task.retry_exponential_backoff:
            # If the min_backoff calculation is below 1, it will be converted to 0 via int. Thus,
            # we must round up prior to converting to an int, otherwise a divide by zero error
            # will occurr in the modded_hash calculation.
            min_backoff = int(math.ceil(delay.total_seconds() * (2 ** (self.try_number - 2))))
            # deterministic per task instance
            hash = int(hashlib.sha1("{}#{}#{}#{}".format(self.dag_id,
                                                         self.task_id,
                                                         self.execution_date,
                                                         self.try_number)
                                    .encode('utf-8')).hexdigest(), 16)
            # between 1 and 1.0 * delay * (2^retry_number)
            modded_hash = min_backoff + hash % min_backoff
            # timedelta has a maximum representable value. The exponentiation
            # here means this value can be exceeded after a certain number
            # of tries (around 50 if the initial delay is 1s, even fewer if
            # the delay is larger). Cap the value here before creating a
            # timedelta object so the operation doesn't fail.
            delay_backoff_in_seconds = min(
                modded_hash,
                timedelta.max.total_seconds() - 1
            )
            delay = timedelta(seconds=delay_backoff_in_seconds)
            if self.task.max_retry_delay:
                delay = min(self.task.max_retry_delay, delay)
        return self.end_date + delay

    def ready_for_retry(self):
        """
        Checks on whether the task instance is in the right state and timeframe
        to be retried.
        """
        return (self.state == State.UP_FOR_RETRY and
                self.next_retry_datetime() < timezone.utcnow())

    @provide_session
    def pool_full(self, session):
        """
        Returns a boolean as to whether the slot pool has room for this
        task to run
        """
        if not self.task.pool:
            return False

        pool = (
            session
            .query(Pool)
            .filter(Pool.pool == self.task.pool)
            .first()
        )
        if not pool:
            return False
        open_slots = pool.open_slots(session=session)

        return open_slots <= 0

    @provide_session
    def get_dagrun(self, session):
        """
        Returns the DagRun for this TaskInstance

        :param session:
        :return: DagRun
        """
        from airflow.models.dagrun import DagRun  # Avoid circular import
        dr = session.query(DagRun).filter(
            DagRun.dag_id == self.dag_id,
            DagRun.execution_date == self.execution_date
        ).first()

        return dr

    @provide_session
    def _check_and_change_state_before_execution(
            self,
            verbose=True,
            ignore_all_deps=False,
            ignore_depends_on_past=False,
            ignore_task_deps=False,
            ignore_ti_state=False,
            mark_success=False,
            test_mode=False,
            job_id=None,
            pool=None,
            session=None):
        """
        Checks dependencies and then sets state to RUNNING if they are met. Returns
        True if and only if state is set to RUNNING, which implies that task should be
        executed, in preparation for _run_raw_task

        :param verbose: whether to turn on more verbose logging
        :type verbose: bool
        :param ignore_all_deps: Ignore all of the non-critical dependencies, just runs
        :type ignore_all_deps: bool
        :param ignore_depends_on_past: Ignore depends_on_past DAG attribute
        :type ignore_depends_on_past: bool
        :param ignore_task_deps: Don't check the dependencies of this TI's task
        :type ignore_task_deps: bool
        :param ignore_ti_state: Disregards previous task instance state
        :type ignore_ti_state: bool
        :param mark_success: Don't run the task, mark its state as success
        :type mark_success: bool
        :param test_mode: Doesn't record success or failure in the DB
        :type test_mode: bool
        :param pool: specifies the pool to use to run the task instance
        :type pool: str
        :return: whether the state was changed to running or not
        :rtype: bool
        """
        task = self.task
        self.refresh_from_task(task, pool_override=pool)
        self.test_mode = test_mode
        self.refresh_from_db(session=session, lock_for_update=True)
        self.job_id = job_id
        self.hostname = get_hostname()
        self.log.info(str(self.log.handlers))
        if not ignore_all_deps and not ignore_ti_state and self.state == State.SUCCESS:
            Stats.incr('previously_succeeded', 1, 1)

        # TODO: Logging needs cleanup, not clear what is being printed
        hr = "\n" + ("-" * 80)  # Line break

        if not mark_success:
            # Firstly find non-runnable and non-requeueable tis.
            # Since mark_success is not set, we do nothing.
            non_requeueable_dep_context = DepContext(
                deps=RUNNING_DEPS - REQUEUEABLE_DEPS,
                ignore_all_deps=ignore_all_deps,
                ignore_ti_state=ignore_ti_state,
                ignore_depends_on_past=ignore_depends_on_past,
                ignore_task_deps=ignore_task_deps)
            if not self.are_dependencies_met(
                    dep_context=non_requeueable_dep_context,
                    session=session,
                    verbose=True):
                session.commit()
                return False

            # For reporting purposes, we report based on 1-indexed,
            # not 0-indexed lists (i.e. Attempt 1 instead of
            # Attempt 0 for the first attempt).
            # Set the task start date. In case it was re-scheduled use the initial
            # start date that is recorded in task_reschedule table
            if self.start_date is None :
                self.start_date = timezone.utcnow()
            task_reschedules = TaskReschedule.find_for_task_instance(self, session)
            if task_reschedules:
                self.start_date = task_reschedules[0].start_date

            # Secondly we find non-runnable but requeueable tis. We reset its state.
            # This is because we might have hit concurrency limits,
            # e.g. because of backfilling.
            dep_context = DepContext(
                deps=REQUEUEABLE_DEPS,
                ignore_all_deps=ignore_all_deps,
                ignore_depends_on_past=ignore_depends_on_past,
                ignore_task_deps=ignore_task_deps,
                ignore_ti_state=ignore_ti_state)
            if not self.are_dependencies_met(
                    dep_context=dep_context,
                    session=session,
                    verbose=True):
                self.state = State.SCHEDULED
                self.log.warning(hr)
                self.log.warning(
                    "Rescheduling due to concurrency limits reached "
                    "at task runtime. Attempt %s of "
                    "%s. State set to SCHEDULED.", self.try_number, self.max_tries + 1
                )
                self.log.warning(hr)
                self.queued_dttm = timezone.utcnow()
                session.merge(self)
                session.commit()
                return False

        # print status message
        self.log.info(hr)
        self.log.info("Starting attempt %s of %s", self.try_number, self.max_tries + 1)
        self.log.info(hr)
        self._try_number += 1

        if not test_mode:
            session.add(Log(State.RUNNING, self))
        self.state = State.RUNNING
        self.pid = os.getpid()
        self.end_date = None
        if not test_mode:
            session.merge(self)
        session.commit()

        # Closing all pooled connections to prevent
        # "max number of connections reached"
        settings.engine.dispose()
        if verbose:
            if mark_success:
                self.log.info("Marking success for %s on %s", self.task, self.execution_date)
            else:
                self.log.info("Executing %s on %s", self.task, self.execution_date)
        return True

    @provide_session
    @Sentry.enrich_errors
    def _run_raw_task(
            self,
            mark_success=False,
            test_mode=False,
            job_id=None,
            pool=None,
            session=None):
        """
        Immediately runs the task (without checking or changing db state
        before execution) and then sets the appropriate final state after
        completion and runs any post-execute callbacks. Meant to be called
        only after another function changes the state to running.

        :param mark_success: Don't run the task, mark its state as success
        :type mark_success: bool
        :param test_mode: Doesn't record success or failure in the DB
        :type test_mode: bool
        :param pool: specifies the pool to use to run the task instance
        :type pool: str
        """
        from airflow.sensors.base_sensor_operator import BaseSensorOperator
        from airflow.models.renderedtifields import RenderedTaskInstanceFields as RTIF
        from airflow.shareit.trigger.trigger_helper import TriggerHelper
        task = self.task
        self.test_mode = test_mode
        self.refresh_from_task(task, pool_override=pool)
        self.refresh_from_db(session=session)
        self.job_id = job_id
        self.hostname = get_hostname()
        self.task_desc = TaskDesc.get_task(task_name=self.dag_id)

        context = {}
        actual_start_date = timezone.utcnow()
        try:
            if not mark_success:
                ex_conf = self.get_external_conf()
                if 'uuid' in ex_conf.keys():
                    if self.try_number <= 1:
                        task.submit_uuid = ex_conf['uuid']
                    template_params = EventTriggerMapping.get_template_params(ex_conf['uuid'])
                    if template_params:
                        context = self.get_template_context(json.loads(template_params))
                else:
                    context = self.get_template_context()

                task_copy = copy.copy(task)
                task_copy.task_desc = self.task_desc

                # Sensors in `poke` mode can block execution of DAGs when running
                # with single process executor, thus we change the mode to`reschedule`
                # to allow parallel task being scheduled and executed
                if isinstance(task_copy, BaseSensorOperator) and \
                        conf.get('core', 'executor') == "DebugExecutor":
                    self.log.warning("DebugExecutor changes sensor mode to 'reschedule'.")
                    task_copy.mode = 'reschedule'

                self.task = task_copy

                def signal_handler(signum, frame):
                    self.log.error("Received SIGTERM. Terminating subprocesses.")
                    task_copy.on_kill()
                    raise AirflowException("Task received SIGTERM signal")
                signal.signal(signal.SIGTERM, signal_handler)

                # Don't clear Xcom until the task is certain to execute
                self.clear_xcom_data()

                start_time = time.time()

                task_copy.flush()

                self.render_templates(context=context)
                if STORE_SERIALIZED_DAGS:
                    RTIF.write(RTIF(ti=self, render_templates=False), session=session)
                    try:
                        RTIF.delete_old_records(self.task_id, self.dag_id, session=session)
                    except Exception as e:
                        self.log.error("Error in delete_old_records while RenderedTaskInstanceFields",exc_info=True)
                        # self.log.exception(e)
                    session.commit()
                task_copy.pre_execute(context=context)


                if task.email_on_start and task.owner:
                    notify_type = conf.get('email', 'notify_type')
                    if self.is_send_notify(State.START,"email") and "email" in notify_type:
                        try:
                            self.email_notify_alert(State.START)
                        except Exception as e:
                            self.log.error("任务开始执行email发送通知失败,失败原因:{}".format(e))
                    if self.is_send_notify(State.START,"dingTalk") and "dingding" in notify_type:
                        try:
                            self.ding_notify_alert(State.START)
                        except Exception as e:
                            self.log.error("任务开始执行钉钉发送通知失败,失败原因:{}".format(e))
                    if self.is_send_notify(State.START,"phone") and "phone" in notify_type:
                        try:
                            self.phone_notify_alert(State.START,alert_head="任务开始执行")
                        except Exception as e:
                            self.log.error("任务开始电话通知失败,失败原因:{}".format(e))
                    if self.is_send_notify(State.START,"wechat") and "wechat" in notify_type:
                        try:
                            self.wechat_notify_alert(State.START)
                        except Exception as e:
                            self.log.error("任务开始企业微信通知失败,失败原因:{}".format(e))

                # If a timeout is specified for the task, make it fail
                # if it goes beyond
                result = None
                if task_copy.execution_timeout:
                    try:
                        with timeout(int(
                                task_copy.execution_timeout.total_seconds())):
                            result = task_copy.execute(context=context)
                    except AirflowTaskTimeout:
                        task_copy.on_kill()
                        raise
                else:
                    result = task_copy.execute(context=context)

                # If the task returns a result, push an XCom containing it
                if task_copy.do_xcom_push and result is not None:
                    self.xcom_push(key=XCOM_RETURN_KEY, value=result)

                task_copy.post_execute(context=context, result=result)

                end_time = time.time()
                duration = end_time - start_time
                Stats.timing(
                    'dag.{dag_id}.{task_id}.duration'.format(
                        dag_id=task_copy.dag_id,
                        task_id=task_copy.task_id),
                    duration)

                Stats.incr('operator_successes_{}'.format(
                    self.task.__class__.__name__), 1, 1)
                Stats.incr('ti_successes')
            self.refresh_from_db(lock_for_update=True)
            self.state = State.SUCCESS
        except AirflowSkipException as e:
            # Recording SKIP
            # log only if exception has any arguments to prevent log flooding
            if e.args:
                self.log.info(e)
            self.refresh_from_db(lock_for_update=True)
            self.state = State.SKIPPED
            self.log.info(
                'Marking task as SKIPPED.'
                'dag_id=%s, task_id=%s, execution_date=%s, start_date=%s, end_date=%s',
                self.dag_id,
                self.task_id,
                self.execution_date.strftime('%Y%m%dT%H%M%S') if hasattr(
                    self,
                    'execution_date') and self.execution_date else '',
                self.start_date.strftime('%Y%m%dT%H%M%S') if hasattr(
                    self,
                    'start_date') and self.start_date else '',
                self.end_date.strftime('%Y%m%dT%H%M%S') if hasattr(
                    self,
                    'end_date') and self.end_date else '')
        except AirflowRescheduleException as reschedule_exception:
            self.refresh_from_db()
            self._handle_reschedule(actual_start_date, reschedule_exception, test_mode, context)
            TriggerHelper.task_finish_trigger(self)
            return
        except AirflowException as e:
            self.refresh_from_db()
            # for case when task is marked as success/failed externally
            # current behavior doesn't hit the success callback
            if self.state in {State.SUCCESS, State.FAILED}:
                TriggerHelper.task_finish_trigger(self)
                return
            else:
                self.handle_failure(e, test_mode, context)
                TriggerHelper.task_finish_trigger(self)
                raise
        except (Exception, KeyboardInterrupt) as e:
            self.handle_failure(e, test_mode, context)
            TriggerHelper.task_finish_trigger(self)
            raise

        self.end_date = timezone.utcnow()
        self.set_duration()
        TriggerHelper.task_finish_trigger(self)
        if task.email_on_success and task.owner:
            notify_type = conf.get('email', 'notify_type')
            if self.is_send_notify(State.SUCCESS,"email") and "email" in notify_type:
                try:
                    self.email_notify_alert(State.SUCCESS)
                except Exception as e:
                    self.log.error("任务开始执行email发送通知失败,失败原因:{}".format(e))
            if self.is_send_notify(State.SUCCESS,"dingTalk") and "dingding" in notify_type:
                try:
                    self.ding_notify_alert(State.SUCCESS)
                except Exception as e:
                    self.log.error("任务成功钉钉发送通知失败,失败原因:{}".format(e))
            if self.is_send_notify(State.SUCCESS,"phone") and "phone" in notify_type:
                try:
                    self.phone_notify_alert(State.SUCCESS,alert_head="任务执行成功")
                except Exception as e:
                    self.log.error("任务成功电话通知失败,失败原因:{}".format(e))
            if self.is_send_notify(State.SUCCESS, "wechat") and "wechat" in notify_type:
                try:
                    self.wechat_notify_alert(State.SUCCESS)
                except Exception as e:
                    self.log.error("任务成功企业微信通知失败,失败原因:{}".format(e))
        # Success callback
        try:
            if task.on_success_callback:
                task.on_success_callback(context)
        except Exception as e3:
            self.log.error("Failed when executing success callback")
            self.log.exception(e3)
        # Recording SUCCESS
        try:
            self.log.info(
                'Marking task as SUCCESS.'
                'dag_id=%s, task_id=%s, execution_date=%s, start_date=%s, end_date=%s',
                self.dag_id,
                self.task_id,
                self.execution_date.strftime('%Y%m%dT%H%M%S.%f') if hasattr(
                    self,
                    'execution_date') and self.execution_date else '',
                self.start_date.strftime('%Y%m%dT%H%M%S.%f') if hasattr(
                    self,
                    'start_date') and self.start_date else '',
                self.end_date.strftime('%Y%m%dT%H%M%S.%f') if hasattr(
                    self,
                    'end_date') and self.end_date else '')
        except Exception as e:
            s = traceback.format_exc()
            self.log.error(s)
        if not test_mode:
            session.add(Log(self.state, self))
            session.merge(self)
        session.commit()





    @provide_session
    def run(
            self,
            verbose=True,
            ignore_all_deps=False,
            ignore_depends_on_past=False,
            ignore_task_deps=False,
            ignore_ti_state=False,
            mark_success=False,
            test_mode=False,
            job_id=None,
            pool=None,
            session=None):
        res = self._check_and_change_state_before_execution(
            verbose=verbose,
            ignore_all_deps=ignore_all_deps,
            ignore_depends_on_past=ignore_depends_on_past,
            ignore_task_deps=ignore_task_deps,
            ignore_ti_state=ignore_ti_state,
            mark_success=mark_success,
            test_mode=test_mode,
            job_id=job_id,
            pool=pool,
            session=session)
        if res:
            self._run_raw_task(
                mark_success=mark_success,
                test_mode=test_mode,
                job_id=job_id,
                pool=pool,
                session=session)


    def dry_run(self):
        task = self.task
        task_copy = copy.copy(task)
        self.task = task_copy

        self.render_templates()
        task_copy.dry_run()

    @provide_session
    def _handle_reschedule(self, actual_start_date, reschedule_exception, test_mode=False, context=None,
                           session=None):
        # Don't record reschedule request in test mode
        if test_mode:
            return

        self.end_date = timezone.utcnow()
        self.set_duration()

        # Log reschedule request
        session.add(TaskReschedule(self.task, self.execution_date, self._try_number,
                    actual_start_date, self.end_date,
                    reschedule_exception.reschedule_date))

        # set state
        self.state = State.UP_FOR_RESCHEDULE

        # Decrement try_number so subsequent runs will use the same try number and write
        # to same log file.
        self._try_number -= 1

        session.merge(self)
        session.commit()
        self.log.info('Rescheduling task, marking task as UP_FOR_RESCHEDULE')

    @provide_session
    def handle_failure(self, error, test_mode=None, context=None, session=None):
        if test_mode is None:
            test_mode = self.test_mode
        if context is None:
            context = self.get_template_context()

        self.log.exception(error)
        task = self.task
        self.end_date = timezone.utcnow()
        self.set_duration()
        td = TaskDesc.get_task(task_name=self.dag_id)
        Stats.incr('operator_failures_{}'.format(task.__class__.__name__), 1, 1)
        Stats.incr('ti_failures')
        if not test_mode:
            session.add(Log(State.FAILED, self))

        # Log failure duration
        session.add(TaskFail(task, self.execution_date, self.start_date, self.end_date))

        if context is not None:
            context['exception'] = error
        if not hasattr(self, 'task_desc'):
            self.task_desc = TaskDesc.get_task(task_name=self.dag_id)
        # Let's go deeper
        try:
            # Since this function is called only when the TI state is running,
            # try_number contains the current try_number (not the next). We
            # only mark task instance as FAILED if the next task instance
            # try_number exceeds the max_tries.
            if self.is_eligible_to_retry():
                self.state = State.UP_FOR_RETRY
                self.log.info('Marking task as UP_FOR_RETRY')

                if task.email_on_retry and task.owner:
                    notify_type = conf.get('email', 'notify_type')
                    if self.is_send_notify(State.RETRY,"email") and "email" in notify_type:
                        try:
                            self.email_notify_alert(State.RETRY)
                        except Exception as e:
                            self.log.error("任务开始执行email发送通知失败,失败原因:{}".format(e))
                    if self.is_send_notify(State.RETRY,"dingTalk") and "dingding" in notify_type:
                        try:
                            self.ding_notify_alert(State.RETRY)
                        except Exception as e:
                            self.log.error("任务重试钉钉发送通知失败,失败原因:{}".format(e))
                    if self.is_send_notify(State.RETRY,"phone") and "phone" in notify_type:
                        try:
                            self.phone_notify_alert(State.RETRY, alert_head="任务正在重试")
                        except Exception as e:
                            self.log.error("任务重试电话通知失败,失败原因:{}".format(e))
                    if self.is_send_notify(State.RETRY,"wechat") and "wechat" in notify_type:
                        try:
                            self.wechat_notify_alert(State.RETRY)
                        except Exception as e:
                            self.log.error("任务重试企业微信通知失败,失败原因:{}".format(e))
            else:
                self.state = State.FAILED
                if task.retries:
                    self.log.info(
                        'All retries failed; marking task as FAILED.'
                        'dag_id=%s, task_id=%s, execution_date=%s, start_date=%s, end_date=%s',
                        self.dag_id,
                        self.task_id,
                        self.execution_date.strftime('%Y%m%dT%H%M%S') if hasattr(
                            self,
                            'execution_date') and self.execution_date else '',
                        self.start_date.strftime('%Y%m%dT%H%M%S') if hasattr(
                            self,
                            'start_date') and self.start_date else '',
                        self.end_date.strftime('%Y%m%dT%H%M%S') if hasattr(
                            self,
                            'end_date') and self.end_date else '')
                else:
                    self.log.info(
                        'Marking task as FAILED.'
                        'dag_id=%s, task_id=%s, execution_date=%s, start_date=%s, end_date=%s',
                        self.dag_id,
                        self.task_id,
                        self.execution_date.strftime('%Y%m%dT%H%M%S') if hasattr(
                            self,
                            'execution_date') and self.execution_date else '',
                        self.start_date.strftime('%Y%m%dT%H%M%S') if hasattr(
                            self,
                            'start_date') and self.start_date else '',
                        self.end_date.strftime('%Y%m%dT%H%M%S') if hasattr(
                            self,
                            'end_date') and self.end_date else '')
                # 灰度测试任务的流程，如果失败了就关闭灰度
                ex_conf = self.get_external_conf()
                notify_type = conf.get('email', 'notify_type')
                if task.email_on_failure and task.owner:
                    if self.is_send_notify(State.FAILED,"email") and "email" in notify_type:
                        try:
                            self.email_notify_alert(State.FAILED)
                        except Exception as e:
                            self.log.error("任务开始执行email发送通知失败,{}".format(str(e)))
                    if self.is_send_notify(State.FAILED,"dingTalk") and "dingding" in notify_type:
                        try:
                            self.ding_notify_alert(State.FAILED)
                        except Exception as e:
                            self.log.error("任务失败钉钉发送通知失败,{}".format(str(e)))
                    if self.is_send_notify(State.FAILED,"phone") and "phone" in notify_type:
                        try:
                            self.phone_notify_alert(State.FAILED,alert_head="任务执行失败")
                        except Exception as e:
                            self.log.error("任务失败电话通知失败,{}".format(str(e)))
                    if self.is_send_notify(State.FAILED,"wechat") and "wechat" in notify_type:
                        try:
                            self.wechat_notify_alert(State.FAILED)
                        except Exception as e:
                            self.log.error("任务失败企业微信通知失败,:{}".format(str(e)))

        except Exception:
            self.log.error(traceback.format_exc())


        # Handling callbacks pessimistically
        try:
            if self.state == State.UP_FOR_RETRY and task.on_retry_callback:
                task.on_retry_callback(context)
            if self.state == State.FAILED and task.on_failure_callback:
                task.on_failure_callback(context)
        except Exception as e3:
            self.log.error("Failed at executing callback")
            self.log.exception(e3)

        if not test_mode:
            session.merge(self)
        session.commit()

    def is_eligible_to_retry(self):
        """Is task instance is eligible for retry"""
        return self.task.retries and self.try_number <= self.max_tries

    @provide_session
    def get_template_context(self, other_args=None,session=None):
        task = self.task
        from airflow.shareit.utils.task_cron_util import TaskCronUtil
        from airflow.shareit.utils.task_util import TaskUtil
        from airflow import macros
        tables = None
        if 'tables' in task.params:
            tables = task.params['tables']

        params = {}
        run_id = ''
        dag_run = None
        if hasattr(task, 'dag'):
            if task.dag.params:
                params.update(task.dag.params)
            from airflow.models.dagrun import DagRun  # Avoid circular import
            dag_run = (
                session.query(DagRun)
                .filter_by(
                    dag_id=task.dag.dag_id,
                    execution_date=self.execution_date)
                .first()
            )
            run_id = dag_run.run_id if dag_run else None
            session.expunge_all()
            session.commit()

        ds = self.execution_date.strftime('%Y-%m-%d')
        ts = self.execution_date.isoformat()
        yesterday_ds = (self.execution_date - timedelta(1)).strftime('%Y-%m-%d')
        tomorrow_ds = (self.execution_date + timedelta(1)).strftime('%Y-%m-%d')
        lastmonth_date = DateCalculationUtil.datetime_month_caculation(self.execution_date, -1).strftime('%Y-%m')

        # For manually triggered dagruns that aren't run on a schedule, next/previous
        # schedule dates don't make sense, and should be set to execution date for
        # consistency with how execution_date is set for manually triggered tasks, i.e.
        # triggered_date == execution_date.
        if dag_run and dag_run.external_trigger:
            prev_execution_date = self.execution_date
            prev_2_execution_date  = self.execution_date
            next_execution_date = self.execution_date
        else:
            # prev_execution_date = task.dag.previous_schedule(self.execution_date)
            # prev_2_execution_date = task.dag.previous_schedule(self.prev_execution_date)
            # next_execution_date = task.dag.following_schedule(self.execution_date)
            prev_execution_date = TaskCronUtil.get_pre_execution_date(task.dag_id,self.execution_date)
            prev_2_execution_date = TaskCronUtil.get_pre_execution_date(task.dag_id,prev_execution_date)
            next_execution_date = TaskCronUtil.get_next_execution_date(task.dag_id,self.execution_date)

        next_ds = None
        next_ds_nodash = None
        if next_execution_date:
            next_ds = next_execution_date.strftime('%Y-%m-%d')
            next_ds_nodash = next_ds.replace('-', '')
            next_execution_date = pendulum.instance(next_execution_date)

        prev_ds = None
        prev_ds_nodash = None
        if prev_execution_date:
            prev_ds = prev_execution_date.strftime('%Y-%m-%d')
            prev_ds_nodash = prev_ds.replace('-', '')
            prev_execution_date = pendulum.instance(prev_execution_date)

        if prev_2_execution_date:
            prev_2_execution_date = pendulum.instance(prev_2_execution_date)

        ds_nodash = ds.replace('-', '')
        ts_nodash = self.execution_date.strftime('%Y%m%dT%H%M%S')
        ts_nodash_with_tz = ts.replace('-', '').replace(':', '')
        yesterday_ds_nodash = yesterday_ds.replace('-', '')
        tomorrow_ds_nodash = tomorrow_ds.replace('-', '')
        lastmonth_date_ds_nodash = lastmonth_date.replace('-', '')

        ti_key_str = "{dag_id}__{task_id}__{ds_nodash}".format(
            dag_id=task.dag_id, task_id=task.task_id, ds_nodash=ds_nodash)

        if task.params:
            params.update(task.params)

        if conf.getboolean('core', 'dag_run_conf_overrides_params'):
            self.overwrite_params_with_dag_run_conf(params=params, dag_run=dag_run)

        class VariableAccessor:
            """
            Wrapper around Variable. This way you can get variables in
            templates by using ``{{ var.value.variable_name }}`` or
            ``{{ var.value.get('variable_name', 'fallback') }}``.
            """
            def __init__(self):
                self.var = None

            def __getattr__(
                self,
                item,
            ):
                self.var = Variable.get(item)
                return self.var

            def __repr__(self):
                return str(self.var)

            @staticmethod
            def get(
                item,
                default_var=Variable._Variable__NO_DEFAULT_SENTINEL,
            ):
                return Variable.get(item, default_var=default_var)

        class VariableJsonAccessor:
            """
            Wrapper around Variable. This way you can get variables in
            templates by using ``{{ var.json.variable_name }}`` or
            ``{{ var.json.get('variable_name', {'fall': 'back'}) }}``.
            """
            def __init__(self):
                self.var = None

            def __getattr__(
                self,
                item,
            ):
                self.var = Variable.get(item, deserialize_json=True)
                return self.var

            def __repr__(self):
                return str(self.var)

            @staticmethod
            def get(
                item,
                default_var=Variable._Variable__NO_DEFAULT_SENTINEL,
            ):
                return Variable.get(item, default_var=default_var, deserialize_json=True)

        context = {
            'conf': conf,
            'dag': task.dag,
            'ds': ds,
            'next_ds': next_ds,
            'next_ds_nodash': next_ds_nodash,
            'prev_ds': prev_ds,
            'prev_ds_nodash': prev_ds_nodash,
            'ds_nodash': ds_nodash,
            'ts': ts,
            'ts_nodash': ts_nodash,
            'ts_nodash_with_tz': ts_nodash_with_tz,
            'yesterday_ds': yesterday_ds,
            'yesterday_ds_nodash': yesterday_ds_nodash,
            'tomorrow_ds': tomorrow_ds,
            'tomorrow_ds_nodash': tomorrow_ds_nodash,
            'END_DATE': ds,
            'end_date': ds,
            'dag_run': dag_run,
            'run_id': run_id,
            'execution_date': pendulum.instance(self.execution_date),
            'prev_execution_date': prev_execution_date,
            'prev_2_execution_date': prev_2_execution_date,
            'prev_execution_date_success': lazy_object_proxy.Proxy(
                lambda: self.previous_execution_date_success),
            'prev_start_date_success': lazy_object_proxy.Proxy(lambda: self.previous_start_date_success),
            'next_execution_date': next_execution_date,
            'lastmonth_date': lastmonth_date,
            'lastmonth_date_ds_nodash':lastmonth_date_ds_nodash,
            'latest_date': ds,
            'macros': macros,
            'params': params,
            'tables': tables,
            'task': task,
            'task_instance': self,
            'ti': self,
            'task_instance_key_str': ti_key_str,
            'test_mode': self.test_mode,
            'var': {
                'value': VariableAccessor(),
                'json': VariableJsonAccessor()
            },
            'inlets': task.inlets,
            'outlets': task.outlets,
            'owner': task.owner.split(',')[0] if task.owner else ''
        }
        if other_args:
            for key, value in other_args.items():
                if key not in context:
                    context[key] = value
        context = TaskUtil.set_utc0_template_context(context)

        return context

    def get_rendered_template_fields(self):
        """
        Fetch rendered template fields from DB if Serialization is enabled.
        Else just render the templates
        """
        from airflow.models.renderedtifields import RenderedTaskInstanceFields
        if STORE_SERIALIZED_DAGS:
            rtif = RenderedTaskInstanceFields.get_templated_fields(self)
            if rtif:
                for field_name, rendered_value in rtif.items():
                    setattr(self.task, field_name, rendered_value)
            else:
                try:
                    self.render_templates()
                except (TemplateAssertionError, UndefinedError) as e:
                    six.raise_from(AirflowException(
                        "Webserver does not have access to User-defined Macros or Filters "
                        "when Dag Serialization is enabled. Hence for the task that have not yet "
                        "started running, please use 'airflow tasks render' for debugging the "
                        "rendering of template_fields."
                    ), e)
        else:
            self.render_templates()

    def overwrite_params_with_dag_run_conf(self, params, dag_run):
        if dag_run and dag_run.conf:
            params.update(dag_run.conf)

    def render_templates(self, context=None):
        """Render templates in the operator fields."""
        if not context:
            context = self.get_template_context()

        self.task.render_template_fields(context)

    def email_alert(self, exception):
        exception_html = str(exception).replace('\n', '<br>')
        jinja_context = self.get_template_context()
        # This function is called after changing the state
        # from State.RUNNING so use prev_attempted_tries.
        jinja_context.update(dict(
            exception=exception,
            exception_html=exception_html,
            try_number=self.prev_attempted_tries,
            max_tries=self.max_tries))

        jinja_env = self.task.get_template_env()

        default_subject = 'Airflow alert: {{ti}}'
        # For reporting purposes, we report based on 1-indexed,
        # not 0-indexed lists (i.e. Try 1 instead of
        # Try 0 for the first attempt).
        default_html_content = (
            'Try {{try_number}} out of {{max_tries + 1}}<br>'
            'Exception:<br>{{exception_html}}<br>'
            'Log: <a href="{{ti.log_url}}">Link</a><br>'
            'Host: {{ti.hostname}}<br>'
            'Log file: {{ti.log_filepath}}<br>'
            'Mark success: <a href="{{ti.mark_success_url}}">Link</a><br>'
        )

        default_html_content_err = (
            'Try {{try_number}} out of {{max_tries + 1}}<br>'
            'Exception:<br>Failed attempt to attach error logs<br>'
            'Log: <a href="{{ti.log_url}}">Link</a><br>'
            'Host: {{ti.hostname}}<br>'
            'Log file: {{ti.log_filepath}}<br>'
            'Mark success: <a href="{{ti.mark_success_url}}">Link</a><br>'
        )

        def render(key, content):
            if conf.has_option('email', key):
                path = conf.get('email', key)
                with open(path) as f:
                    content = f.read()

            return jinja_env.from_string(content).render(**jinja_context)

        subject = render('subject_template', default_subject)
        html_content = render('html_content_template', default_html_content)
        html_content_err = render('html_content_template', default_html_content_err)
        try:
            send_email(self.task.email, subject, html_content)
        except Exception:
            send_email(self.task.email, subject, html_content_err)

    def scmp_alert(self, exception, status):
        if status == 'up_for_retry':
            severity = self.task.alarm_severity_on_retry
        if status == 'failed':
            severity = self.task.alarm_severity_on_failure
        jinja_context = self.get_template_context()
        # This function is called after changing the state
        # from State.RUNNING so use prev_attempted_tries.
        jinja_context.update(dict(
            exception=exception,
            try_number=self.prev_attempted_tries,
            max_tries=self.max_tries))

        jinja_env = self.task.get_template_env()

        default_subject = 'Airflow alert: {{ti}}'
        # For reporting purposes, we report based on 1-indexed,
        # not 0-indexed lists (i.e. Try 1 instead of
        # Try 0 for the first attempt).
        default_content = (
            'Try {{try_number}} out of {{max_tries + 1}}\n'
            'Exception: {{exception}}\n'
            'Log: {{ti.log_url}}\n'
            'Host: {{ti.hostname}}\n'
            'Log file: {{ti.log_filepath}}\n'
            'Mark success: {{ti.mark_success_url}}\n'
        )

        subject = jinja_env.from_string(default_subject).render(**jinja_context)
        content = jinja_env.from_string(default_content).render(**jinja_context)
        send_alarm(groups=self.task.alarm_to_scmp_groups, subject=subject, message=content, severity=severity)

    def ding_group_chat_notify_alert(self,batch_id):
        from airflow.shareit.utils.alarm_adapter import send_ding_group_chat_notify
        task = TaskDesc.get_task(task_name=self.dag_id)
        task_name = task.ds_task_name
        ds_task_id = task.ds_task_id
        workflow_id = task.workflow_name
        trigger_type = ''
        if self.task_type == PIPELINE:
            trigger_type = '正常调度'
        if self.task_type == FIRST:
            trigger_type = '首次上线调度'
        if self.task_type == BACKFILL:
            trigger_type = '补数'
        if self.task_type == CLEAR:
            trigger_type = '重算'
        base_content = """__[DataCake 提示信息]__  
        <font color='#dd0000'>灰度任务执行失败</font><br />
        
        任务id: {ds_task_id}  
        任务名:  {task_name}   
        工作流id: {workflow_id} 
        实例日期: {execution_date}  
        实例类型: {trigger_type}  
        执行次数: {try_number}  
        batchId: {batch_id}  
                """
        content = base_content.format(ds_task_id=ds_task_id,
                                      task_name=task_name,
                                      execution_date=self.execution_date.strftime('%Y-%m-%dT%H:%M:%S'),
                                      batch_id=batch_id,
                                      workflow_id= workflow_id if workflow_id else '',
                                      try_number=str(self.try_number - 1 if self.try_number > 0 else 0),
                                      trigger_type=trigger_type
                                      )
        send_ding_group_chat_notify(title='灰度测试任务',content=content,)

    def ding_notify_alert(self,status=None):
        from airflow.shareit.utils.alarm_adapter import send_ding_notify,build_notify_content,get_receiver_list
        task_name = TaskDesc.get_task(task_name=self.dag_id).ds_task_name
        if self.task_id != self.dag_id:
            task_name = TaskDesc.get_task(task_name=self.dag_id).ds_task_name + "." + TaskDesc.get_task(task_name=self.task_id).ds_task_name
        content = build_notify_content(state=status, task_name=task_name,execution_date=self.execution_date)
        cur_task_param = TaskDesc.get_task(task_name=self.task_id).extra_param
        if cur_task_param.has_key(status):
            notifyCollaborator = json.loads(TaskDesc.get_task(task_name=self.task_id).extra_param[status])["notifyCollaborator"]
            if notifyCollaborator:
                if self.task.notified_owner:
                    receiver = get_receiver_list(receiver=self.task.notified_owner)
                else:
                    receiver = get_receiver_list(receiver=self.task.owner)
            else:
                receiver = get_receiver_list(receiver=self.task.owner)
        else:
            receiver = get_receiver_list(receiver=self.task.owner)
        send_ding_notify(content=content,receiver=receiver)

    def phone_notify_alert(self,status=None,alert_head=None):
        notifyCollaborator = False
        cur_task_param = TaskDesc.get_task(task_name=self.dag_id).extra_param
        if cur_task_param.has_key(status):
            notifyCollaborator = json.loads(TaskDesc.get_task(task_name=self.task_id).extra_param[status])[
                "notifyCollaborator"]
        from airflow.shareit.models.regular_alert import RegularAlert
        ra = RegularAlert()
        ra.sync_regular_alert_to_db(
            task_name=self.dag_id,
            execution_date=make_naive(self.execution_date),
            alert_type='phone',
            state=State.CHECKING,
            trigger_condition=status,
            is_notify_collaborator=notifyCollaborator,
            is_regular=False
        )

    def email_notify_alert(self,status=None):
        from airflow.shareit.utils.alarm_adapter import get_receiver_list,send_email_by_notifyAPI,build_notify_content
        # task_name = TaskDesc.get_task(task_name=self.dag_id).ds_task_name
        # if self.task_id != self.dag_id:
        #     task_name = TaskDesc.get_task(task_name=self.dag_id).ds_task_name + "." + TaskDesc.get_task(task_name=self.task_id).ds_task_name
        # receivers = get_receiver_list(receiver=self.task.emails)
        alert_model = json.loads(self.task_desc.extra_param[status])
        reciver_list = [i.get('email', '') for i in alert_model.get('emailReceivers', [])]
        user_group = self.task_desc.user_group.get('currentGroup')
        template_code = self.task_desc.template_code
        content = build_notify_content_email(state=status, task_name=self.task_desc.ds_task_name, execution_date=self.execution_date,
                                             user_group=user_group,template_code=template_code)
        send_email_by_notifyAPI(content=content.decode('utf-8').replace('\n', '<br>'),receiver=reciver_list)

    def wechat_notify_alert(self,status=None):
        from airflow.shareit.utils.alarm_adapter import build_notify_content
        from airflow.shareit.hooks.message.wechat_hook import WechatHook
        user_group = self.task_desc.user_group.get('currentGroup')
        template_code = self.task_desc.template_code
        content = build_notify_content(state=status, task_name=self.task_desc.ds_task_name, execution_date=self.execution_date,
                                       user_group=user_group,template_code=template_code)
        alert_model = json.loads(self.task_desc.extra_param[status])
        robot_key = alert_model.get('wechatRobotKey','')
        reciver_list = [i.get('wechatId','') for i in alert_model.get('wechatReceivers',[])]
        WechatHook(robot_key=robot_key).send_markdown(content,mentioned_list=reciver_list)
        pass


    def set_duration(self):
        if self.end_date and self.start_date:
            self.duration = (self.end_date - self.start_date).total_seconds()
        else:
            self.duration = None

    def xcom_push(
            self,
            key,
            value,
            execution_date=None):
        """
        Make an XCom available for tasks to pull.

        :param key: A key for the XCom
        :type key: str
        :param value: A value for the XCom. The value is pickled and stored
            in the database.
        :type value: any pickleable object
        :param execution_date: if provided, the XCom will not be visible until
            this date. This can be used, for example, to send a message to a
            task on a future date without it being immediately visible.
        :type execution_date: datetime
        """

        if execution_date and execution_date < self.execution_date:
            raise ValueError(
                'execution_date can not be in the past (current '
                'execution_date is {}; received {})'.format(
                    self.execution_date, execution_date))

        XCom.set(
            key=key,
            value=value,
            task_id=self.task_id,
            dag_id=self.dag_id,
            execution_date=execution_date or self.execution_date)

    def xcom_pull(
            self,
            task_ids=None,
            dag_id=None,
            key=XCOM_RETURN_KEY,
            include_prior_dates=False):
        """
        Pull XComs that optionally meet certain criteria.

        The default value for `key` limits the search to XComs
        that were returned by other tasks (as opposed to those that were pushed
        manually). To remove this filter, pass key=None (or any desired value).

        If a single task_id string is provided, the result is the value of the
        most recent matching XCom from that task_id. If multiple task_ids are
        provided, a tuple of matching values is returned. None is returned
        whenever no matches are found.

        :param key: A key for the XCom. If provided, only XComs with matching
            keys will be returned. The default key is 'return_value', also
            available as a constant XCOM_RETURN_KEY. This key is automatically
            given to XComs returned by tasks (as opposed to being pushed
            manually). To remove the filter, pass key=None.
        :type key: str
        :param task_ids: Only XComs from tasks with matching ids will be
            pulled. Can pass None to remove the filter.
        :type task_ids: str or iterable of strings (representing task_ids)
        :param dag_id: If provided, only pulls XComs from this DAG.
            If None (default), the DAG of the calling task is used.
        :type dag_id: str
        :param include_prior_dates: If False, only XComs from the current
            execution_date are returned. If True, XComs from previous dates
            are returned as well.
        :type include_prior_dates: bool
        """

        if dag_id is None:
            dag_id = self.dag_id

        pull_fn = functools.partial(
            XCom.get_one,
            execution_date=self.execution_date,
            key=key,
            dag_id=dag_id,
            include_prior_dates=include_prior_dates)

        if is_container(task_ids):
            return tuple(pull_fn(task_id=t) for t in task_ids)
        else:
            return pull_fn(task_id=task_ids)

    @provide_session
    def get_num_running_task_instances(self, session):
        TI = TaskInstance
        # .count() is inefficient
        return session.query(func.count()).filter(
            TI.dag_id == self.dag_id,
            TI.task_id == self.task_id,
            TI.state == State.RUNNING
        ).scalar()

    def init_run_context(self, raw=False):
        """
        Sets the log context.
        """
        self.raw = raw
        self._set_context(self)

    @staticmethod
    def update_name(old_name,new_name,transaction=False,session=None):
        task_instances = session.query(TaskInstance).filter(TaskInstance.dag_id == old_name).all()
        for t1 in task_instances:
            t1.dag_id = new_name
            if t1.task_id == old_name:
                t1.task_id = new_name
            session.merge(t1)
        if not transaction:
            session.commit()

    @staticmethod
    @provide_session
    def update_task_type(task_id,execution_date,task_type,session=None):
        task_instances = session.query(TaskInstance).filter(TaskInstance.task_id == task_id).filter(TaskInstance.execution_date == execution_date).all()
        for ti in task_instances:
            ti.task_type=task_type
            session.merge(ti)
        session.commit()

    @staticmethod
    @provide_session
    def update_task_type_restart(session=None):
        task_instances = session.query(TaskInstance)\
            .filter(TaskInstance.state.in_([State.RUNNING,State.QUEUED]))\
            .filter(TaskInstance.operator == "BashOperator")\
            .all()
        for ti in task_instances:
            dag = task_manager.get_dag(dag_id=ti.dag_id)
            task = dag.get_task(task_id=ti.task_id)
            if ti.try_number <= task.retries:
                ti.state = State.SCHEDULED
                session.merge(ti)
                session.commit()
            else:
                ti.state = State.FAILED
                session.merge(ti)
                session.commit()
                TriggerHelper.task_finish_trigger(ti)


    @staticmethod
    def update_task_type_by_dict(task_dict,execution_date,task_type):
        for task in six.itervalues(task_dict):
            TaskInstance.update_task_type(task.task_id,execution_date,task_type)

    @staticmethod
    @provide_session
    def check_is_success(task_id,execution_date,session=None):
        res = session.query(TaskInstance).filter(TaskInstance.task_id == task_id).filter(
            TaskInstance.execution_date == execution_date).all()
        if res is not None and len(res) > 0:
            if res[0].state == State.SUCCESS:
                return True
        return False

    @staticmethod
    @provide_session
    def find_by_execution_dates(dag_id=None, task_id=None, execution_dates=None, state=None, only_state=False, session=None):
        if dag_id is None and task_id is None and state is None:
            return None
        qry = session.query(TaskInstance)
        if only_state:
            qry = session.query(TaskInstance).with_entities(TaskInstance.state)
        if dag_id is not None:
            qry = qry.filter(TaskInstance.dag_id == dag_id)
        if task_id is not None:
            qry = qry.filter(TaskInstance.task_id == task_id)
        if execution_dates is not None and isinstance(execution_dates, list) and len(execution_dates) > 0:
            qry = qry.filter(TaskInstance.execution_date.in_(execution_dates))
        if state is not None:
            qry = qry.filter(TaskInstance.state == state)
        return qry.all()

    @staticmethod
    @provide_session
    def find_by_execution_date(dag_id=None, task_id=None, execution_date=None, session=None):
        qry = session.query(TaskInstance)
        if dag_id is None and task_id is None and execution_date is None :
            return None
        if dag_id is not None:
            qry = qry.filter(TaskInstance.dag_id == dag_id)
        if task_id is not None:
            qry = qry.filter(TaskInstance.task_id == task_id)
        if execution_date is not None :
            qry = qry.filter(TaskInstance.execution_date == execution_date)
        return qry.all()

    @staticmethod
    def get_qry_with_date_range(dag_id=None, task_id=None, start_date=None, end_date=None,states=None,task_name_list=None,ti_types=None,backfill_label=None,session=None):
        qry = session.query(TaskInstance)
        if dag_id or task_name_list:
            if dag_id:
                qry = qry.filter(TaskInstance.dag_id == dag_id)
            else:
                qry = qry.filter(TaskInstance.dag_id.in_(task_name_list))
        if task_id:
            qry = qry.filter(TaskInstance.task_id == task_id)
        if start_date:
            qry = qry.filter(TaskInstance.execution_date >= start_date)
        if end_date:
            qry = qry.filter(TaskInstance.execution_date <= end_date)
        if states:
            qry = qry.filter(TaskInstance.state.in_(states))
        if ti_types:
            qry = qry.filter(TaskInstance.task_type.in_(ti_types))
        if backfill_label:
            qry = qry.filter(TaskInstance.backfill_label == backfill_label)
        return qry

    @staticmethod
    @provide_session
    def find_with_date_range(dag_id=None, task_id=None, start_date=None, end_date=None, states=None,task_name_list=None, session=None,
                             ti_types=None, page=None, size=None, offset=None,backfill_label=None):
        qry = TaskInstance.get_qry_with_date_range(dag_id=dag_id, start_date=start_date, end_date=end_date, states=states,task_name_list=task_name_list,
                                                   ti_types=ti_types, backfill_label=backfill_label, session=session)
        # execution_date倒序
        qry = qry.order_by(TaskInstance.execution_date.desc())
        if size is not None and page is not None:
            return qry.limit(size).offset(offset).all()
        return qry.all()

    @staticmethod
    @provide_session
    def count_with_date_range(dag_id=None, task_id=None, start_date=None, end_date=None, states=None,
                              task_name_list=None, ti_types=None, backfill_label=None, session=None):
        qry = TaskInstance.get_qry_with_date_range(dag_id=dag_id, start_date=start_date, end_date=end_date,
                                                   states=states, task_name_list=task_name_list,
                                                   ti_types=ti_types, backfill_label=backfill_label, session=session)
        return qry.count()

    def get_time_out_seconds(self):
        res = self.get_taskinstance_durations_by_task_name(dag_id=self.dag_id, task_id=self.task_id)
        if len(res) == 3:
            durations = [task.duration for task in res]
            return max(int(sum(durations) / 3 * 1.5),30 * 60)
        else:
            return 0
    def get_owner(self):
        return TaskDesc.get_owner(task_name=self.task_id)


    @staticmethod
    @provide_session
    def get_taskinstance_durations_by_task_name(dag_id,task_id,session=None):
        qry = session.query(TaskInstance)
        if dag_id:
            qry = qry.filter(TaskInstance.dag_id == dag_id)
        if task_id:
            qry = qry.filter(TaskInstance.task_id == task_id)
        qry = qry.filter(TaskInstance.state == State.SUCCESS).filter(TaskInstance.duration != None).order_by(TaskInstance.execution_date.desc()).limit(3)
        return qry.all()

    @staticmethod
    @provide_session
    def get_all_import_sharestore_tasks(session=None):
        sql = """
        select * from task_instance where operator ="ImportSharestoreOperator" and state ='scheduled' 
        """
        res = session.execute(sql)
        session.close()
        return res.fetchall()


    @staticmethod
    @provide_session
    def find_taskinstance_dates(dag_id, end_date, session=None):
        from sqlalchemy import and_
        return session.query(TaskInstance).with_entities(TaskInstance.execution_date).filter(and_(
            TaskInstance.dag_id == dag_id,
            TaskInstance.execution_date < end_date
        )).order_by(TaskInstance.execution_date.desc()).limit(20).all()
        # sql = '''
        # select execution_date
        # from task_instance
        # where dag_id='{0}' and execution_date<'{1}'
        # '''.format(dag_id, end_date)
        # res = session.execute(sql)
        # return res.fetchall()


    def is_send_notify(self,state,notify_type):
        extra_params = self.task_desc.extra_param
        if extra_params.has_key(state):
            types = json.loads(extra_params[state])["alertType"]
            if notify_type in types:
                return True
        else:
            if notify_type == "dingTalk":
                if extra_params.has_key("dingAlert"):
                    return True
            if notify_type == "phone":
                if extra_params.has_key("phoneAlert"):
                    return True
        return False


    @staticmethod
    @provide_session
    def find_taskinstance_by_sql(session=None):
        sql='''
        select *
        from task_instance
        where task_id = 'test_python_shell'
        '''
        res = session.execute(sql)
        session.close()
        return res.fetchall()

    @staticmethod
    @provide_session
    def list_all_scheduled_task(session=None):
        sql='''
        select
            a.*
        from 
        (
            select 
                *
            from task_instance
            where state in ('queued','up_for_retry','scheduled')
            and task_id <> 'createCheckFile'
        )a  join 
        (
            select 
                dag_id
            from dag
            where is_paused = false and is_active = true
        )b 
        on a.dag_id = b.dag_id 
        '''
        res = session.execute(sql).fetchall()
        session.close()
        res_ids = []
        key_dict={}
        for r in res:
            if r['dag_id'] not in key_dict.keys():
                res_ids.append(r['dag_id'])
                key_dict[r['dag_id']] = True
        return res_ids

    @staticmethod
    @provide_session
    def list_all_scheduled_task_local_model(session=None):
        sql = '''
                select
                    a.*
                from 
                (
                    select 
                        *
                    from task_instance
                    where state in ('queued','up_for_retry','scheduled')
                    and task_id <> 'createCheckFile'
                    and operator = 'BashOperator'
                )a  join 
                (
                    select 
                        dag_id
                    from dag
                    where is_paused = false and is_active = true
                )b 
                on a.dag_id = b.dag_id 
                '''
        res = session.execute(sql).fetchall()
        session.close()
        res_ids = []
        key_dict = {}
        for r in res:
            if r['dag_id'] not in key_dict.keys():
                res_ids.append(r['dag_id'])
                key_dict[r['dag_id']] = True
        return res_ids


    @staticmethod
    @provide_session
    def clear_task_instance_list(tis,
                                 dag=None,
                                 task_type=None,
                                 target_state=State.SCHEDULED,
                                 transaction=False,
                                 external_conf=None,
                                 backfill_label=None,
                                 update_label=True,
                                 session=None,
                                 ):
        """
        Clears a set of task instances, but makes sure the running ones
        get killed.

        :param tis: a list of task instances
        :param session: current session
        :param activate_dag_runs: flag to check for active dag run
        :param dag: DAG object
        """
        from airflow.hooks.genie_hook import GenieHook
        job_ids = []
        now_date = utcnow().replace(tzinfo=timezone.beijing)
        # gh = GenieHook()
        for ti in tis:
            if task_type is not None:
                ti.task_type = task_type
            if ti.state == State.RUNNING:
                if ti.job_id:
                    job_ids.append(ti.job_id)
                '''
                    没办法在这里去kill任务,因为是事务的, 所以kill任务还是得放在operator中
                '''
                # external_conf = ti.get_external_conf()
                # if external_conf and ti.operator == "ScriptOperator":
                #     if external_conf.has_key("genie_job_id") and gh.check_job_exists(external_conf["genie_job_id"]):
                #         gh.kill_job_by_id(external_conf["genie_job_id"])

            ti.state = target_state
            if update_label:
                ti.backfill_label = backfill_label
            if target_state == State.SCHEDULED and dag:
                task = dag.get_task(ti.task_id)
                ti.max_tries = ti.try_number + task.retries - 1
            if target_state in [State.SUCCESS,State.FAILED]:
                if not ti.start_date:
                    ti.start_date = now_date
                if not ti.end_date:
                    ti.end_date = now_date
            if external_conf:
                ti.external_conf = external_conf
            session.merge(ti)
            # else:
            #     task_id = ti.task_id
            #     if dag and dag.has_task(task_id):
            #         task = dag.get_task(task_id)
            #         ti.refresh_from_task(task)
            #         task_retries = task.retries
            #         ti.max_tries = ti.try_number + task_retries - 1
            #     else:
            #         # Ignore errors when updating max_tries if dag is None or
            #         # task not found in dag since database records could be
            #         # outdated. We make max_tries the maximum value of its
            #         # original max_tries or the last attempted try number.
            #         ti.max_tries = max(ti.max_tries, ti.prev_attempted_tries)
            #     ti.state = target_state
        # if job_ids:
        #     from airflow.jobs import BaseJob as BJ
        #     for job in session.query(BJ).filter(BJ.id.in_(job_ids)).all():
        #         job.state = State.SHUTDOWN
        #         session.merge(job)
        if not transaction:
            session.commit()

    @provide_session
    def add_taskinstance_log(self,session=None):
        try:
            is_exists = TaskInstanceLog.check_is_exists(self.dag_id,self.task_id,self.execution_date,self.get_try_number(),session=session)
            if is_exists:
                return
            ti_log = TaskInstanceLog(self)
            session.merge(ti_log)
            session.commit()
        except Exception as e:
            self.log.error("add taskinstance log failed,task_id:{},execution_date:{}".format(self.task_id,self.execution_date.strftime("%Y%m%d %H:%M:%S")))
            s = traceback.format_exc()
            self.log.error(s)

    @staticmethod
    @provide_session
    def start_taskinstance(dag_id,task_id,execution_date,session=None):
        qry = session.query(TaskInstance)\
            .filter(TaskInstance.dag_id == dag_id)\
            .filter(TaskInstance.task_id == task_id)\
            .filter(TaskInstance.execution_date == execution_date)
        tis = qry.all()
        if tis is not None and len(tis) > 0:
            ti = tis[0]
            ex_conf = ti.get_external_conf()
            ex_conf = ti.format_external_conf(ex_conf)
            ti.save_external_conf(ex_conf=ex_conf)
            ti.end_date = None
            ti.start_date = timezone.utcnow()
            session.merge(ti)
            session.commit()

    def get_try_number(self):
        return self._try_number


    @staticmethod
    @provide_session
    def is_latest_runs(task_name,execution_date,session=None):
        res = session.query(TaskInstance.dag_id,
                            func.max(TaskInstance.execution_date).label('execution_date')) \
            .filter(TaskInstance.dag_id == task_name) \
            .group_by(TaskInstance.dag_id).all()
        if res:
            if DateUtil.date_equals(res[0][1],execution_date,fmt='%Y-%m-%d %H:%M:%S.%f'):
                return True
        return False

    @staticmethod
    @provide_session
    def get_actual_dag_id(task_name, execution_date, session=None):
        tis = session.query(TaskInstance).filter(TaskInstance.dag_id == task_name).filter(
            TaskInstance.task_id == task_name).filter(TaskInstance.execution_date == execution_date).all()
        if tis:
            ti = tis[0]
            ex_conf = ti.get_external_conf()
            if ex_conf and isinstance(ex_conf, dict) and 'actual_dag_id' in ex_conf.keys():
                return ex_conf['actual_dag_id'], True
        return task_name, False

    @staticmethod
    def format_external_conf(ex_conf):
        if ex_conf is None:
            return {}
        if not isinstance(ex_conf, dict):
            return ex_conf
        new_ex_conf = {}
        for k, v in ex_conf.items():
            if k not in ['actual_dag_id','callback_url','callback_id','extra_info','callback_info','uuid']:
                continue
            # if k in ['genie_job_id', 'version','is_kyuubi','batch_id']:
            #     continue
            new_ex_conf[k] = v
        return new_ex_conf
    
    @staticmethod
    @provide_session
    def find_running_ti(session=None):
        tis = session.query(TaskInstance).filter(TaskInstance.state == State.RUNNING).all()
        return tis

    @staticmethod
    @provide_session
    def find_queued_ti(session=None):
        tis = session.query(TaskInstance).filter(TaskInstance.state == State.QUEUED).all()
        return tis

    @staticmethod
    @provide_session
    def find_scheduled_ti(session=None):
        qry = session.query(TaskInstance).filter(TaskInstance.state == State.SCHEDULED)
        return qry.all()

    @staticmethod
    @provide_session
    def find_running_ti_by_task(dag_id=None,session=None):
        tis = session.query(TaskInstance).filter(TaskInstance.dag_id == dag_id, TaskInstance.state == State.RUNNING).all()
        return tis


    @staticmethod
    @provide_session
    def recovery_ti_state(dag_id,task_id,execution_date,state,duration,end_date,session=None):
        tis = session.query(TaskInstance).filter(TaskInstance.dag_id == dag_id,TaskInstance.task_id == task_id,TaskInstance.execution_date == execution_date).all()
        if tis and len(tis) > 0 :
            ti = tis[0]
            ti.state = state
            ti.duration = duration
            ti.end_date = end_date
            session.merge(ti)
            session.commit()

    def get_duration(self):
        if self.start_date is None:
            return 0
        end_date = self.end_date if self.end_date is not None else utcnow()
        return (end_date.replace(tzinfo=beijing) - self.start_date.replace(tzinfo=beijing)).total_seconds()

    def get_genie_info(self):
        genie_job_id = ''
        genie_job_url = ''
        ex_conf = self.get_external_conf()
        if ex_conf:
            if 'batch_id' in ex_conf.keys():
                try:
                    genie_job_id = ex_conf['batch_id']
                    app_url = ex_conf.get('app_url','')
                    # if ex_conf.get('is_spark',False) and app_url:
                    #     genie_job_url = app_url
                    # else :
                        # log_web_host = conf.get("job_log", "log_web_host")
                    log_cloud_home = conf.get('job_log', 'log_cloud_file_location').strip()
                    genie_job_url = '{host}/BDP/logs/pipeline_log/{time}/{batch_id}.txt'.format(
                        host=log_cloud_home,
                        # dag_id=self.dag_id,
                        time=self.start_date.strftime('%Y%m%d'),
                        batch_id=genie_job_id
                    )
                except Exception:
                    pass
        return genie_job_id, genie_job_url

    def get_version(self):
        ex_conf = self.get_external_conf()
        if ex_conf:
            try:
                for k in ex_conf:
                    if k == "version":
                        return ex_conf["version"]
            except Exception:
                pass
        return -1

    def is_kyuubi_job(self):
        ex_conf = self.get_external_conf()
        if ex_conf:
            if 'is_kyuubi' in ex_conf.keys():
                return True
        return False

    @staticmethod
    def state_merge(tis):
        if not tis or len(tis) == 0:
            return None
        if len(tis) == 1:
            return tis[0].state
        for ti in tis :
            if ti.state in [State.FAILED,State.TASK_UP_FOR_RETRY,State.RUNNING,State.QUEUED,State.SCHEDULED]:
                return ti.state

        return State.SUCCESS

    def get_spark_webui(self,url_dict):
        if not url_dict:
            return ''
        ex_conf = self.get_external_conf()
        if ex_conf:
            if 'app_id' in ex_conf.keys() and 'app_url' in ex_conf.keys():
                if self.state == State.RUNNING:
                    # return ex_conf['app_url']
                    pass
                else:
                    region = ex_conf['region']
                    if region in url_dict.keys():
                        return '{}/{}/jobs'.format(url_dict[region],ex_conf['app_id'])
                    else:
                        return ex_conf['app_url']
        return ''
