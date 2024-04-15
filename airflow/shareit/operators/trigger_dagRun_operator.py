# -*- coding: utf-8 -*-
"""
Author: zhangtao
CreateDate: 2020/11/24
"""
import json
from datetime import datetime

from airflow.api.common.experimental.trigger_dag import trigger_dag
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import pendulum


class DagRunOrder(object):
    def __init__(self, run_id=None, payload=None):
        self.run_id = run_id
        self.payload = payload


class NewTriggerDagRunOperator(BaseOperator):
    """
    Triggers a DAG run for a specified ``dag_id``

    :param trigger_dag_id: the dag_id to trigger
    :type trigger_dag_id: str
    :param python_callable: a reference to a python function that will be
        called while passing it the ``context`` object and a placeholder
        object ``obj`` for your callable to fill and return if you want
        a DagRun created. This ``obj`` object contains a ``run_id`` and
        ``payload`` attribute that you can modify in your function.
        The ``run_id`` should be a unique identifier for that DAG run, and
        the payload has to be a picklable object that will be made available
        to your tasks while executing that DAG run. Your function header
        should look like ``def foo(context, dag_run_obj):``
    :type python_callable: python callable
    :param execution_date: Execution date for the dag
    :type execution_date: datetime.datetime
    """
    template_fields = ('execution_date',)
    ui_color = '#ffefeb'

    @apply_defaults
    def __init__(
            self,
            trigger_dag_id,
            python_callable=None,
            execution_date=None,
            *args, **kwargs):
        super(NewTriggerDagRunOperator, self).__init__(*args, **kwargs)
        self.python_callable = python_callable
        self.trigger_dag_id = trigger_dag_id
        self.execution_date = execution_date

    def trans_to_full_datetime(self):
        execution_date = self.execution_date
        print "execution_date: ", execution_date
        # execution_date: 2019-01-08T16:30:00+00:00
        date_arr = execution_date.split("T")[0].split("-")
        time_arr = execution_date.split("T")[1].split("+")[0].split(":")
        year = int(date_arr[0])
        month = int(date_arr[1])
        day = int(date_arr[2])
        hour = int(time_arr[0])
        minute = int(time_arr[1])
        second = int(time_arr[2])
        utc = pendulum.timezone('UTC')
        execution_date = datetime(year, month, day, hour, minute, second, tzinfo=utc)
        return execution_date

    def execute(self, context):
        execution_date = self.trans_to_full_datetime()
        dro = DagRunOrder(run_id='trig__' + execution_date.isoformat())
        if self.python_callable is not None:
            dro = self.python_callable(context, dro)
        if dro:
            trigger_dag(dag_id=self.trigger_dag_id,
                        run_id=dro.run_id,
                        conf=json.dumps(dro.payload),
                        execution_date=execution_date,
                        replace_microseconds=False)
        else:
            self.log.info("Criteria not met, moving on")
