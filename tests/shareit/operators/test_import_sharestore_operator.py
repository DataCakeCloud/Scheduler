import datetime
import unittest

import pendulum

from airflow.hooks.genie_hook import GenieHook
from airflow.models import TaskInstance
from airflow.shareit.hooks.kyuubi_hook import KyuubiHook
from airflow.shareit.jobs.alert_job import AlertJob, callback_processor
from airflow.shareit.operators.kyuubi_operator import KyuubiOperator
from airflow.shareit.utils.task_manager import get_dag
from airflow.utils import timezone
from airflow.utils.timezone import beijing


class ImportSharestoreOperatorTest(unittest.TestCase):

    def test_import_sharestore_op(self):
        dag = get_dag('1_15366')
        tasks= dag.tasks
        task = tasks[1]
        execution_date = pendulum.parse('2023-03-07 06:00:00')
        dag_id = '1_15366'
        tis = TaskInstance.find_by_execution_dates(dag_id=dag_id,execution_dates=[execution_date])
        ti = tis[1]
        context = {"ti": ti}
        task.execute(context)
