# -*- coding: utf-8 -*-
"""
Author: Tao Zhang
Date: 2022/2/23
If you are doing your best,you will not have to worry about failure.
"""
import unittest
from datetime import datetime

from airflow.models import DagRun
from airflow.shareit.models.task_desc import TaskDesc
from airflow.utils import timezone


class TaskDescTest(unittest.TestCase):

    def test_get_all_task_name(self):
        # names = TaskDesc.get_all_task_name()
        result = TaskDesc.get_task_with_tenant(ds_task_name='ecom_dpa_id_shopee_gaid_cvr_label_daily')
        print result

    def test_update_run(self):
        dt=datetime(year=2022,month=7,day=19).replace(tzinfo=timezone.beijing)
        DagRun.update_run_id(dag_id='test_clear_and_kill_task',execution_date=dt,run_id='1111')

    def test_get_all_cron_task(self):
         res = TaskDesc.get_all_cron_task()
         print len(res)

    def test_get_tenant_id(self):
        print TaskDesc.get_tenant_id(task_name='1_23438')

    def test_get_cluster_manage_info(self):
        tenant_id,region,provider = TaskDesc.get_cluster_manager_info(task_name='1_23604')
        print tenant_id,region,provider