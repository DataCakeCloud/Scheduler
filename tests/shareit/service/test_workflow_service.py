# -*- coding: utf-8 -*-
"""
Author: Tao Zhang
Date: 2022/6/21
If you are doing your best,you will not have to worry about failure.
"""
import json
import re
import time

import unittest
from datetime import datetime, timedelta

import pendulum
from airflow.utils import timezone

from airflow.shareit.service.task_service import TaskService
from airflow.shareit.service.workflow_chart_service import WorkflowChartService
from airflow.shareit.trigger.trigger_helper import TriggerHelper
from airflow.utils.timezone import beijing
from airflow.utils.db import provide_session
from airflow.models.dag import DagModel
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.shareit.models.dag_dataset_relation import DagDatasetRelation
from airflow.shareit.models.trigger import Trigger
from airflow.shareit.models.task_desc import TaskDesc
from airflow.shareit.service.workflow_service import WorkflowService

test_tasks = [{
    "task_code": {"depend_types": "", "trigger_param": {"type": "data", "output_granularity": "hourly", }, "retries": 2,
                  "max_active_runs": 15, "email_on_retry": False, "end_date": "", "input_datasets": [
            {"detailed_gra": "",
             "check_path": "s3://shareit.deploy.us-east-1/BDP/BDP-spark-base-image/spark-2.4.3-base-v1.tar.gz",
             "offset": -1, "granularity": "hourly", "guid": "", "id": "test.zhangtao_test_wftask2_output@ue1",
             "metadata": {"source": "", "region": "ue1", "db": "test", "type": "hive", "table": "zhangtao_test_001"}}],
                  "start_date": "", "owner": "zhangtao", "output_datasets": [
            {"guid": "", "id": "test.zhangtao_test_wftask3_output@ue1", "offset": -1,
             "metadata": {"source": "", "region": "ue1", "db": "test", "type": "hive", "table": "zhangtao_test_002"}}],
                  "email_on_failure": False, "task_items": [
            {"cluster_tags": "type:k8s,region:ue1,rbac.cluster:bdp-test,sla:normal", "task_type": "GenieJobOperator",
             "command": "--deploy-mode cluster --conf spark.app.name=zhangtao_test_task1 --conf spark.kubernetes.driver.label.owner=zhangtao --conf spark.kubernetes.executor.label.owner=zhangtao --conf spark.kubernetes.driver.label.entry=DataStudio --conf spark.kubernetes.executor.label.entry=DataStudio -e \"/*zheshiyigebianmabiaoshi*/c2hvdyUyMGRhdGFiYXNlcw==/*zheshiyigebianmabiaoshi*/\"",
             "command_tags": "type:spark-submit-sql-ds"}], "extra_params": {}, "email_on_success": False,
                  "filterField": {"excludes": ["execution_timeout"], "maxLevel": 0, "includes": []},
                  "emails": "zhangtao@ushareit.com", "name": "zhangtao_test_wftask3"}
},
    {
        "task_code": {"depend_types": "", "trigger_param": {"type": "data", "output_granularity": "hourly", },
                      "retries": 2, "max_active_runs": 15, "email_on_retry": False, "end_date": "", "input_datasets": [
                {"detailed_gra": "",
                 "check_path": "s3://shareit.deploy.us-east-1/BDP/BDP-spark-base-image/spark-2.4.3-base-v1.tar.gz",
                 "offset": -1, "granularity": "hourly", "guid": "", "id": "test.zhangtao_test_wftask1_output@ue1",
                 "metadata": {"source": "", "region": "ue1", "db": "test", "type": "hive",
                              "table": "zhangtao_test_001"}}],
                      "start_date": "", "owner": "zhangtao", "output_datasets": [
                {"guid": "", "id": "test.zhangtao_test_wftask2_output@ue1", "offset": -1,
                 "metadata": {"source": "", "region": "ue1", "db": "test", "type": "hive",
                              "table": "zhangtao_test_002"}}], "email_on_failure": False, "task_items": [
                {"cluster_tags": "type:k8s,region:ue1,rbac.cluster:bdp-test,sla:normal",
                 "task_type": "GenieJobOperator",
                 "command": "--deploy-mode cluster --conf spark.app.name=zhangtao_test_task1 --conf spark.kubernetes.driver.label.owner=zhangtao --conf spark.kubernetes.executor.label.owner=zhangtao --conf spark.kubernetes.driver.label.entry=DataStudio --conf spark.kubernetes.executor.label.entry=DataStudio -e \"/*zheshiyigebianmabiaoshi*/c2hvdyUyMGRhdGFiYXNlcw==/*zheshiyigebianmabiaoshi*/\"",
                 "command_tags": "type:spark-submit-sql-ds"}], "extra_params": {}, "email_on_success": False,
                      "filterField": {"excludes": ["execution_timeout"], "maxLevel": 0, "includes": []},
                      "emails": "zhangtao@ushareit.com", "name": "zhangtao_test_wftask2"}
    },
    {
        "task_code": {"depend_types": "", "trigger_param": {"type": "data", "output_granularity": "hourly", },
                      "retries": 2, "max_active_runs": 15, "email_on_retry": False, "end_date": "", "input_datasets": [
                {"detailed_gra": "",
                 "check_path": "s3://shareit.deploy.us-east-1/BDP/BDP-spark-base-image/spark-2.4.3-base-v1.tar.gz",
                 "offset": -1, "granularity": "hourly", "guid": "", "id": "test.zhangtao_test_wftask1_input@ue1",
                 "metadata": {"source": "", "region": "ue1", "db": "test", "type": "hive",
                              "table": "zhangtao_test_001"}}],
                      "start_date": "", "owner": "zhangtao", "output_datasets": [
                {"guid": "", "id": "test.zhangtao_test_wftask1_output@ue1", "offset": -1,
                 "metadata": {"source": "", "region": "ue1", "db": "test", "type": "hive",
                              "table": "zhangtao_test_002"}}], "email_on_failure": False, "task_items": [
                {"cluster_tags": "type:k8s,region:ue1,rbac.cluster:bdp-test,sla:normal",
                 "task_type": "GenieJobOperator",
                 "command": "--deploy-mode cluster --conf spark.app.name=zhangtao_test_task1 --conf spark.kubernetes.driver.label.owner=zhangtao --conf spark.kubernetes.executor.label.owner=zhangtao --conf spark.kubernetes.driver.label.entry=DataStudio --conf spark.kubernetes.executor.label.entry=DataStudio -e \"/*zheshiyigebianmabiaoshi*/c2hvdyUyMGRhdGFiYXNlcw==/*zheshiyigebianmabiaoshi*/\"",
                 "command_tags": "type:spark-submit-sql-ds"}], "extra_params": {}, "email_on_success": False,
                      "filterField": {"excludes": ["execution_timeout"], "maxLevel": 0, "includes": []},
                      "emails": "zhangtao@ushareit.com", "name": "zhangtao_test_wftask1"}
    }]




class WorkflowServiceTest(unittest.TestCase):

    def test_lineagechart(self):
       execution_date = pendulum.parse('2023/01/09 05:00').replace(tzinfo=timezone.beijing)
       # execution_date = None
       tenant_id = 21
       data= WorkflowChartService().get_lineagechart(135,execution_date,tenant_id=tenant_id)
       print json.dumps(data)

    def test_get_instance_latest(self):
        workflow_id_list=[105]
        data = WorkflowService.list_workflow_taskinstance_latest(workflow_id_list=workflow_id_list, num=7)
        print json.dumps(data)

    def test_get_nextdate_list(self):
        workflow_id_list = [111]
        data = WorkflowService.list_workflow_next_execution_date(workflow_id_list=workflow_id_list)
        print json.dumps(data)
        time.sleep()

    def test_draw(self):
        tasks =[{"taskId":14391},{"taskId":14692},{"taskId":14527}]
        data = WorkflowChartService.draw(tasks)
        print json.dumps(data)

    def test_linechart(self):
        # start_date = pendulum.parse('2022-12-01 00:00:00').replace(tzinfo=timezone.beijing)
        # end_date = pendulum.parse('2022-12-02 23:59:59').replace(tzinfo=timezone.beijing)
        start_date=None
        end_date=None
        workflow_id = 22
        version = 1
        data = WorkflowChartService.get_linechart(workflow_id=workflow_id,version=version,start_date=start_date,end_date=end_date,tenant_id=32)
        print json.dumps(data)

    def test_list_instance_date(self):
        workflow_id = 135
        pagesize = 15
        page_num = 1
        tenant_id = 21
        data = WorkflowService.list_instance_date(workflow_id,pagesize,page_num,tenant_id=tenant_id)
        print data

    def test_grid(self):
        # start_date = pendulum.parse('2022-12-07 00:00:00').replace(tzinfo=timezone.beijing)
        # end_date = pendulum.parse('2022-12-07 23:59:59').replace(tzinfo=timezone.beijing)
        start_date=None
        end_date=None
        workflow_id = 10006
        tenant_id = 1
        data = WorkflowChartService.get_gridchart(workflow_id,1,start_date=start_date,end_date=end_date,tenant_id=tenant_id)
        print json.dumps(data)


    def test_ganttchart(self):
        execution_date = pendulum.parse('2023-02-14 02:06:00').replace(tzinfo=timezone.beijing)
        # execution_date=None
        workflow_id = 10006
        tenant_id = 1
        data = WorkflowChartService.get_ganttchart(workflow_id,execution_date=execution_date,tenant_id=tenant_id)
        print json.dumps(data)

    def test_get_baseinfo(self):
        execution_date = pendulum.parse('2022-12-02 16:30:00').replace(tzinfo=timezone.beijing)
        ds_task_id = 17086
        task_name = 'task12111545'
        workflow_id = 65
        data = WorkflowService.get_taskinstance_baseinfo(workflow_id=workflow_id,task_name=task_name,ds_task_id=ds_task_id,execution_date=execution_date)
        print json.dumps(data)

    def test_list_instance(self):
        workflow_id = 9
        size = 15
        num= 1
        start_date = pendulum.parse('2022-12-02 00:00:00').replace(tzinfo=timezone.beijing)
        end_date = pendulum.parse('2022-12-02 23:59:59').replace(tzinfo=timezone.beijing)
        data = WorkflowService.list_instance(workflow_id,size,num,start_date,end_date)
        print json.dumps(data)

    def test_batch_clear(self):
        workflow_id = 14
        execution_dates = [pendulum.parse('2022-12-06 00:00:00').replace(tzinfo=timezone.beijing)]
        task_info = {'taskId':1,'taskName':'shareit_workflow_n2_task1'}
        WorkflowService.batch_clear(workflow_id,execution_dates,json.dumps([task_info]) )
        # a = pendulum.parse('2022-10-23 00:00:00')
        # print a + timedelta(days=30)

    def test_backfill(self):
        workflow_id = 14
        start_date = pendulum.parse('2022-11-30 00:00:00').replace(tzinfo=timezone.beijing)
        end_date = pendulum.parse('2022-11-30 23:00:00').replace(tzinfo=timezone.beijing)
        WorkflowService.batch_backfill(workflow_id,start_date,end_date,True)
        if '':
            print 111

    def test_get_taskinstance_baseinfo(self):
        workflow_id = 14
        execution_date = pendulum.parse('2022-12-08 16:25:00').replace(tzinfo=timezone.beijing)
        task_id = 16941
        task_name = 'task12081615'
        data = WorkflowService.get_taskinstance_baseinfo(workflow_id,execution_date,task_name,task_id)

        print json.dumps(data)