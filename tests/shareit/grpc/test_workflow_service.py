# -*- coding: utf-8 -*-
import base64
import json
import unittest

import pytest
from google.protobuf import json_format

from airflow.shareit.grpc.grpcRun import GrpcRun
from airflow.shareit.grpc.service.workflow_grpc_service import WorkflowGrpcService
from airflow.shareit.jobs.alert_job import AlertJob, callback_processor
from airflow.shareit.grpc.TaskServiceApi.WorkflowServiceApi_pb2_grpc import WorkflowRpcServiceStub,add_WorkflowRpcServiceServicer_to_server,WorkflowServiceApi__pb2
from airflow.shareit.grpc.TaskServiceApi.entity import ScheduleJob_pb2, ScheduleJob_pb2_grpc, UserInfo_pb2

'''
    这里用的pytest-grpc包,https://pypi.org/project/pytest-grpc/
'''
@pytest.fixture(scope='module')
def grpc_add_to_server():
    return add_WorkflowRpcServiceServicer_to_server


@pytest.fixture(scope='module')
def grpc_servicer():
    return WorkflowGrpcService()


@pytest.fixture(scope='module')
def grpc_stub_cls(grpc_channel):
    return WorkflowRpcServiceStub


class TestWorkflowGrpcService:

    def get_task_code_grpc(self,task_name,ds_task_id):
        # date_calculation_param = ScheduleJob_pb2.DataCaculationParam()
        date_calculation_param = ScheduleJob_pb2.DataCaculationParam()
        task_items = json.dumps([{
            'cluster_tags':'type:k8s,region:ue1,rbac.cluster:bdp-test,sla:normal',
            'task_type': 'GenieJobOperator',
            'command': '--deploy-mode cluster --conf spark.kubernetes.driverEnv.HADOOP_USER_NAME=wuhaorui --conf spark.app.name=depend_replace_day --conf spark.kubernetes.driver.label.owner=wuhaorui --conf spark.kubernetes.executor.label.owner=wuhaorui --conf spark.kubernetes.driver.label.entry=DataStudio --conf spark.kubernetes.executor.label.entry=DataStudio -e \"/*zheshiyigebianmabiaoshi*/c2VsZWN0JTIwMQ==/*zheshiyigebianmabiaoshi*/\"',
            'command_tags': 'type:spark-submit-sql-ds'
        }])
        # task_items = ScheduleJob_pb2.TaskItems(
        #     map={
        #         'cluster_tags':'type:k8s,region:ue1,rbac.cluster:bdp-test,sla:normal',
        #         'task_type':'GenieJobOperator',
        #         'command':'--deploy-mode cluster --conf spark.kubernetes.driverEnv.HADOOP_USER_NAME=wuhaorui --conf spark.app.name=depend_replace_day --conf spark.kubernetes.driver.label.owner=wuhaorui --conf spark.kubernetes.executor.label.owner=wuhaorui --conf spark.kubernetes.driver.label.entry=DataStudio --conf spark.kubernetes.executor.label.entry=DataStudio -e \"/*zheshiyigebianmabiaoshi*/c2VsZWN0JTIwMQ==/*zheshiyigebianmabiaoshi*/\"',
        #         'command_tags':'type:spark-submit-sql-ds'
        #     }
        # )
        # event_depend = ScheduleJob_pb2.EventDepend()
        event_depend = ScheduleJob_pb2.EventDepend(
            taskId=str(ds_task_id),
            dependId='mysql2hive_task01_copy',
            granularity='daily',
            useDateCalcuParam=False,
            unitOffset='0'
        )
        input_dataset = ScheduleJob_pb2.Dataset(
            useDateCalcuParam=False,
            dataCaculationParam=date_calculation_param,
            checkPath='',
            id='tet.depend_replace_day@ue1',
            granularity='daily',
            offset=0,
            unitOffset='-1',
            readyTime='0 0 * * *'
        )
        output_dataset = ScheduleJob_pb2.Dataset(
            id='abc.depend_replace_day121@ue1',
            offset=-29,
            fileName='_SUCCESS',
            location="s3://sprs-qz-dw-prod-ue1/datastudio/check_signal/depend_replace_day/{{execution_date.strftime('%Y%m%d_%H')}}",
            granularity='hourly',
        )
        triggerParam = ScheduleJob_pb2.TriggerParam(
            type='dataset',ouputGranularity='daily',crontab='0 0 * * *'
        )
        task_code = ScheduleJob_pb2.TaskCode(
            name=task_name, emails='', owner='', retries=0, maxActiveRuns=1, emailOnSuccess=False,
            emailOnFailure=False, emailOnStart=False,
            startDate=None, endDate=None, executionTimeout=1000, extraParam='', dependTypes='dataset,event', version=12,
            inputDatasets=[input_dataset], outputDatasets=[output_dataset], eventDepend=[event_depend],
            taskItems=task_items,
            triggerParam=triggerParam
        )
        schedule_job = ScheduleJob_pb2.ScheduleJob(
            taskId=ds_task_id,taskName=task_name,taskCode=task_code
        )
        return schedule_job

    def test_offline_workflow(self,grpc_stub):
        request = WorkflowServiceApi__pb2.OfflineRequest(workflowId=80)
        response = grpc_stub.offline(request)
        print response

    def test_delete_workflow(self,grpc_stub):
        userinfo = UserInfo_pb2.UserInfo(tenantId=1)
        res = json_format.MessageToJson(userinfo)
        res2 = base64.b64encode(res)
        request = WorkflowServiceApi__pb2.DeleteWorkflowRequest(workflowId=80)
        response = grpc_stub.deleteWorkflow(request,metadata=[('current_login_user',res2)])
        print response

    def test_online_workflow(self,grpc_stub):
        schedule_job = self.get_task_code_grpc('workflow_task2',13001)
        jobs = []
        jobs.append(schedule_job)
        # a = json_format.MessageToDict(task_code,including_default_value_fields=True)
        # print json.dumps(a)
        request = WorkflowServiceApi__pb2.OnlineRequest(
            workflowId=111,version=1,workflowName='workflow_first',granularity='daily',crontab='0 0 * * *',jobs=jobs
        )
        # task_items = schedule_job.taskCode.taskItems
        # task_items_dict = json.loads(task_items)
        # schedule_job_json = json_format.MessageToDict(schedule_job)
        # (schedule_job_json['taskCode'])['task_items'] = task_items_dict
        # print json.dumps(schedule_job_json)
        resonse = grpc_stub.online(request)
        print json.dumps(json_format.MessageToDict(resonse))

    def test_dataset_to_json(self,grpc_stub):
        output_dataset = ScheduleJob_pb2.Dataset(
            id='abc.depend_replace_day@ue1',
            offset=-29,
            fileName='_SUCCESS',
            location="s3://sprs-qz-dw-prod-ue1/datastudio/check_signal/depend_replace_day/{{execution_date.strftime('%Y%m%d_%H')}}",
            granularity='hourly',
        )

        output_dataset1 = ScheduleJob_pb2.Dataset(
        )

        a= json_format.MessageToDict(output_dataset1)
        print json.dumps(a)


    def test_debug(self,grpc_stub):
        schedule_job = self.get_task_code_grpc('test_debug_task', 13054)
        jobs = []
        jobs.append(schedule_job)
        use_name = 'wuhaorui'
        chat_id = '5cd6b8d3-b48e-ef16-77e9-18bd2ebc9322'
        request = WorkflowServiceApi__pb2.DebugTaskRequest(
            userName=use_name,chatId=chat_id,scheduleJob=jobs
        )
        resp = grpc_stub.debugTask(request)
        print json.dumps(json_format.MessageToDict(resp))

    def test_stop(self,grpc_stub):
        use_name = 'wuhaorui'
        chat_id = '5cd6b8d3-b48e-ef16-77e9-18bd2ebc9322'
        task_id = [13054]
        request = WorkflowServiceApi__pb2.StopTaskRequest(
            userName=use_name, chatId=chat_id, taskId=task_id
        )
        resp = grpc_stub.stopTask(request)
        print json.dumps(json_format.MessageToDict(resp))

    def test_json_to_message(self,grpc_stub):
        task_items = ScheduleJob_pb2.TaskItems(
            map={
                'cluster_tags':'type:k8s,region:ue1,rbac.cluster:bdp-test,sla:normal',
                'task_type':'GenieJobOperator',
                'command':'--deploy-mode cluster --conf spark.kubernetes.driverEnv.HADOOP_USER_NAME=wuhaorui --conf spark.app.name=depend_replace_day --conf spark.kubernetes.driver.label.owner=wuhaorui --conf spark.kubernetes.executor.label.owner=wuhaorui --conf spark.kubernetes.driver.label.entry=DataStudio --conf spark.kubernetes.executor.label.entry=DataStudio -e \"/*zheshiyigebianmabiaoshi*/c2VsZWN0JTIwMQ==/*zheshiyigebianmabiaoshi*/\"',
                'command_tags':'type:spark-submit-sql-ds'
            }
        )
        task_code = ScheduleJob_pb2.TaskCode(
            taskItems=[task_items]
        )

        datec = ScheduleJob_pb2.DataCaculationParam(
            month = ScheduleJob_pb2.DataCaculationParam.DateUnitParam(type='offset',unitOffset='0')
        )

        jsons = json_format.MessageToDict(datec, including_default_value_fields=True)
        print json.dumps(jsons)
        # a={}
        # jsons = json_format.MessageToDict(task_items, including_default_value_fields=True)
        # print json.dumps(jsons)
        # maps = jsons['map']
        # a['a'] = [maps]
        # print json.dumps(a)
