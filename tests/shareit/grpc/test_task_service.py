# -*- coding: utf-8 -*-
import base64
import json
import time

import pytest
import base64
from google.protobuf import json_format

from airflow.shareit.grpc.TaskServiceApi import TaskSchedulerApi_pb2
from airflow.shareit.grpc.TaskServiceApi.TaskSchedulerApi_pb2_grpc import add_TaskSchedulerRpcApiServicer_to_server, \
    TaskSchedulerRpcApiStub
from airflow.shareit.grpc.TaskServiceApi.entity import UserInfo_pb2, UserInfo_pb2_grpc, ScheduleJob_pb2
from airflow.shareit.grpc.service.task_grpc_service import TaskServiceGrpcService


@pytest.fixture(scope='module')
def grpc_add_to_server():
    return add_TaskSchedulerRpcApiServicer_to_server


@pytest.fixture(scope='module')
def grpc_servicer():
    return TaskServiceGrpcService()


@pytest.fixture(scope='module')
def grpc_stub_cls(grpc_channel):
    return TaskSchedulerRpcApiStub


class TestTaskServiceGrpc:

    def test_grpc_task_render_info(self, grpc_stub):
        """
        原接口：/render
        :return: 获取render信息
        """
        execution_date = "2022-12-30 00:00:00.000"
        content = """s3://shareit.ads.us-east-1/dw/table/dws/dws_ads_direct_income_day/dt={{ (execution_date + macros.timedelta(hours=-24)).strftime("%Y%m%d") }}"""
        request = TaskSchedulerApi_pb2.RenderRequest(executionDate=execution_date, content=content)
        response = grpc_stub.getTaskRenderInfo(request)
        print(response)

    def test_grpc_date_transform(self, grpc_stub):
        """
        :原接口：/task/date/transform
        :return: 对airflow的日期做转换
        """
        is_sql = True
        airflow_crontab = "0 0 0 0 0"
        task_gra = "daily"
        airflow_content = "{{yesterday_ds}}"
        is_data_trigger = True
        request = TaskSchedulerApi_pb2.DateTransformRequest(isDataTrigger=is_data_trigger,
                                                            airflowCrontab=airflow_crontab,
                                                            taskGra=task_gra, airflowContent=airflow_content,
                                                            isSql=is_sql)
        response = grpc_stub.dateTransform(request)
        print(response.data)

    def test_grpc_dataset_info(self, grpc_stub):
        """
        :原接口：/dataset/info
        :return: 获取数据集信息
        """
        name = 'lhy.lhy@ue1'
        request = TaskSchedulerApi_pb2.DataInfoRequest(name=name)
        response = grpc_stub.datasetInfo(request)
        print(response)

    def test_grpc_date_preview(self, grpc_stub):
        """
        :原接口：/task/date/preview
        :return: 日期预览
        """
        task_gra = 'daily'
        user_info = ''
        task_depend = ''
        data_depend = '''{
            "check_path": "s3://[[%Y%m%d]]",
            "dateCalculationParam": {},
            "granularity": "daily",
            "guid": "",
            "id": "dasdsa.asdsad@ue1",
            "metadata": {
                "db": "dasdsa",
                "region": "ue1",
                "source": "",
                "table": "asdsad",
                "type": "hive"
            },
            "offset": 0,
            "ready_time": "0 0 * * *",
            "unitOffset": "-1~0",
            "use_date_calcu_param": false,
            "sourceTableList": [],
            "isExternal": 1,
            "granularityList": [
                "hourly",
                "daily",
                "weekly",
                "monthly"
            ],
            "checkPathList": []
        }'''
        task_crontab = '00 00 * * *'

        request = TaskSchedulerApi_pb2.DatePreviewRequest(taskGra=task_gra, dataDepend=data_depend,
                                                          taskDepend=task_depend,
                                                          taskCrontab=task_crontab)
        response = grpc_stub.datePreview(request)
        print(response)

    def test_grpc_set_success(self, grpc_stub):
        """
        :原接口：/success
        :return: 设置success
        """
        name = 'gateway-test22'
        execution_date = '2022/12/13 00:00:00.000'
        user_info = UserInfo_pb2.UserInfo(userName='tianxu')
        import base64
        user = json_format.MessageToJson(user_info)
        user = base64.b64encode(user)
        request = TaskSchedulerApi_pb2.StateSetRequest(name=name, executionDate=execution_date)
        response = grpc_stub.setSuccess(request, metadata=[('current_login_user', user)])
        print(response)

    def test_grpc_set_fail(self, grpc_stub):
        """
        :原接口：/failed
        :return: 设置fail
        """
        name = 'gateway-test22'
        execution_date = '2022/12/13 00:00:00.000'
        request = TaskSchedulerApi_pb2.StateSetRequest(name=name, executionDate=execution_date)
        response = grpc_stub.setFail(request)
        print(response)

    def test_grpc_clear(self, grpc_stub):
        """
        :原接口：/clear
        :return: 任务重跑（离线）
        """
        name = 'gateway-test22'
        user_info = ''
        execution_date = '2022/12/11 00:00:00.000'
        request = TaskSchedulerApi_pb2.ClearTaskRequest(taskName=name, executionDate=execution_date)
        response = grpc_stub.clear(request)
        print(response)

    def test_grpc_getDagRunLatest(self, grpc_stub):
        """
        :原接口：/dagruns/laststatus
        :return: 获取任务实例历史信息
        """
        names = 'test_fix_sensor'
        user_info = ''
        request = TaskSchedulerApi_pb2.DagRunLatestRequest(names=names)
        response = grpc_stub.getDagRunLatest(request)
        print(response)

    def test_grpc_getTaskDiagnose(self, grpc_stub):
        """
        :原接口：/task/diagnose
        :return: 运维页-检查上游
        """
        task_id = 'diagnose_daily'
        state = 'success'
        user_info = ''
        execution_date = '2022/11/07 00:00:00.000'
        request = TaskSchedulerApi_pb2.DiagnoseParamRequest(name=task_id, state=state, executionDate=execution_date)
        response = grpc_stub.getTaskDiagnose(request)
        print(response)

    def test_grpc_taskDelete(self, grpc_stub):
        """
        :原接口：/task/delete
        :return: 删除任务
        """
        name = 'diagnose_daily_03_2'
        user_info = ''
        request = TaskSchedulerApi_pb2.DeleteTaskRequest(name=name)
        response = grpc_stub.taskDelete(request)
        print(response)

    def test_grpc_task_update_name(self, grpc_stub):
        """
        :原接口：/task/updatename
        :return: 更新任务名称
        """
        old_name = 'diagnose_daily_03_22'
        new_name = 'diagnose_daily_03_33'
        user_info = ''
        request = TaskSchedulerApi_pb2.UpdateTaskNameRequest(oldName=old_name, newName=new_name)
        response = grpc_stub.taskUpdatename(request)
        print(response)

    def test_grpc_PageListInfo(self, grpc_stub):
        """
        :原接口：/dagruns/latest
        :return: 运维页实例
        """
        userinfo = UserInfo_pb2.UserInfo(tenantId=1)
        res = json_format.MessageToJson(userinfo)
        res2 = base64.b64encode(res)
        name = 'test_pyspark'
        page = 1
        size = 30
        # state = 'success'
        # start_date = '2022-12-16 00:00:00'
        # end_date = '2022-12-24 23:59:59'
        state = ''
        start_date = ''
        end_date = ''
        user_info = UserInfo_pb2.UserInfo(userName='tianxu')
        user = json_format.MessageToJson(user_info)
        user = base64.b64encode(user)
        start = time.time()
        request = TaskSchedulerApi_pb2.PageListInfoRequest(name=name, page=page, size=size, state=state, startDate=start_date, endDate=end_date)
        response = grpc_stub.PageListInfo(request)
        end = time.time()
        interval = end-start
        print('interval=', interval)
        print response.data

    def test_grpc_getLogUrl(self, grpc_stub):
        """
        :ds: /task/logsui
        :pipeline：/logurl
        :return: 运维页--更多--查看日志(kibana日志url)
        """
        name = 'diagnose_daily_03_1'
        user_info = ''
        execution_date = '2022/11/08 00:00:00.000'
        request = TaskSchedulerApi_pb2.GetLogRequest(name=name, executionDate=execution_date)
        response = grpc_stub.getLogUrl(request)
        print(response)

    def test_grpc_get_grpc_taskBackfill(self, grpc_stub):
        """
        :ds: /task/backfill
        :pipeline：/task/backfill
        :return: 首页-补数
        """
        task_ids = 'echo_1'
        start_date = '2022-12-25 00:00:00'
        end_date = '2022-12-26 23:59:59'
        operator = 'tianxu'
        core_task_name = 'echo_1'
        is_send_notify = True
        is_check_dependency = True
        request = TaskSchedulerApi_pb2.BackFillRequest(names=task_ids, coreTaskName=core_task_name, operator=operator,
                                                       startDate=start_date, endDate=end_date, isSendNotify=is_send_notify,
                                                       isCheckDependency=is_check_dependency)
        response = grpc_stub.taskBackfill(request)
        print(response)

    def test_grpc_progressReminder(self, grpc_stub):
        """
        :ds: /task/backfill/process
        :pipeline：/backfill/progress/reminder
        :return: 补数行为记录
        """
        user_action_id = 1
        request = TaskSchedulerApi_pb2.UserActionRequest(userActionId=user_action_id)
        response = grpc_stub.progressReminder(request)
        print(response)

    def test_grpc_getInstanceRelation(self, grpc_stub):
        """
        :ds: ds直接转发，不处理
        :原接口：/task/instance/relation
        :return: 运维页面-血缘关系
        """
        level = 1
        user_info = ''
        core_task = ''
        execution_date = ''
        is_downstream = True  # default=True
        request = TaskSchedulerApi_pb2.DataRelationRequest()
        response = grpc_stub.getInstanceRelation(request)
        print(response)

    def test_grpc_pauseTask(self, grpc_stub):
        """
        :ds: /task/onlineAndOffline
        :pipeline：/paused
        :return: 任务上下线
        """
        name = 'diagnose_daily_03_1'
        user_info = ''
        is_online = False  # False表示下线，True不起作用
        request = TaskSchedulerApi_pb2.PausedParamRequest(name=name, isOnline=is_online)
        response = grpc_stub.pauseTask(request)
        print(response)

    def test_grpc_getLast7Status(self, grpc_stub):
        """
        :原接口：/dagruns/last7instancestatus
        :return: 获取last7状态
        """
        # names = 'diagnose_daily_03_1,diagnose_daily_03_22'
        start = time.time()
        # names = 'test_migrate_ue1_sg1_0307,migration_ue1_sg1'
        names = 'test_migrate_ue1_sg2_0307'
        request = TaskSchedulerApi_pb2.Last7StatusRquest(names=names)
        response = grpc_stub.getLast7Status(request)
        end = time.time()
        print(response)
        print('cost= %.2f' % (end-start))  # 34.45

    def test_grpc_updateTask(self, grpc_stub):
        """
        :原接口：/task/update
        :return: 更新任务
        """
        scheduler_job = '''{
          "taskId": 17277,
          "taskName": "echo_1",
          "taskCode": {
            "name": "echo_1",
            "emails": "wuhaorui@ushareit.com",
            "owner": "wuhaorui",
            "retries": 1,
            "max_active_runs": 1,
            "email_on_failure": true,
            "input_datasets": [{
              "granularity": "daily",
              "offset": 0,
              "ready_time": "0 0 * * *",
              "check_path": "",
              "use_date_calcu_param": false,
              "unitOffset": "0",
              "date_calculation_param": {
              }
            }],
            "output_datasets": [{
              "id": "echo_1.echo_1@ue1",
              "granularity": "hourly",
              "offset": -1
            }],
            "extra_params": "{\"dingAlert\":\"true\"}",
            "event_depend": [{
              "task_id": "17279",
              "depend_id": "echo_2",
              "granularity": "daily",
              "unitOffset": "0",
              "date_calculation_param": {
              }
            }],
            "trigger_param": {
              "type": "cron",
              "output_granularity": "daily",
              "crontab": "00 00 * * *"
            },
            "depend_types": "event",
            "version": 7,
            "task_items": "[{\"command_tags\":\"type:spark-submit-sql-ds\",\"cluster_tags\":\"type:k8s,region:ue1,sla:normal\",\"task_type\":\"GenieJobOperator\",\"command\":\"--deploy-mode cluster --conf spark.kubernetes.driverEnv.HADOOP_USER_NAME\u003dwuhaorui --conf spark.app.name\u003decho_1 --conf spark.app.taskId\u003d17277 --conf spark.app.workflowId\u003d0 --conf spark.kubernetes.driver.label.owner\u003dwuhaorui --conf spark.kubernetes.executor.label.owner\u003dwuhaorui --conf spark.kubernetes.driver.label.entry\u003dDataStudio --conf spark.kubernetes.executor.label.entry\u003dDataStudio --conf spark.kubernetes.driver.label.tenantId\u003d1 --conf spark.kubernetes.executor.label.tenantId\u003d1 --conf spark.kubernetes.driver.label.groupId\u003d321983 --conf spark.kubernetes.executor.label.groupId\u003d321983 -e \\\"/*zheshiyigebianmabiaoshi*/c2VsZWN0JTIwMQ\u003d\u003d/*zheshiyigebianmabiaoshi*/\\\"\"}]"
          }
        }
        '''
        param = json_format.Parse(scheduler_job, ScheduleJob_pb2.ScheduleJob)
        request = TaskSchedulerApi_pb2.UpdateParamRequest(scheduleJob=param)
        response = grpc_stub.updateTask(request)
        print(response)
