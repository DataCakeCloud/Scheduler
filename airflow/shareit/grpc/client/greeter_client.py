# -*- coding: utf-8 -*-
import base64
import json
import sys

import grpc
from google.protobuf import json_format
from google.protobuf.json_format import MessageToJson

import airflow.shareit.grpc.TaskServiceApi.TaskServiceApi_pb2
import airflow.shareit.grpc.TaskServiceApi.TaskServiceApi_pb2_grpc
from airflow import LoggingMixin
from airflow.configuration import conf
from airflow.shareit.constant.return_code import ReturnCodeConstant
from airflow.shareit.grpc.TaskServiceApi import TaskServiceApi_pb2
from airflow.shareit.grpc.TaskServiceApi.entity import UserInfo_pb2

reload(sys)
sys.setdefaultencoding("utf-8")
class GreeterClient(LoggingMixin):
    def __init__(self):
        grpc_env = conf.get('grpc', 'ds_grpc_host')
        # domain = grpc_env
        self.domain = str(grpc_env)

    def get_user_info(self,tenant_id):
        userinfo = UserInfo_pb2.UserInfo(tenantId=tenant_id)
        userinfo_json = json_format.MessageToJson(userinfo)
        encode_json = base64.b64encode(userinfo_json)
        return encode_json


    def getTaskInfoByName(self,name,tenant_id=1):
        # NOTE(gRPC Python Team): .close() is possible on a channel and should be
        # used in circumstances in which the with statement does not fit the needs
        # of the code.
        # grpc_env = conf.get('core', 'grpc_env')
        # # domain = grpc_env + '.ds-task-api.bdp.sg2.grpc:80'  # type=<class 'future.types.newstr.newstr'>
        # domain = '{}.ds-task-api.bdp.sg2.grpc:80'.format(grpc_env)
        user_info_str = self.get_user_info(tenant_id=tenant_id)
        with grpc.insecure_channel(self.domain) as channel:
            stub = airflow.shareit.grpc.TaskServiceApi.TaskServiceApi_pb2_grpc.TaskRpcServiceStub(channel)
            response = stub.getTaskInfoByName(
                airflow.shareit.grpc.TaskServiceApi.TaskServiceApi_pb2.NameRequest(taskName=name),metadata=[('current_login_user',user_info_str)])
        # print("TaskServiceApi client received isSparkTask: ", response.info.isSparkTask)
        # return response
        serialized_message_with_defaults = MessageToJson(
          response,
          including_default_value_fields=True,  # this does it
        )
        return json.loads(serialized_message_with_defaults)

    def getTaskInfoByNameNew(self,name,tenant_id=1):
        # NOTE(gRPC Python Team): .close() is possible on a channel and should be
        # used in circumstances in which the with statement does not fit the needs
        # of the code.
        # grpc_env = conf.get('core', 'grpc_env')
        # # domain = grpc_env + '.ds-task-api.bdp.sg2.grpc:80'  # type=<class 'future.types.newstr.newstr'>
        # domain = '{}.ds-task-api.bdp.sg2.grpc:80'.format(grpc_env)
        user_info_str = self.get_user_info(tenant_id=tenant_id)
        with grpc.insecure_channel(self.domain) as channel:
            stub = airflow.shareit.grpc.TaskServiceApi.TaskServiceApi_pb2_grpc.TaskRpcServiceStub(channel)
            response = stub.getTaskInfoByName(
                airflow.shareit.grpc.TaskServiceApi.TaskServiceApi_pb2.NameRequest(taskName=name),metadata=[('current_login_user',user_info_str)])
        # print("TaskServiceApi client received isSparkTask: ", response.info.isSparkTask)
        return response


    def getTaskInfoByID(self,ds_task_list,tenant_id=1):
        # NOTE(gRPC Python Team): .close() is possible on a channel and should be
        # used in circumstances in which the with statement does not fit the needs
        # of the code.
        # grpc_env = conf.get('core', 'grpc_env')
        # # domain = grpc_env + '.ds-task-api.bdp.sg2.grpc:80'  # type=<class 'future.types.newstr.newstr'>
        # domain = '{}.ds-task-api.bdp.sg2.grpc:80'.format(grpc_env)
        user_info_str = self.get_user_info(tenant_id=tenant_id)
        arr = []
        for task in ds_task_list:
            if 'version' in task.keys():
                id_pair = TaskServiceApi_pb2.IdVersionPair(taskID=int(task['taskId']),version=int(task['version']))
            else :
                id_pair = TaskServiceApi_pb2.IdVersionPair(taskID=int(task['taskId']))
            arr.append(id_pair)

        with grpc.insecure_channel(self.domain) as channel:
            stub = airflow.shareit.grpc.TaskServiceApi.TaskServiceApi_pb2_grpc.TaskRpcServiceStub(channel)
            response = stub.getTaskInfoByID(
                airflow.shareit.grpc.TaskServiceApi.TaskServiceApi_pb2.IDRequest(idVersionPair=arr),metadata=[('current_login_user',user_info_str)])
        # print("TaskServiceApi client received isSparkTask: ", response.info.isSparkTask)
        if response.code != ReturnCodeConstant.CODE_SUCCESS:
            raise ValueError("grpc.getTaskInfoByID error ,{}".format(response.message))
        return response
        # serialized_message_with_defaults = MessageToJson(
        #   response,
        #   including_default_value_fields=True,  # this does it
        # )
        # return json.loads(serialized_message_with_defaults)
