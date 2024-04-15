# -*- coding: utf-8 -*-
import multiprocessing
import time
from signal import signal, SIGTERM

import grpc
from concurrent import futures

from grpc_reflection.v1alpha import reflection

from airflow import LoggingMixin
from airflow.configuration import conf

from airflow.shareit.grpc.TaskServiceApi import WorkflowServiceApi_pb2, WorkflowServiceApi_pb2_grpc, \
    TaskSchedulerApi_pb2, TaskSchedulerApi_pb2_grpc, TaskServiceApi_pb2_grpc, TaskServiceApi_pb2
from airflow.shareit.grpc.TaskServiceApi.entity import CommonResponse_pb2, CommonResponse_pb2_grpc
from airflow.shareit.grpc.service.task_grpc_temp_service import TaskServiceTempGrpcService
from airflow.shareit.grpc.service.workflow_grpc_service import WorkflowGrpcService
from airflow.shareit.grpc.service.task_grpc_service import TaskServiceGrpcService


class GrpcRun(LoggingMixin):
    def __init__(self):
        self.max_workers = conf.getint('grpc', 'max_workers')
        self.port = conf.getint('grpc','port')
        # self.max_workers = 4
        # self.port = 9999

    @staticmethod
    def _run(max_workers,port):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers), options=[
            ('grpc.max_send_message_length', 100 * 1024 * 1024),
            ('grpc.max_receive_message_length', 100 * 1024 * 1024)])

        WorkflowServiceApi_pb2_grpc.add_WorkflowRpcServiceServicer_to_server(WorkflowGrpcService(), server)
        TaskSchedulerApi_pb2_grpc.add_TaskSchedulerRpcApiServicer_to_server(TaskServiceGrpcService(), server)
        TaskServiceApi_pb2_grpc.add_TaskRpcServiceServicer_to_server(TaskServiceTempGrpcService(), server)
        server.add_insecure_port('[::]:{}'.format(str(port)))
        server.start()

        SERVICE_NAMES =(
            WorkflowServiceApi_pb2.DESCRIPTOR.services_by_name['WorkflowRpcService'].full_name,
            TaskSchedulerApi_pb2.DESCRIPTOR.services_by_name['TaskSchedulerRpcApi'].full_name,
            TaskServiceApi_pb2.DESCRIPTOR.services_by_name['TaskRpcService'].full_name,
            reflection.SERVICE_NAME
        )
        # 开启反射服务,这样可以通过grpcurl直接调用
        reflection.enable_server_reflection(SERVICE_NAMES, server)
        def sig_handle(*args):
            server.stop(None)
            # 调用server.stop函数，调用stop(10)后，该服务会拒绝客户端新的请求，并返回一个threading.Event
            # event = server.stop(10)
            # # 调用event.wait()后，服务会一直等着，直到10秒后服务才会关闭，关闭之前服务还是能正常处理请求
            # event.wait()

        # 接收一个SIGTERM信号的注册
        signal(SIGTERM, sig_handle)
        # 等待服务停止
        server.wait_for_termination()

    def start_asyn(self):
        self.log.info('[Datastudio server] START GRPC SERVING asyn............')
        # self._run(self.max_workers,self.port)
        self._process = multiprocessing.Process(
            target=type(self)._run,
            args=(
                self.max_workers,
                self.port,
            ),
            name="grpcRun-Process"
        )
        # self._start_time = timezone.utcnow()
        self._process.start()
        self.log.info("[Datastudio server] Launched GrpcRun with pid: %s", self._process.pid)

    def start(self):
        self.log.info('[Datastudio server] START GRPC SERVING............')
        self._run(self.max_workers,self.port)
        # self._process = multiprocessing.Process(
        #     target=type(self)._run,
        #     args=(
        #         self.max_workers,
        #         self.port,
        #     ),
        #     name="grpcRun-Process"
        # )
        # # self._start_time = timezone.utcnow()
        # self._process.start()
        # self.log.info("[Datastudio server] Launched GrpcRun with pid: %s", self._process.pid)