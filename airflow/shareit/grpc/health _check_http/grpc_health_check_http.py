# -*- coding: utf-8 -*-
import logging
import traceback

import grpc
from flask import Flask, Response
from grpc_health.v1 import health_pb2_grpc, health_pb2
from prometheus_client import generate_latest

from airflow.shareit.monitor.monitor import Monitor

# -*- coding: utf-8 -*-
import multiprocessing
from airflow import LoggingMixin
from airflow.configuration import conf



# monitor = Monitor()
app = Flask(__name__)
grpc_service = health_pb2_grpc.HealthStub(grpc.aio.insecure_channel("0.0.0.0:9000"))

class GrpcHealthCheckHttp(LoggingMixin):
    def __init__(self):
        self.port = conf.getint('monitor','port')
        return

    @staticmethod
    def _run(port):
        # global monitor
        global app
        app.run(host='0.0.0.0',port=port)


    def start(self):
        self.log.info('[Datastudio server] START MONITOR SERVING............')
        self._process = multiprocessing.Process(
            target=type(self)._run,
            args=(
                self.port,
            ),
            name="monitor-Process"
        )
        self._process.start()
        self.log.info("[Datastudio server] Launched MONITOR with pid: %s", self._process.pid)

@app.route('/health/check')
def health_check():
    try:
        client = health_pb2_grpc.HealthStub(grpc.aio.insecure_channel("0.0.0.0:9000"))
        # 在flask中会卡在这个地方
        res = client.Check(
            health_pb2.HealthCheckRequest()
        )

    except Exception as e:
        logging.error(traceback.format_exc())
        logging.error("/metrics code:2 " + str(e), exc_info=True)

