# -*- coding: utf-8 -*-
import logging
import traceback

from flask import Flask, Response
from prometheus_client import generate_latest

from airflow.shareit.monitor.monitor import Monitor

# -*- coding: utf-8 -*-
import multiprocessing
from airflow import LoggingMixin
from airflow.configuration import conf


# monitor = Monitor()
app = Flask(__name__)


class MonitorRun(LoggingMixin):
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

@app.route('/metrics')
def export_metrics():
    try:
        monitor = Monitor()
        monitor.get_prometheus_metrics()
        return Response(generate_latest(monitor.collector_registry), mimetype='text/plain')
    except Exception as e:
        logging.error(traceback.format_exc())
        logging.error("/metrics code:2 " + str(e), exc_info=True)

