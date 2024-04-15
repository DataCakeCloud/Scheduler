# -*- coding: utf-8 -*-
"""
author: zhangtao
Copyright (c) 2021/12/15 Shareit.com Co., Ltd. All Rights Reserved.
"""

import time
import requests

from airflow.utils import timezone
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException

from airflow.shareit.hooks.flink_batch_hook import FlinkBatchHook
from airflow.shareit.utils.state import State


class FlinkBatchOperator(BaseOperator):
    template_fields = ('command',)
    ui_color = '#c94989'

    @apply_defaults
    def __init__(self,
                 command=None,
                 flink_conn_id="flink_default",
                 hang_timeout=300,
                 *args, **kwargs):
        super(FlinkBatchOperator, self).__init__(*args, **kwargs)
        self.command = command
        self.flink_conn_id = flink_conn_id
        self.flink_batch_hook = None
        self.hang_timeout = hang_timeout

    def execute(self, context):
        # if "KubernetesExecutor" in self.executor_config:
        #     if "image" in self.executor_config["KubernetesExecutor"]:
        #         pass
        #     else:
        #         self.executor_config["KubernetesExecutor"]["image"] = "848318613114.dkr.ecr.us-east-1.amazonaws.com" \
        #                                                               "/shareit-bdp/airflow-environment" \
        #                                                               ":flink_executor1"
        # else:
        #     self.executor_config["KubernetesExecutor"] = {
        #     "image": "848318613114.dkr.ecr.us-east-1.amazonaws.com/shareit-bdp/airflow-environment:flink_executor1"}
        # 获取flink url，并将其保存至taskInstance的external_conf
        ti = None
        if isinstance(context, dict) and "ti" in context.keys():
            ti = context['ti'] if self.task_id == context['ti'].dag_id else None
        job_name, cluster = self._get_name_and_cluster()
        flink_url = "http://{job_name}-rest.{cluster}.flink.ushareit.org/jobs/00000000000000000000000000000000".format(
            job_name=job_name, cluster=cluster)
        try:
            if ti:
                ti.save_external_conf(ex_conf={"flink_url": flink_url})
        except Exception as e:
            self.log.debug("Can not save flink url " + str(e))

        self.flink_batch_hook = FlinkBatchHook(command=self.command,
                                               conn_id=self.flink_conn_id,
                                               flink_job_name=job_name,
                                               flink_cluster_name=cluster)
        self.flink_batch_hook.run_batch_process(flink_url=flink_url)
        # 请求flink url 获取任务状态，如果不属于flink_submit_success_states中状态或者hang_timeout没请求到状态。则任务失败
        execute_start_time = timezone.utcnow()
        while (timezone.utcnow() - execute_start_time).total_seconds() < self.hang_timeout:
            self.log.info(
                "The flink batch task has been submitted successfully and get state from flink url:{url}".format(
                    url=flink_url))
            try:
                state = None
                r = requests.get(url=flink_url)
                if r.status_code == 200:
                    state = r.json()["state"]
                if state is not None:
                    execute_start_time = timezone.utcnow()
                    if state not in State.flink_submit_success_states:
                        raise AirflowException(
                            "Get {state} state from flink url, Task will set failed".format(state=state))
                else:
                    self.log.info(
                        "Can not get state from flink url,will retry after 60s")
            except Exception as e:
                raise e
            time.sleep(60)
        raise AirflowException("{timeout} second can not get qualified state".format(timeout=self.hang_timeout))

    def on_kill(self):
        """
        kill genie job when kill airflow task
        :return:
        """
        self.flink_batch_hook.kill()

    def _get_name_and_cluster(self):
        """
        从command中获取参数-Dkubernetes.cluster-id,-Dkubernetes.context 的值
        """
        command_format = self.command
        while "= " in command_format or " =" in command_format:
            command_format = command_format.replace("= ", "=").replace(" =", "=")
        command_list = command_format.split()
        job_name = ""
        cluster = ""
        for args in command_list:
            if "-Dkubernetes.cluster-id=" in args:
                job_name = args.split("-Dkubernetes.cluster-id=")[1]
            elif "-Dkubernetes.context=" in args:
                cluster = args.split("-Dkubernetes.context=")[1]
        return job_name, cluster
