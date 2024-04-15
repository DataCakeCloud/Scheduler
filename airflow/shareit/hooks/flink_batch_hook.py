# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
"""
Author: Tao Zhang
Date: 2021/12/14
"""
import time

import six
import requests
import subprocess

from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException
from airflow.shareit.utils.state import State


class FlinkBatchHook(BaseHook):
    """
    提交flink 批处理任务的Hook
    """

    def __init__(self,
                 command=None,
                 conn_id='flink_batch_default',
                 flink_cluster_name=None,
                 flink_job_name=None
                 ):
        """
        :param command: 批处理命令
        :type command: str or list[str]
        :param conn_id: connection_id string
        :type conn_id: str
        """
        self.command = command
        self._conn = self.get_connection(conn_id)
        self.flink_job_name = flink_job_name
        self.flink_cluster_name = flink_cluster_name
        self.namespace_map = {"shareit-cce-test": "cbs-flink",
                              "bdp-flink-sg1-prod": "bdp-flink",
                              "bdp-flink-ue1-prod": "bdp-flink",
                              "bdp-flink-sg2-prod": "bdp-flink"}
        self.cid_map = {"shareit-cce-test": "7",
                        "bdp-flink-sg1-prod": "47",
                        "bdp-flink-ue1-prod": "48",
                        "bdp-flink-sg2-prod": "51"}

    def get_conn(self):
        pass

    def _prepare_command(self, cmd):
        """
        Construct the flink batch process command to execute. Verbose output is enabled
        as default.

        :param cmd: command to append to the spark-sql command
        :type cmd: str or list[str]
        :return: full command to be executed
        """
        connection_cmd = ["flink", "run-application"]
        if isinstance(cmd, list):
            connection_cmd += cmd
        elif isinstance(cmd, six.string_types):
            cmd_list = cmd.split(" ")
            for param in cmd_list:
                if param != "":
                    connection_cmd.append(param.strip())

        self.log.debug("Flink batch process cmd: %s", connection_cmd)

        return connection_cmd

    def _generate_kube_config(self, config=None):
        """
        根据flink_connection 生成文件，放在 ～/.kube/config
        """
        if config is None or config == "":
            self.log.info("Config file is empty！Skip the step of configuring kube")
            return
        self.log.debug(config)
        import os
        home_path = os.environ['HOME']
        config_dir = home_path + "/.kube/"
        config_path = config_dir + "config"
        if not os.path.exists(config_dir):
            os.makedirs(config_dir)
        write_err = None
        for _ in range(3):
            try:
                with open(config_path, "w") as config_file:
                    config_file.write(config)
                write_err = None
                break
            except Exception as e:
                write_err = e
        if write_err is not None:
            self.log.error(str(write_err))

    def run_batch_process(self, cmd="", flink_url=None, **kwargs):
        """
        Remote Popen (actually execute the flink run-application)

        :param cmd: command to append to the flink batch command
        :type cmd: str or list[str]
        :param flink_url: flink url to get task state
        :type flink_url: str
        :param kwargs: extra arguments to Popen (see subprocess.Popen)
        :type kwargs: dict
        """
        self._generate_kube_config(config=self._conn.extra)
        if cmd is None or cmd == "":
            cmd = self.command
        flink_batch_cmd = self._prepare_command(cmd)
        self._sp = subprocess.Popen(flink_batch_cmd,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT,
                                    **kwargs)

        for line in iter(self._sp.stdout.readline, ''):
            self.log.info(line)

        return_code = self._sp.wait()
        state = ""
        for tmp in range(62):
            try:
                time.sleep(5)
                r = requests.get(url=flink_url)
                if r.status_code == 200:
                    state = r.json()["state"]
                    break
            except Exception as e:
                continue
        self.log.info("Get state:{ss} from:{url}".format(ss=state, url=flink_url))
        if return_code or state not in State.flink_submit_success_states:
            self._kill_flink_pod()
            raise AirflowException(
                "Cannot execute {} on {}. Process exit code: {}.".format(
                    cmd, self._conn.host, return_code
                )
            )

    def kill(self):
        if self._sp and self._sp.poll() is None:
            self.log.info("Killing the flink batch job")
            self._sp.kill()
        # self._kill_flink_pod()

    def _get_token(self):
        return ''

    def _kill_flink_pod(self):
        self.log.info("Try to kill flink pod through SCMP API ...")
        try:
            token = self._get_token()
            if self.flink_cluster_name is None or self.flink_cluster_name not in self.namespace_map:
                self.log.error("Flink cluster name: {name} parsing error!".format(name=self.flink_cluster_name))
                return
            else:
                namespace = self.namespace_map[self.flink_cluster_name]
                cid = self.cid_map[self.flink_cluster_name]
            baseurl = "http://prod.openapi-hulk.sgt.sg2.api/hulk/openapi/api/v2/apps/{namespace}/deployment/{job_name}".format(
                namespace=namespace,
                job_name=self.flink_job_name)
            data = {
                "cid": cid
            }
            headers = {
                'Authorization': token,
                'Content-Type': 'application/json',
                'cid': cid
            }
            requests.packages.urllib3.disable_warnings()
            resp = requests.delete(baseurl, headers=headers, params=data, verify=False)
            if resp.status_code == 200:
                self.log.info("Success kill flink pod.")
            else:
                self.log.error("Failed to kill flink pod!")
        except Exception as e:
            self.log.error("Failed to kill flink pod! "+str(e),exc_info=True)
