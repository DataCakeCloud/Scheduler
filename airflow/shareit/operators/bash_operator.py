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
import base64
import json
import os
import random
import signal
import time
import traceback
import urllib
import zlib
from subprocess import Popen, STDOUT, PIPE
from tempfile import gettempdir, NamedTemporaryFile
import uuid

import boto3
import requests
from builtins import bytes

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.models.cpu_memory import CpuMemory
from airflow.shareit.utils import hive_util
from airflow.shareit.utils.cloud.cloud_utli import CloudUtil
from airflow.shareit.utils.constant_config_util import ConstantConfigUtil
from airflow.shareit.utils.normal_util import NormalUtil
from airflow.shareit.utils.obs_util import Obs
from airflow.shareit.utils.s3_util import S3
from airflow.utils.db import provide_session
from airflow.utils.decorators import apply_defaults
from airflow.utils.file import TemporaryDirectory
from airflow.utils.operator_helpers import context_to_airflow_vars
from airflow.configuration import conf as ds_conf, conf


class BashOperator(BaseOperator):
    """
    Execute a Bash script, command or set of commands.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BashOperator`

    :param bash_command: The command, set of commands or reference to a
        bash script (must be '.sh') to be executed. (templated)
    :type bash_command: str
    :param xcom_push: If xcom_push is True, the last line written to stdout
        will also be pushed to an XCom when the bash command completes.
    :type xcom_push: bool
    :param env: If env is not None, it must be a mapping that defines the
        environment variables for the new process; these are used instead
        of inheriting the current process environment, which is the default
        behavior. (templated)
    :type env: dict
    :param output_encoding: Output encoding of bash command
    :type output_encoding: str
    """
    template_fields = ('bash_command', 'env')
    template_ext = ('.sh', '.bash',)
    ui_color = '#f0ede4'

    @apply_defaults
    def __init__(
            self,
            bash_command=None,
            xcom_push=False,
            workdir="/work",
            type="shell",
            files=None,
            # ssh_command=None,
            env=None,
            output_encoding='utf-8',
            dynamics_cmd=False,
            *args, **kwargs):

        super(BashOperator, self).__init__(*args, **kwargs)
        self.bash_command = bash_command
        self.env = env
        self.xcom_push_flag = xcom_push
        self.output_encoding = output_encoding
        self.sub_process = None
        self.type = type
        self.files = files
        self.workdir=workdir
        self.ssh_command = None
        self.bash_command=None
        # self.mkdir_command=None
        self.target_table=None
        self.partitions = None
        self.pool = conf.get("core", "local_task_pool")
        self.task_desc = None
        self.datax_job = False
        self.spark_job = False
        self.dynamics_cmd = dynamics_cmd

    def choose_schedule_resource(self):
        resources = base64.b64decode(conf.get("core", "host_pool")).decode('utf-8').split(",")
        # if len(resources) == 1:
        #     return resources[0]
        cpu = float(conf.get('resource', 'cpu_threshold'))
        memory = float(conf.get('resource', 'memory_threshold'))
        sleep = int(conf.get('resource', 'sleep'))
        result = ''
        flag = True
        while flag:
            cpu_memory = CpuMemory().get_resource()
            random.shuffle(cpu_memory)
            for item in cpu_memory:
                for resource in resources:
                    if item[1] in resource and (item[2] < cpu and item[3] < memory):
                        result = resource
            if not result:
                self.log.info("There are no qualified machines for scheduling")
                time.sleep(sleep)
            else:
                self.log.info(
                    "There is a machine that meets the resource conditions and is about to execute the task: %s",
                    result)
                flag = False
        return result

    def execute(self, context):
        """
        Execute the bash command in a temporary directory
        which will be cleaned afterwards
        """
        self.log.info("Tmp dir root location: \n %s", gettempdir())
        ti = None
        log_name = str(uuid.uuid4())
        if isinstance(context, dict) and "ti" in context.keys():
            ti = context['ti']
        if ti:
            ex_conf = ti.get_external_conf()
            ex_conf = ti.format_external_conf(ex_conf)
            ex_conf['batch_id'] = log_name
            ex_conf['new_log'] = True
            if ex_conf is not None:
                ti.save_external_conf(ex_conf=ex_conf)

        # Prepare env for child process.

        env = self.env
        if env is None:
            env = os.environ.copy()
        airflow_context_vars = context_to_airflow_vars(context, in_env_var_format=True)
        self.log.debug('Exporting the following env vars:\n%s',
                       '\n'.join(["{}={}".format(k, v)
                                  for k, v in airflow_context_vars.items()]))
        env.update(airflow_context_vars)

        self.lineage_data = self.ssh_command

        with TemporaryDirectory(prefix='airflowtmp') as tmp_dir:
            with NamedTemporaryFile(dir=tmp_dir, prefix=self.task_id) as f:

                f.write(bytes(self.ssh_command, 'utf_8'))
                f.flush()
                fname = f.name
                script_location = os.path.abspath(fname)
                self.log.info(
                    "Temporary script location: %s",
                    script_location
                )
                def pre_exec():
                    # Restore default signal disposition and invoke setsid
                    for sig in ('SIGPIPE', 'SIGXFZ', 'SIGXFSZ'):
                        if hasattr(signal, sig):
                            signal.signal(getattr(signal, sig), signal.SIG_DFL)
                    os.setsid()

                self.log.info("Running command: %s", self.bash_command)
                self.sub_process = Popen(
                    ['bash', fname],
                    stdout=PIPE, stderr=STDOUT,
                    cwd=tmp_dir, env=env,
                    preexec_fn=pre_exec)

                self.log.info("Output:")

                dag_folder = ds_conf.get('core', 'base_log_folder')
                # 本地文件
                app_log_file = '{dag_folder}/{dag_id}/{task_id}/{time}/{batch_id}'.format(dag_folder=dag_folder,
                                                                                          dag_id=ti.dag_id,
                                                                                          task_id=ti.task_id,
                                                                                          time=ti.execution_date.isoformat(),
                                                                                          batch_id=log_name)
                # 上传到云存储的桶
                log_cloud_home = ds_conf.get('job_log', 'log_cloud_file_location').strip()
                # 云存储的文件
                log_cloud_file = '{log_cloud_home}/BDP/logs/pipeline_log/{time}/{batch_id}.txt'.format(
                    log_cloud_home=log_cloud_home,
                    time=ti.start_date.strftime(
                        '%Y%m%d'),
                    batch_id=log_name)
                self.log.info('local file location: {}'.format(app_log_file))
                self.log.info('cloud file location: {}'.format(log_cloud_file))
                log_provider = conf.get('job_log', 'log_provider')
                log_region = conf.get('job_log', 'log_region')
                cloud_object = CloudUtil(provider=log_provider, region=log_region)
                self.save_log(log_cloud_file,app_log_file,"Task starting...",cloud_object)

                line = ''
                log_text = self.bash_command+"\n"
                start_time = int(time.time())
                for line in iter(self.sub_process.stdout.readline, b''):
                    line = line.decode(self.output_encoding).rstrip()
                    log_text += line + "\n"
                    end_time = int(time.time())
                    self.log.info(line)
                    if end_time - start_time >=15:
                        self.save_log(log_cloud_file,app_log_file,log_text,cloud_object)
                        start_time = int(time.time())
                self.sub_process.wait()
                self.log.info("target_table: %s", self.target_table)
                if self.partitions:
                    try:
                        log_text += "\n" + str("alter table {table} add if not exists partition ({partions})".format(
                            table=self.target_table,partions=self.partitions))
                        user_group = self.task_desc.user_group
                        hive_util.execute_msck_partion(self.target_table,
                                                       self.owner,
                                                       user_group.get('uuid',''),
                                                       user_group.get('tenantName',''),
                                                       user_group.get('currentGroup',''),
                                                       self.partitions)
                    except Exception as e:
                        self.log.error(traceback.format_exc())
                        log_text += "\n"+str(e)
                        self.save_log(log_cloud_file, app_log_file, log_text, cloud_object)
                        raise AirflowException("Bash command failed")

                self.save_log(log_cloud_file,app_log_file,log_text,cloud_object)
                self.log.info(
                    "Command exited with return code %s",
                    self.sub_process.returncode
                )

                if self.sub_process.returncode:
                    raise AirflowException("Bash command failed")

        if self.xcom_push_flag:
            return line

    def on_kill(self):
        self.log.info('Sending SIGTERM signal to bash process group')
        if self.sub_process and hasattr(self.sub_process, 'pid'):
            os.killpg(os.getpgid(self.sub_process.pid), signal.SIGTERM)

    def encode_command_with_zip_base64(self):
        self.spark_job=True
        tmpList = self.bash_command.split('/*datacakebianma*/')
        if len(tmpList) == 3:
            compressed = zlib.compress(tmpList[1])
            base64Code = base64.b64encode(compressed)
            base64Code = self._insert_space(line=base64Code, interval=1024)
            tmpList[1] = base64Code
            self.bash_command = " ".join(tmpList)

    def encode_command_with_base64(self):
        tmpList = self.bash_command.split('/*zheshiyigebianmabiaoshi*/')
        if len(tmpList) == 3:
            self.datax_job=True
            json_str = json.loads(tmpList[1])
            hdfs_writer = json_str['job']['content'][0]['writer']
            if "hdfswriter" in json_str['job']['content'][0]['writer']['name']:
                self.target_table = hdfs_writer['parameter']['fileName']
                path = hdfs_writer['parameter']['path']
                partion_path = path.split(self.target_table.split(".")[1])[1]
                if len(partion_path) > 1:
                    partition = partion_path[1:].split("/")
                    dt_list = []
                    for pt in partition:
                        dt_list.append("{key}='{value}'".format(key=pt.split("=")[0], value=pt.split("=")[1]))
                    self.partitions = ",".join(dt_list)
            base64Code = base64.b64encode(tmpList[1])
            base64Code = self._insert_space(line=base64Code, interval=1024)
            tmpList[1] = '''"{base64Code}"'''.format(base64Code=base64Code)
            self.bash_command = " ".join(tmpList)
    def pre_execute(self, context):
        from airflow.shareit.service.task_service import TaskService
        self.encode_command_with_base64()
        self.encode_command_with_zip_base64()
        self.task_file_name = self.task_id + "-" + str(int(time.time() * 1000))
        command = TaskService.render_api(self.bash_command, context, task_name=self.task_id)
        if (self.type == "python"):
            file_name = "python/{}.py".format(self.task_file_name)
            if self.datax_job:
                cmd_str = "sudo python {workdir}/{task_file_name}.py".format(workdir=self.workdir,
                    task_file_name=self.task_file_name)
            else:
                cmd_str = "sudo -u {user_group} -i python {workdir}/{task_file_name}.py".format(user_group=self.task_desc.user_group.get('currentGroup',''),
                                                                                         workdir=self.workdir,
                                                                                         task_file_name=self.task_file_name)
            rm_tmp_cmd = "rm {workdir}/{task_file_name}.py".format(workdir=self.workdir,
                                                                   task_file_name=self.task_file_name)
        else:
            file_name = "shell/{}.sh".format(self.task_file_name)
            if self.datax_job:
                cmd_str = "sudo sh {workdir}/{task_file_name}.sh".format(workdir=self.workdir,
                    task_file_name=self.task_file_name)
            else:
                cmd_str = "sudo -u {user_group} -i sh {workdir}/{task_file_name}.sh".format(user_group=self.task_desc.user_group.get('currentGroup',''),
                                                                                         workdir=self.workdir,
                                                                                         task_file_name=self.task_file_name)
            rm_tmp_cmd = "rm {workdir}/{task_file_name}.sh".format(workdir=self.workdir,
                                                                   task_file_name=self.task_file_name)
            command = "#!/bin/sh\n" + command
        region, provider = NormalUtil.parse_cluster_tag(self.cluster_tags)
        artifact_home = ConstantConfigUtil.get_string('artifact_home',
                                                      '{},{}'.format(provider, region))
        cloud_path = "{}/{}".format(artifact_home.rstrip("/"), file_name)
        region, provider = NormalUtil.parse_cluster_tag(self.cluster_tags)
        self.cloud = CloudUtil(provider=provider, region=region)
        self.cloud.put_content_to_cloud(cloud_path, command)
        self.upload_command()
        artifact_dir = conf.get("core", "artifact_dir")
        cmd_list = []
        if artifact_home.startswith("s3"):
            for file in self.files:
                if self.task_file_name in file:
                    cmd_list.append(
                        "aws s3 cp {file} {workdir}".format(file=self._format_input_path(file), workdir=self.workdir))
                else:
                    cmd_list.append("sudo -u {user_group} -i aws s3 cp {file} {workdir}/{user_group}/job/".format(
                        file=self._format_input_path(file),
                        workdir=artifact_dir,
                        user_group=self.task_desc.user_group.get('currentGroup','')))
        elif artifact_home.startswith("obs") :
            for file in self.files:
                cmd_list.append("obsutil cp {file} {workdir}".format(file=self._format_input_path(file),workdir=self.workdir))
        elif artifact_home.startswith("ks3"):
            AK = conf.get('cloud_authority', 'ks3_ak', fallback='')
            SK = conf.get('cloud_authority', 'ks3_sk', fallback='')
            server = conf.get('cloud_authority', 'ks3_server', fabllback='')
            for file in self.files:
                if self.task_file_name in file:
                    cmd_list.append(
                        "ks3util -i {ak}  -k {sk} -e {server} cp {file} {workdir}".format(ak=AK, sk=SK, server=server,
                                                                                          file=self._format_input_path(file),
                                                                                          workdir=self.workdir))
                else:
                    cmd_list.append(
                        "sudo -u {user_group} -i ks3util -i {ak}  -k {sk} -e {server} cp {file} {workdir}/{user_group}/job/".format(ak=AK,
                                                                                                            sk=SK,
                                                                                                            server=server,
                                                                                                            file=self._format_input_path(file),
                                                                                                            workdir=artifact_dir,
                                                                                                            user_group=self.task_desc.user_group.get('currentGroup','')))
        # if self.mkdir_command:
        #     cmd_list.append(self.mkdir_command)
        cmd_list.append(cmd_str)
        self.ssh_command = '''{ssh_command} "{cmd} && {rm_tmp_cmd}"'''.format(ssh_command=self.ssh_command,
                                                                              cmd=" && ".join(cmd_list),
                                                                              rm_tmp_cmd=rm_tmp_cmd)

    def upload_command(self):
        region, provider = NormalUtil.parse_cluster_tag(self.cluster_tags)
        if (self.type == "python"):
            file_name = "python/{}.py".format(self.task_file_name)
        else:
            file_name = "shell/{}.sh".format(self.task_file_name)
        artifact_home = ConstantConfigUtil.get_string('artifact_home', '{},{}'.format(provider, region))
        file = "{}/{}".format(artifact_home.rstrip("/"), file_name)
        self.files.append(file)

    def _insert_space(self, line=None, interval=None):
        if interval is None:
            interval = 1024
        tmpList = [line[i:i + interval] for i in range(0, len(line), interval)]
        return " ".join(tmpList)

    def _format_input_path(self,input_path=None):
        """
        将http路径改为s3:// 或者obs：// 开头的形式
        :param input_path:
        :return:
        """
        if input_path is None or input_path == "":
            return ""
        input_path = input_path.strip()
        if input_path.startswith("https://s3.amazonaws.com/"):
            return input_path.replace("https://s3.amazonaws.com/", "s3://")
        elif "http" in input_path:
            if "amazonaws.com" in input_path:
                tmp = input_path.split("?")[0].split("://")[1]
                bucket = ".".join(tmp.split("/")[0].split(".")[0:-4])
                key = "/".join(tmp.split("/")[1:])
                return "s3://" + bucket + "/" + key
            elif "myhuaweicloud.com" in input_path:
                tmp = input_path.split("?")[0].split("://")[1]
                bucket = ".".join(tmp.split("/")[0].split(".")[0:-4])
                key = "/".join(tmp.split("/")[1:])
                return "obs://" + bucket + "/" + key
            elif "ksyuncs.com" in input_path:
                tmp = input_path.strip().replace('http://','').split("/")
                bucket = tmp[0].split('.')[0]
                key = "/".join(tmp[1:])
                return "ks3://{}/{}".format(bucket,key)
        return input_path


    def save_log(self,log_cloud_file,app_log_file,content,cloud_object):
        with open(app_log_file, 'w') as f:
            f.write(content)
        cloud_object.upload_with_content_type(log_cloud_file, app_log_file,content_type='text/plain')


    def flush_cmd(self):
        if self.dynamics_cmd:
            url = conf.get("core","de_cmd_url")
            header = {
                'current_login_user': '{{"tenantName":"{}","tenantId":{} }}'.format(self.task_desc.user_group.get('tenantName', ''), self.task_desc.user_group.get('tenantId', '')),
                'currentGroup': self.task_desc.user_group.get('currentGroup', ''),
                'groupId': self.task_desc.user_group.get('groupId', ''),
                'uuid': 'INNER_SCHEDULE'
            }
            url = '{}?taskId={}'.format(url,str(self.task_desc.ds_task_id))
            self.log.info("get cmd, header: {}".format(json.dumps(header)))
            self.log.info("get cmd, url: {}".format(url))
            for i in range(1):
                try:
                    resp = requests.get(url,headers=header)
                    if resp.status_code != 200:
                        self.log.error("GET cmd failed,resp :{}".format(resp.content))
                    resp_json = resp.json()
                    self.log.info('get cmd, resp content:{}'.format(json.dumps(resp_json)))
                    cmd = resp_json.get('data')
                    cmd_list = cmd.split('/*zheshiyigebianmabiaoshi*/')
                    if len(cmd_list) == 3:
                        # base64 解码
                        base64Decode = base64.b64decode(cmd_list[1])
                        # URL 解码
                        base64Decode = urllib.unquote(base64Decode)
                        cmd_list[1] = base64Decode
                        cmd = '/*zheshiyigebianmabiaoshi*/'.join(cmd_list)
                    self.bash_command = cmd
                    break
                except Exception as e:
                    self.log.info("获取cmd失败，重试次数：{}".format(i))
                    self.log.error("获取cmd失败，error:{}".format(str(e)))
                    time.sleep(20)

    def flush(self):
        self.flush_cmd()
        self.ssh_command = self.choose_schedule_resource()
