# -*- coding: utf-8 -*-
import sys

from airflow.configuration import conf
import time

from airflow.shareit.models.task_desc import TaskDesc

import boto3
import six
from airflow.contrib.kubernetes.pod import Resources

import airflow
from airflow.shareit.operators.kyuubi_pod_operator import KyuubiPodOperator
from airflow.shareit.utils import obs_util
from airflow.shareit.utils.cloud.cloud_utli import CloudUtil
from airflow.shareit.utils.constant_config_util import ConstantConfigUtil
from airflow.shareit.utils.normal_util import NormalUtil
from airflow.shareit.utils.obs_util import Obs

reload(sys)
sys.setdefaultencoding('utf-8')
from airflow.utils import apply_defaults


class KyuubiScriptOperator(KyuubiPodOperator):
    @apply_defaults
    def __init__(self,
                 image=None,
                 files=None,
                 scripts=None,
                 workdir=None,
                 resources=None,
                 volumes=None,
                 type="shell",
                 volume_mounts=None,
                 is_delete_operator_pod=True,
                 image_pull_secrets="default-secret",
                 startup_timeout_seconds=7200,
                 task_id="script_task",
                 command=None,
                 is_psql=False,
                 *args,
                 **kwargs):
        super(KyuubiScriptOperator, self).__init__(task_id=task_id, *args, **kwargs)
        if volume_mounts is None:
            volume_mounts = []
        if volumes is None:
            volumes = []
        self.image = image
        self.files = files
        self.scripts = scripts
        self.is_delete_operator_pod = is_delete_operator_pod
        self.image_pull_secrets = image_pull_secrets
        self.startup_timeout_seconds = startup_timeout_seconds
        self.task_id = task_id
        self.resources=resources
        self.volumes = volumes
        self.type = type
        self.volume_mounts = volume_mounts
        self.workdir = workdir
        self.command_tags = None
        self.build_cmds()
        self.is_psql = is_psql
        self.command = command

    def valid_params(self):
        """
        验证输入参数
        :return:
        """
        pass



    def build_cmds(self):
        cmd_list = []
        self.task_file_name = self.task_id + "-" + str(int(time.time() * 1000))
        if (self.type == "python"):
            cmd_str = "python /work/{}.py".format(self.task_file_name)
        else:
            cmd_str = "sh /work/{}.sh".format(self.task_file_name)

        if self.scripts is not None:
            cmd_list = ["sh","-c"]
            cmd_list.append(cmd_str)
        self.cmds = cmd_list


    def get_resources(self,resources):
        input_resource = Resources()
        if resources:
            for item in resources.keys():
                setattr(input_resource, item, resources[item])
        return input_resource

    def pre_execute(self, context):
        from airflow.shareit.service.task_service import TaskService
        cmds=""
        for cmd in self.scripts:
            cmds += "\n"+cmd
        command = TaskService.render_api(cmds,context,task_name=self.task_id)
        # region, provider = NormalUtil.parse_cluster_tag(self.cluster_tags)
        if (self.type == "python"):
            file_name = "python/{}.py".format(self.task_file_name)
        else:
            file_name = "shell/{}.sh".format(self.task_file_name)
        command = "#!/bin/sh\n" + command
        artifact_home = ConstantConfigUtil.get_string('artifact_home', '{},{}'.format(self.provider,self.cluster_region))
        cloud_path = "{}/{}".format(artifact_home.rstrip("/"), file_name)
        cloud = CloudUtil(provider=self.cluster_region,region=self.provider)
        cloud.put_content_to_cloud(cloud_path,command)

    def upload_command(self):
        region, provider = NormalUtil.parse_cluster_tag(self.cluster_tags)
        if(self.type=="python"):
            file_name = "python/{}.py".format(self.task_file_name)
        else:
            file_name = "shell/{}.sh".format(self.task_file_name)
        artifact_home = ConstantConfigUtil.get_string('artifact_home','{},{}'.format(provider,region))
        file = "{}/{}".format(artifact_home.rstrip("/"),file_name)
        return file

    def my_init(self):
        self.tenant_id = TaskDesc.get_tenant_id(self.dag_id)
        self.cluster_region, self.provider = NormalUtil.parse_cluster_tag(self.cluster_tags)
        if self.cluster_region == 'us-east-1':
            self.cluster_tags = 'type:k8s,provider:aws,region:us-east-1,sla:normal,rbac.cluster:ads-prod'
        self.command_tags = 'type:kubernetes-pod-operator'
        if self.command and self.is_psql:
            # psql任务在这里拼装一下command
            if '/*zheshiyigebianmabiaoshi*/' in self.command:
                tmpList = self.command.split('/*zheshiyigebianmabiaoshi*/')
                if len(tmpList) == 3:
                    command = " ".join(tmpList)
                    self.scripts = ["psql {}".format(command)]
        self.build_cmds()
        self.resources = self.get_resources(self.resources) if isinstance(self.resources,dict) else {}
        from airflow.contrib.kubernetes.volume import Volume
        from airflow.contrib.kubernetes.volume_mount import VolumeMount

        vol_list = [Volume(name="script-job-shell-configmap",
                           configs={"configMap": {"name": "script-job-shell-configmap"}}),Volume(name="workdir",configs={"emptyDir":{}})]
        vm_list = [VolumeMount(name="script-job-shell-configmap", mount_path="/work/script_job_shell.py",
                               sub_path="script_job_shell.py", read_only=False),VolumeMount(name="workdir",mount_path="/work",sub_path=None,read_only=False)]

        region, provider = NormalUtil.parse_cluster_tag(self.cluster_tags)

        command = [
            "python",
            "/work/script_job_shell.py"
        ]


        for file in self.files:
            command.append("-d {} ".format(file))
        command.append("-d {} ".format(self.upload_command()))
        command.append("-w /work ")

        initImage = ConstantConfigUtil.get_string(_type='script_base_image',key='{},{}'.format(provider,region))

        init_containers_list = [{
            "name":"initenvironment",
            "image":"{}".format(initImage),
            "command":command,
            "volumeMounts":[
                {
                    "mountPath":"/work/script_job_shell.py",
                    "name":"script-job-shell-configmap",
                    "subPath":"script_job_shell.py"
                },
                {
                    "name":"workdir",
                    "mountPath":"/work"
                }
            ]
        }]
        try:
            if isinstance(self.volumes, list):
                for tmp_v in self.volumes:
                    name = None
                    configs = {}
                    for k in tmp_v:
                        if k == "name":
                            name = tmp_v[k]
                        else:
                            configs[k] = tmp_v[k]
                    vol_list.append(Volume(name=name, configs=configs))
            if isinstance(self.volume_mounts, list):
                for tmp_vm in self.volume_mounts:
                    name = tmp_vm["name"] if "name" in tmp_vm.keys() else None
                    mount_path = tmp_vm["mount_path"] if "mount_path" in tmp_vm.keys() else None
                    sub_path = tmp_vm["sub_path"] if "sub_path" in tmp_vm.keys() else None
                    read_only = tmp_vm["read_only"] if "read_only" in tmp_vm.keys() else False
                    read_only = False if read_only in ["False", "false", False, ""] else True
                    vm_list.append(VolumeMount(name=name, mount_path=mount_path,
                                               sub_path=sub_path, read_only=read_only))
            if isinstance(self.init_containers,list):
                init_containers_list += self.init_containers
        except Exception as e:
            print ("Volume or VolumeMount failed:" + str(e))
        self.volumes = vol_list
        self.volume_mounts = vm_list
        self.init_containers = init_containers_list
        self.env_vars = self.env_vars if isinstance(self.env_vars, dict) else {}
        self.labels = self.labels if isinstance(self.labels, dict) else {}
        self.annotations = self.annotations if isinstance(self.annotations, dict) else {}
        if "iam.shareit.me/huawei" not in self.annotations.keys():
            # hard code inject DataStudio permissions
            self.annotations["iam.shareit.me/huawei"] = "dev-datastudio-big-authority"
        if "iam.shareit.me/source" not in self.annotations.keys():
            self.annotations["iam.shareit.me/source"] = "shareid"
        if "iam.shareit.me/type" not in self.annotations.keys():
            self.annotations["iam.shareit.me/type"] = "env"
        self.node_selectors = self.node_selectors if isinstance(self.node_selectors, dict) else {}
        ## 美东上线了一批arm机器，要避免提交到上面
        if self.cluster_region == 'us-east-1' and not self.node_selectors:
            self.node_selectors['workload'] = 'general'
        self.configmaps = self.configmaps if isinstance(self.configmaps, list) else []
        self.arguments = self.arguments if isinstance(self.arguments, list) else []
        self.tolerations = self.tolerations if isinstance(self.tolerations, list) else []
        self.name = self.task_id.replace("_", "-").lower() if self.name is None or self.name == "" else self.name
        self.service_account_name = str(conf.get('kubernetes', 'spark_service_account_name')) if self.service_account_name is None or self.service_account_name == "" else self.service_account_name
        self.kind = "Pod" if self.kind is None or self.kind =="" else self.kind
        self.api_version = "v1" if self.api_version is None or self.api_version =="" else self.api_version
