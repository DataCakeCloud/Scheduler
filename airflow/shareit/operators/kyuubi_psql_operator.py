# -*- coding: utf-8 -*-
import sys
import boto3
from airflow.contrib.kubernetes.pod import Resources
from airflow.shareit.operators.kyuubi_script_operator import KyuubiScriptOperator
from airflow.shareit.utils.obs_util import Obs

reload(sys)
sys.setdefaultencoding('utf-8')
from airflow.utils import apply_defaults


class KyuubiPsqlOperator(KyuubiScriptOperator):
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
                 *args,
                 **kwargs):
        super(KyuubiPsqlOperator, self).__init__(task_id=task_id, *args, **kwargs)
        self.image = '4'
        self.type = 'shell'
        self.image_pull_policy = 'IfNotPresent'
        self.env_vars = {"PGPASSWORD":"etqonlixeFjko6dwikvnpjyc6ahxImlf"}
        self.namespace = 'bdp'
        self.command_tags = 'type:kubernetes-pod-operator'
        self.command = command
        self.is_psql = True
        self.scripts = []
        self.files = []
