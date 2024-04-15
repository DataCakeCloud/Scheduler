# -*- coding: utf-8 -*-
import base64
import urllib

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.shareit.constant.kyuubi_state import KyuubiState
from airflow.shareit.hooks.datacake_qe_hook import DatacakeQeHook
from airflow.shareit.hooks.kyuubi_hook import KyuubiHook
from airflow.shareit.models.task_desc import TaskDesc
from airflow.shareit.utils.constant_config_util import ConstantConfigUtil
from airflow.shareit.utils.normal_util import NormalUtil
from airflow.shareit.utils.parse_args_util import ParseArgs
from airflow.shareit.utils.spark_util import SparkUtil
from airflow.utils.decorators import apply_defaults
import zlib

class DatacakeQeOperator(BaseOperator):
    """
    """

    template_fields = ('command',)
    ui_color = '#f0ed04'

    @apply_defaults
    def __init__(self,
                 job_name=None,
                 command=None,
                 cluster_tags=None,
                 command_tags=None,
                 *args, **kwargs):
        super(DatacakeQeOperator, self).__init__(*args, **kwargs)
        self.job_name = job_name or kwargs['task_id']
        self.command = command
        self.command_tags = command_tags
        self.cluster_tags = cluster_tags
        self.cluster_region = ''
        self.provider = ''
        self.task_desc = None

    def execute(self, context):
        ti = context['ti'] if self.task_id == context['ti'].dag_id else None
        owner = context['owner']
        self.hook = DatacakeQeHook(owner=owner)
        self.encode_command_with_url_base64()
        status = self.hook.submit_job(ti, self.command, self.task_desc.ds_task_id, self.task_desc.ds_task_name,
                                      self.emails.split(','),
                                      self.task_desc.granularity,
                                      user_group=self.task_desc.user_group)
        if status.upper() != 'SUCCESS':
            raise AirflowException('job failed')

    def my_init(self):
        # 获取batch_type
        self.cluster_region, self.provider = NormalUtil.parse_cluster_tag(self.cluster_tags)
        self.tenant_id = TaskDesc.get_tenant_id(self.dag_id)


    def encode_command_with_url_base64(self):
        tmpList = self.command.split('/*datacakebianma*/')
        if len(tmpList) == 3:
            tmpList[1] = base64.b64encode(urllib.quote(tmpList[1]))
            self.command = " ".join(tmpList)

    def on_kill(self):
        self.hook.kill_job()
