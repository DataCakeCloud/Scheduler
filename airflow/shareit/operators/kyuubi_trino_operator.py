# -*- coding: utf-8 -*-
import base64
import re

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.shareit.constant.kyuubi_state import KyuubiState
from airflow.shareit.hooks.kyuubi_trino_hook import KyuubiTrinoHook
from airflow.shareit.models.task_desc import TaskDesc
from airflow.shareit.utils.normal_util import NormalUtil
from airflow.utils.decorators import apply_defaults
import zlib

class KyuubiTrinoOperator(BaseOperator):
    """
    """

    template_fields = ('command',)
    ui_color = '#f0ed04'

    @apply_defaults
    def __init__(self,
                 batch_type=None,
                 job_name=None,
                 command=None,
                 cluster_tags=None,
                 command_tags=None,
                 *args, **kwargs):
        super(KyuubiTrinoOperator, self).__init__(*args, **kwargs)
        self.batch_type = batch_type
        self.job_name = job_name or kwargs['task_id']
        self.command = command
        self.command_tags = command_tags
        self.cluster_tags = cluster_tags

    def execute(self, context):
        self.kyuubi_trino_hook = KyuubiTrinoHook()
        ti = None
        owner = ''
        class_name = None
        conf = {}
        args = []
        if isinstance(context, dict) and "ti" in context.keys():
            ti = context['ti'] if self.task_id == context['ti'].dag_id else None
        if isinstance(context, dict) and "owner" in context.keys():
            owner = context['owner']

        self.encode_command_with_base64()
        conf['kyuubi.session.cluster.tags'] = self.cluster_tags
        status = self.kyuubi_trino_hook.submit_job(ti=ti,
                                             owner=owner,
                                             batch_type=self.batch_type,
                                             job_name=self.job_name,
                                             conf=conf,
                                             command=self.command,
                                             )

        if status.upper() != KyuubiState.KYUUBI_FINISHED:
            raise AirflowException('Genie job failed')

    def my_init(self):
        # 获取batch_type
        self.batch_type = 'trino'
        self.cluster_region, self.provider = NormalUtil.parse_cluster_tag(self.cluster_tags)
        self.tenant_id = TaskDesc.get_tenant_id(self.dag_id)

    def get_region(self):
        pattern = re.compile("region:([^,:]+)")
        search_region = re.search(pattern, self.cluster_tags)
        if search_region:
            region = search_region.group(1)
            self.region = region
        else:
            self.region = None


    def encode_command_with_base64(self):
        tmpList = self.command.split('/*zheshiyigebianmabiaoshi*/')
        if len(tmpList) == 3:
           self.command = tmpList[1]


    def on_kill(self):
        self.kyuubi_hook.kill_job()
