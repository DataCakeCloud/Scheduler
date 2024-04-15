# -*- coding: utf-8 -*-
import base64
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.shareit.constant.kyuubi_state import KyuubiState
from airflow.shareit.hooks.kyuubi_hook import KyuubiHook
from airflow.shareit.hooks.kyuubi_jdbc_hook import KyuubiJdbcHook
from airflow.shareit.models.task_desc import TaskDesc
from airflow.shareit.utils.constant_config_util import ConstantConfigUtil
from airflow.shareit.utils.normal_util import NormalUtil
from airflow.shareit.utils.parse_args_util import ParseArgs
from airflow.shareit.utils.spark_util import SparkUtil
from airflow.shareit.utils.uid.uuid_generator import UUIDGenerator
from airflow.utils.decorators import apply_defaults
import zlib

class KyuubiHiveOperator(BaseOperator):
    template_fields = ('command',)
    ui_color = '#f0ed04'

    @apply_defaults
    def __init__(self,
                 batch_type=None,
                 job_name=None,
                 command=None,
                 cluster_tags=None,
                 command_tags=None,
                 default_db=None,
                 *args, **kwargs):
        super(KyuubiHiveOperator, self).__init__(*args, **kwargs)
        self.batch_type = batch_type
        self.job_name = job_name or kwargs['task_id']
        self.command = command
        self.command_tags = command_tags
        self.cluster_tags = cluster_tags
        self.cluster_region = ''
        self.provider = ''
        self.default_db = default_db
        self.task_desc = None

    def execute(self, context):
        # self.kyuubi_hook = KyuubiHook()
        batch_id = UUIDGenerator().generate_uuid_no_index()
        self.hook = KyuubiJdbcHook(batch_id=batch_id)
        ti = None
        owner = ''
        conf = {}
        if isinstance(context, dict) and "ti" in context.keys():
            ti = context['ti'] if self.task_id == context['ti'].dag_id else None
        if isinstance(context, dict) and "owner" in context.keys():
            owner = context['owner']

        self.encode_command()
        user_group = self.task_desc.user_group
        conf['kyuubi.session.group'] = user_group.get('currentGroup', '')
        conf['kyuubi.session.groupId'] = user_group.get('uuid','')
        conf['kyuubi.session.tenant'] = user_group.get('tenantName','')
        conf['kyuubi.session.cluster.tags'] = self.cluster_tags
        conf['kyuubi.engine.jdbc.connection.database'] = self.default_db
        conf['kyuubi.engine.type'] = 'JDBC'
        conf['kyuubi.engine.jdbc.connection.provider'] = 'HiveConnectionProvider'
        conf['catalog.probe.taskName'] = self.task_desc.ds_task_name
        conf['catalog.probe.taskId'] = str(self.task_desc.ds_task_id)
        conf['catalog.probe.from'] = 'DE'

        if hasattr(self,'submit_uuid') and self.submit_uuid:
            conf['kyuubi.batch.id'] = self.submit_uuid

        status = self.hook.submit_job(ti=ti,
                                             owner=owner,
                                             batch_type=self.batch_type,
                                             conf=conf,
                                             default_db=self.default_db,
                                             sql=self.command,
                                             region='{},{}'.format(self.provider,self.cluster_region)
                                             )

        if status.upper() != KyuubiState.KYUUBI_FINISHED:
            raise AirflowException('Kyuubi job failed')

    def my_init(self):
        # 获取batch_type
        self.batch_type = 'hive'
        self.cluster_region, self.provider = NormalUtil.parse_cluster_tag(self.cluster_tags)
        self.tenant_id = TaskDesc.get_tenant_id(self.dag_id)

    def encode_command(self):
        tmpList = self.command.split('/*zheshiyigebianmabiaoshi*/')
        if len(tmpList) == 3:
           self.command = tmpList[1]
           self.default_db = tmpList[0].strip()

    def _insert_space(self, line=None, interval=None):
        if interval is None:
            interval = 1024
        tmpList = [line[i:i + interval] for i in range(0, len(line), interval)]
        return " ".join(tmpList)

    def on_kill(self):
        self.hook.kill_job()
