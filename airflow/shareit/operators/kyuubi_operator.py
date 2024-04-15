# -*- coding: utf-8 -*-
import base64
import urllib

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.shareit.constant.kyuubi_state import KyuubiState
from airflow.shareit.hooks.kyuubi_hook import KyuubiHook
from airflow.shareit.models.task_desc import TaskDesc
from airflow.shareit.utils.constant_config_util import ConstantConfigUtil
from airflow.shareit.utils.normal_util import NormalUtil
from airflow.shareit.utils.parse_args_util import ParseArgs
from airflow.shareit.utils.spark_util import SparkUtil
from airflow.utils.decorators import apply_defaults
import zlib

class KyuubiOperator(BaseOperator):
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
        super(KyuubiOperator, self).__init__(*args, **kwargs)
        self.batch_type = batch_type
        self.job_name = job_name or kwargs['task_id']
        self.command = command
        self.command_tags = command_tags
        self.cluster_tags = cluster_tags
        self.cluster_region = ''
        self.provider = ''
        self.task_desc = None

    def execute(self, context):
        self.kyuubi_hook = KyuubiHook()
        ti = None
        owner = ''
        class_name = None
        conf = {}
        args = []
        if isinstance(context, dict) and "ti" in context.keys():
            ti = context['ti'] if self.task_id == context['ti'].dag_id else None
        if isinstance(context, dict) and "owner" in context.keys():
            owner = context['owner']
        if 'tf-operator-cli' in self.command_tags:
            resource = self.dependencies
            arg_str = self.command.split('.yaml')[1]
            args = ParseArgs.parse(arg_str)
            self.batch_type = 'tfjob'
        else:
            self.encode_command_with_base64()
            self.encode_command_with_url_base64()
            class_name, resource, conf, args = self.parse_command()

        user_group = self.task_desc.user_group
        for k, v in conf.items():
            if k == 'spark.sql.default.dbName':
                conf['kyuubi.engine.jdbc.connection.database'] = v
                break
        conf['kyuubi.session.group'] = user_group.get('currentGroup', '')
        conf['kyuubi.session.groupId'] = user_group.get('uuid','')
        conf['kyuubi.session.tenant'] = user_group.get('tenantName','')
        conf['kyuubi.session.cluster.tags'] = self.cluster_tags
        conf['kyuubi.session.command.tags'] = self.command_tags
        conf['kyuubi.session.retry.times'] = ti.try_number if ti else 1
        conf['spark.hadoop.lakecat.client.userName'] = user_group.get('uuid','')
        conf['spark.hadoop.catalog.probe.taskName'] = self.task_desc.ds_task_name
        conf['spark.hadoop.catalog.probe.taskId'] = str(self.task_desc.ds_task_id)
        conf['spark.hadoop.catalog.probe.from'] = 'DE'

        if hasattr(self,'submit_uuid') and self.submit_uuid:
            conf['kyuubi.batch.id'] = self.submit_uuid

        status = self.kyuubi_hook.submit_job(ti=ti,
                                             owner=owner,
                                             batch_type=self.batch_type,
                                             job_name=self.job_name,
                                             class_name=class_name,
                                             conf=conf,
                                             args=args,
                                             resource=resource,
                                             region='{},{}'.format(self.provider,self.cluster_region),
                                             group_uid=user_group.get('uuid','')
                                             )

        if status.upper() != KyuubiState.KYUUBI_FINISHED:
            raise AirflowException('Genie job failed')

    def my_init(self):
        # 获取batch_type
        self.batch_type = 'spark'
        self.cluster_region, self.provider = NormalUtil.parse_cluster_tag(self.cluster_tags)
        self.tenant_id = TaskDesc.get_tenant_id(self.dag_id)

    def parse_command(self):
        class_name, resource, conf, args = SparkUtil.spark_param_parsed(self.command)
        if 'type:spark-submit-sql-ds' in self.command_tags:
            resource = ConstantConfigUtil.get_string(_type='spark_sql_jar_location',key='{},{}'.format(self.provider,self.cluster_region))
            class_name = ConstantConfigUtil.get_string(_type='spark_sql_jar_class_name',key='spark_sql_jar_class_name')

        return class_name, resource, conf, args

    def encode_command_with_url_base64(self):
        tmpList = self.command.split('/*datacakebianma*/')
        if len(tmpList) == 3:
            tmpList[1] = base64.b64encode(urllib.quote(tmpList[1]))
            self.command = "/*datacakebianma*/".join(tmpList)

    def encode_command_with_base64(self):
        tmpList = self.command.split('/*zheshiyigebianmabiaoshi*/')
        if len(tmpList) == 3:
            if "psql" in self.command_tags:
                self.command = " ".join(tmpList)
            else:
                compressed = zlib.compress(tmpList[1])
                base64Code = base64.b64encode(compressed)
                base64Code = self._insert_space(line=base64Code, interval=1024)
                tmpList[1] = base64Code
                self.command = " ".join(tmpList)

    def _insert_space(self, line=None, interval=None):
        if interval is None:
            interval = 1024
        tmpList = [line[i:i + interval] for i in range(0, len(line), interval)]
        return " ".join(tmpList)

    def on_kill(self):
        self.kyuubi_hook.kill_job()
