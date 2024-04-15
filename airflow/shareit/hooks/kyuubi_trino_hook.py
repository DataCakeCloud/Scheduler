# -*- coding: utf-8 -*-
import base64
import json
import time
import traceback
import jaydebeapi
import six
import requests
import subprocess
import pymysql
from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException


class KyuubiTrinoHook(BaseHook):
    """
    任务提交给datacake - gateway。 gateway是基于kyuubi实现的
    """

    def __init__(self,
                 conn_id='kyuubi_jdbc',
                 batch_type=None,
                 ):
        """
        :param command: 批处理命令
        :type command: str or list[str]
        :param conn_id: connection_id string
        :type conn_id: str
        """
        self._conn = self.get_connection(conn_id)
        self.host = self._conn.host
        self.batch_type = batch_type
        self.body = {}
        self.batch_id = ''
        self.jdbc = 'org.apache.kyuubi.jdbc.KyuubiHiveDriver'

    def submit_job(self,
                   ti,
                   batch_type=None,
                   job_name=None,
                   command=None,
                   conf=None,
                   owner=None
                   ):

        self.owner = owner
        self.command = command
        self.driverPath = "/tmp/kyuubi-hive-jdbc-shaded-1.7.0.jar"
        if ti:
            ex_conf = ti.get_external_conf()
            ex_conf = ti.format_external_conf(ex_conf)
            ex_conf['is_kyuubi'] = True
            if ex_conf is not None:
                ti.save_external_conf(ex_conf=ex_conf)
        # k8s,region:sg2,sla:normal
        if "ue1" in conf['kyuubi.session.cluster.tags'] or "us-east-1" in conf['kyuubi.session.cluster.tags']:
            self.tags = "region:ue1,name:aws"
            self.mysql_conn = "tableau_jdbc_ue1"
        elif "sg1" in conf['kyuubi.session.cluster.tags'] or "ap-southeast-1" in conf['kyuubi.session.cluster.tags']:
            self.tags = "region:sg1,name:aws"
            self.mysql_conn = "tableau_jdbc_sg1"
        else:
            self.tags = "region:sg2,name:huawei"
            self.mysql_conn = "tableau_jdbc_sg2"
        conf['kyuubi.session.cluster.tags'].split(",")
        self.log.info("trino job start execute ...............")
        return self.execute()

    def execute(self):
        host = self._conn.host
        port = self._conn.port
        password = self._conn.password
        url = 'jdbc:hive2://{host}:{port}/;auth=noSasl;user={owner}?kyuubi.engine.type=trino;kyuubi.session.cluster.tags={tags};kyuubi.session.engine.trino.connection.catalog=hive;kyuubi.engine.share.level=CONNECTION;'\
            .format(host=host,port=port,owner=self.owner,tags=self.tags)
        self.log.info('request url: {}'.format(url))
        cmds = self.command.split(";")
        try:
            conn = jaydebeapi.connect(self.jdbc, url, [self.owner, password], self.driverPath)
        except Exception as e:
            self.log.info('connection  failed :\n{}'.format(str(e)))
            return "FAILED"


        try:
            mysql_conn = self.get_connection(self.mysql_conn)
            ds_conn = pymysql.connect(host=mysql_conn.host, port=mysql_conn.port, user=mysql_conn.login,
                                      passwd=mysql_conn.password)
            with ds_conn.cursor() as cur:
                cur.execute(cmds[0])
                ds_conn.commit()
        except Exception as e:
            self.log.error("[DataStudio pipeline] trino job execute failed: " + str(e))
            return "FIALED"

        try:
            with conn.cursor() as cur:
                cur.execute(cmds[1])
                result = cur.fetchall()
        except Exception as e:
            self.log.error("[DataStudio pipeline] trino job execute failed: " + str(e))
            return "FIALED"
        conn.close()
        self.log.info('task execute result :\n{}'.format(result))
        self.log.info('Job submit success')
        return "FINISHED"

