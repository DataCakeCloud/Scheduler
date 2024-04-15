# -*- coding: utf-8 -*-
import base64
import datetime
import json
import os
import time
import traceback

import six
import requests
import subprocess

from airflow.configuration import conf

from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException
from airflow.shareit.utils.alarm_adapter import send_email_by_notifyAPI



class DatacakeQeHook(BaseHook):
    def __init__(self,
                 conn_id='api_gateway_default',
                 owner=None,
                 ):
        self._conn = self.get_connection(conn_id)
        self.host = self._conn.host
        self.body = {}
        self.batch_id = ''
        self.owner = owner

    def submit_job(self, ti, sql, task_id, task_name, emails, granularity,user_group=None):
        self._set_header(user_group)
        self.body = {'querySql':sql.strip(),'taskId':task_id}
        self.execute()
        if self.state == 'success' and self.download_url:
            content = '您的数据分析任务已经运行成功。' \
                  '任务名称：{}，' \
                  '任务实例日期：{}。  <p><a href="{}" target="_blank">点击此处下载</a></p>'.format(task_name,ti.execution_date.strftime('%Y-%m-%d %H:%M:%S'),self.download_url)
            title = 'DataCake数据分析任务【{}_{}】运行成功'.format(task_name,ti.execution_date.strftime('%Y-%m-%d %H:%M:%S')) if str(granularity) == '7'  else 'DataCake数据分析任务【{}】运行成功'.format(task_name)
            send_email_by_notifyAPI(title=title,content=content,receiver=emails)
        return self.state

    def execute(self):
        host = self._conn.host
        url = '{}/qe/cronQuery/execute'.format(host)
        self.log.info('request header: {}'.format(json.dumps(self.header)))
        self.log.info('request body: {}'.format(json.dumps(self.body)))
        resp = None
        for i in range(30):
            try:
                resp = requests.post(url, json=self.body,headers=self.header)
                break
            except Exception as e:
                self.log.info("qe服务连接失败，重试次数：{}".format(i))
                self.log.error("qe服务连接失败，error:{}".format(str(e)))
                time.sleep(20)
        if not resp:
            raise AirflowException("Cannot connect to qe server")
        status_code = resp.status_code
        if status_code != 200:
            raise AirflowException("POST cronQuery/execute failed,resp :{}".format(resp.content) )
        resp_json = resp.json()
        self.log.info('response: {}'.format(json.dumps(resp_json)))
        if resp_json.get('code') != 0 :
            self.log.info('qe调用失败,错误:{}'.format(resp_json.get('message','')))
            self.state = 'failed'
            return
        else :
            self.state = 'success'

        self.download_url = resp_json.get('downloadUrl','')
        self.log.info('Job submit success, downloadUrl : {}'.format(self.download_url))


    def _set_header(self,user_group):

        token = conf.get('core', 'datacake_token')

        self.header = {
            'datacake_token': token,
            'current_login_user': '{{"userId":"{}","tenantName":"{}","tenantId":{} }}'.format(
                self.owner,user_group.get('tenantName', ''), user_group.get('tenantId', '')),
            'currentGroup': user_group.get('currentGroup', ''),
            'groupId': user_group.get('groupId', ''),
            'uuid': user_group.get('uuid', '')
        }


    def kill_job(self):
        pass

