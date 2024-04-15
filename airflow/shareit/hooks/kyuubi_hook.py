# -*- coding: utf-8 -*-
import base64
import json
import os
import time
import traceback

import six
import requests
import subprocess

from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException, AirflowTaskTimeout
from airflow.shareit.constant.kyuubi_state import KyuubiState
from airflow.shareit.utils.cloud.cloud_utli import CloudUtil
from airflow.shareit.utils.s3_util import S3
from airflow.shareit.utils.state import State
from airflow.configuration import conf as ds_conf
from airflow.shareit.utils.uid.uuid_generator import UUIDGenerator

SPARK = 'spark'
GET_LOG = 'get_log'
KILL_JOB = 'kill_job'
GET_INFO = 'get_info'
LOCAL_LOG = '{dag_folder}/{dag_id}/{task_id}/{time}/{batch_id}'
CLOUD_TASK_LOG = '{log_cloud_home}/BDP/logs/pipeline_log/{time}/{batch_id}.txt'



class KyuubiHook(BaseHook):
    """
    任务提交给datacake - gateway。 gateway是基于kyuubi实现的
    """

    def __init__(self,
                 conn_id='kyuubi_default',
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

    def submit_job(self,
                   ti,
                   batch_type=None,
                   resource=None,
                   job_name=None,
                   class_name=None,
                   username=None,
                   executor=None,
                   conf=None,
                   args=None,
                   owner=None,
                   region=None,
                   group_uid=None
                   ):
        self.owner = owner
        self._set_header(group_uid)
        self.get_spark_body(resource=resource, batch_type=batch_type, class_name=class_name, job_name=job_name,
                            conf=conf, args=args)

        if ti:
            ex_conf = ti.get_external_conf()
            ex_conf = ti.format_external_conf(ex_conf)
            ex_conf['is_kyuubi'] = True
            if ex_conf is not None:
                ti.save_external_conf(ex_conf=ex_conf)


        log_provider = conf.get('job_log', 'log_provider')
        log_region = conf.get('job_log', 'log_region')
        cloud_object = CloudUtil(provider=log_provider, region=log_region)
        self.execute(ti,cloud_object,ex_conf)
        dag_folder = ds_conf.get('core', 'base_log_folder')
        # log_provider = conf.get('core', 'log_provider')
        # log_region = conf.get('core', 'log_region')
        # cloud_object = CloudUtil(provider=log_provider, region=log_region)
        # 本地文件
        app_log_file = LOCAL_LOG.format(dag_folder=dag_folder,
                                        dag_id=ti.dag_id, task_id=ti.task_id,
                                        time=ti.execution_date.isoformat(),
                                        batch_id=self.batch_id)
        # 上传到云存储的桶
        log_cloud_home = ds_conf.get('job_log', 'log_cloud_file_location').strip()
        # 云存储的文件
        log_cloud_file = CLOUD_TASK_LOG.format(log_cloud_home=log_cloud_home,
                                               time=ti.start_date.strftime(
                                                   '%Y%m%d'),
                                               batch_id=self.batch_id)
        self.log.info('local file location: {}'.format(app_log_file))
        self.log.info('cloud file location: {}'.format(log_cloud_file))

        # 先上传一次文件，防止用户打开链接时没有数据就变成下载文件了
        with open(app_log_file, 'a') as f:
            f.write("Task starting...")
        cloud_object.upload_with_content_type(log_cloud_file, app_log_file,content_type='text/plain')

        # 将batchid加入到taskinstance中
        try:
            if ti:
                ex_conf = ti.get_external_conf()
                ex_conf = ti.format_external_conf(ex_conf)
                ex_conf['batch_id'] = self.batch_id
                ex_conf['is_kyuubi'] = True
                ex_conf['region'] = region
                ex_conf['new_log'] = True
                ex_conf['is_tfjob'] = True if batch_type == 'tfjob' else False
                ex_conf['is_spark'] = True if batch_type == 'spark' else False
                if ex_conf is not None and isinstance(ex_conf, dict) and "callback_info" in ex_conf.keys():
                    ti.save_external_conf(ex_conf=ex_conf)
                    # 回调，把batch_id传回其他系统
                    try:
                        from airflow.utils.state import State
                        from airflow.shareit.utils.callback_util import CallbackUtil
                        callback_info = ex_conf['callback_info']
                        url = callback_info['callback_url']
                        data = {"callback_id": callback_info['callback_id'], "state": State.RUNNING,
                                "instance_id": self.batch_id, "args": callback_info['extra_info']}
                        CallbackUtil.callback_post_guarantee(url, data, ti.dag_id, ti.execution_date,
                                                             self.batch_id)
                    except (AirflowTaskTimeout,AirflowException):
                        raise
                    except Exception as e:
                        self.log.error("[DataStudio pipeline] Callback ti state request failed: " + str(e))
                else:
                    ti.save_external_conf(ex_conf=ex_conf)
        except (AirflowTaskTimeout,AirflowException):
            raise
        except Exception as e:
            self.log.error("Can not save kyuubi job id " + str(e))

        log_offset = 0
        while True:
            err = None
            state = None
            log_offset = self.save_app_log(log_offset, app_log_file, log_cloud_file, cloud_object=cloud_object)
            self.save_webui_and_sparkid(ti,batch_type)
            time.sleep(5) #减少调用频率
            # kyuubi服务出现问题导致连接不上，5分钟内连接不到，退出
            for i in range(30):
                try:
                    err = None
                    state = self.get_job_state(batch_type)
                    break
                except (AirflowTaskTimeout,AirflowException):
                    raise
                except Exception as e:
                    self.log.info("获取kyuubi任务状态失败，重试次数：{}".format(i))
                    self.log.error("Can not get kyuubi job info  " + str(e))
                    err = e
                    time.sleep(20)
            if err:
                raise AirflowException("Cannot get job info ,batch_id:{},error:{}".format(self.batch_id,str(err)))

            if state in KyuubiState.finish_state:
                time.sleep(15)
                self.save_app_log(log_offset, app_log_file, log_cloud_file, cloud_object=cloud_object, overwrite=True)
                self.log.info('Job finished , state : {}'.format(state))
                break
        return state

    def get_spark_body(self,
                       resource=None,
                       batch_type=None,
                       class_name=None,
                       job_name=None,
                       conf=None,
                       args=None):

        # ldap_user = ds_conf.get('kyuubi','ldap_user')
        # ldap_pswd = ds_conf.get('kyuubi','ldap_pswd')
        # if conf :
        #     conf['kyuubi.session.ldap.userName'] = ldap_user
        #     conf['kyuubi.session.ldap.password'] = ldap_pswd
        # else :
        #     conf = {'kyuubi.session.ldap.userName':ldap_user,'kyuubi.session.ldap.password':ldap_pswd}

        self.body['resource'] = resource
        self.body['name'] = job_name
        self.body['batchType'] = batch_type
        self.body['conf'] = conf
        self.body['args'] = args

        if batch_type == 'spark':
            if not class_name:
                self.body['batchType'] = 'pyspark'
            else:
                self.body['batchType'] = batch_type
            self.body['className'] = class_name


    def execute(self,ti,cloud_object,ex_conf):
        host = self._conn.host
        url = '{}/api/v1/batches'.format(host)
        self.log.info('request header: {}'.format(self.header))
        self.log.info('request body: {}'.format(json.dumps(self.body)))
        resp = None
        for i in range(30):
            try:
                resp = requests.post(url, json=self.body,headers=self.header)
                break
            except Exception as e:
                self.log.info("kyuubi服务连接失败，重试次数：{}".format(i))
                self.log.error("kyuubi服务连接失败，error:{}".format(str(e)))
                time.sleep(20)
        if not resp:
            if resp != None:
                self.batch_id = UUIDGenerator().generate_uuid_no_index()
                ex_conf['batch_id'] = self.batch_id
                ti.save_external_conf(ex_conf=ex_conf)
                dag_folder = ds_conf.get('core', 'base_log_folder')
                app_log_file = LOCAL_LOG.format(dag_folder=dag_folder,
                                                dag_id=ti.dag_id, task_id=ti.task_id,
                                                time=ti.execution_date.isoformat(),
                                                batch_id=self.batch_id)
                # 上传到云存储的桶
                log_cloud_home = ds_conf.get('job_log', 'log_cloud_file_location').strip()
                # 云存储的文件
                log_cloud_file = CLOUD_TASK_LOG.format(log_cloud_home=log_cloud_home,
                                                       time=ti.start_date.strftime(
                                                           '%Y%m%d'),
                                                       batch_id=self.batch_id)
                # 先上传一次文件，防止用户打开链接时没有数据就变成下载文件了
                with open(app_log_file, 'a') as f:
                    f.write(resp.content)
                cloud_object.upload_with_content_type(log_cloud_file, app_log_file, content_type='text/plain')
                raise AirflowException("POST api/v1/batches failed,resp :{}".format(resp.content))
            else :
                raise AirflowException("Cannot connect to kyuubi server")
        status_code = resp.status_code
        if status_code != 200:
            raise AirflowException("POST api/v1/batches failed,resp :{}".format(resp.content))
        resp_json = resp.json()
        batch_id = resp_json['id']
        self.log.info('Job submit success, batch_id : {}'.format(batch_id))
        self.batch_id = batch_id

    def _get_url(self, type):
        url = '{host}/api/v1/batches/{batch_id}'.format(host=self.host, batch_id=self.batch_id)
        if type == GET_LOG:
            return url + '/localLog'
        if type == GET_INFO:
            return url
        if type == KILL_JOB:
            return url
    def _set_header(self,group_uid):
        if self.owner:
            self.header = {'Authorization':'Basic '+ base64.b64encode(group_uid)}
        else:
            self.header = {}


    def get_log(self, index, size):
        url = self._get_url(GET_LOG)
        params = {'from': index, 'size': size}
        resp = requests.get(url, params,headers=self.header)
        resp_json = resp.json()
        return resp_json['logRowSet'], resp_json['rowCount']

    def get_job_info(self):
        url = self._get_url(GET_INFO)
        resp = requests.get(url,headers=self.header)
        if resp.status_code != 200:
            raise ValueError("status_code :{}".format(resp.status_code))
        resp_json = resp.json()
        return resp_json

    def kill_job(self):
        url = self._get_url(KILL_JOB)
        for _ in range(3):
            requests.delete(url,headers=self.header)

    def get_job_state(self,batch_type):
        job_info = self.get_job_info()
        app_id = job_info['appId']
        self.app_id = app_id
        state = job_info['state']
        appState = job_info['appState']
        if batch_type == 'spark':
            # 当不是tfjob任务时
            # 当state为FINISHED，appState不为FINISHED时，认为任务失败
            if state == KyuubiState.KYUUBI_FINISHED and appState != KyuubiState.KYUUBI_FINISHED:
                self.log.info('Job state:{},appState:{}'.format(state, appState))
                state = KyuubiState.KYUUBI_ERROR
        return state

    def save_webui_and_sparkid(self,ti,batch_type):
        '''
            保存webui和sparkid到ti的external_conf中
        '''
        try:
            if batch_type != 'spark':
                return
            ex_conf = ti.get_external_conf()
            if 'app_id' in ex_conf.keys() and 'app_url' in ex_conf.keys():
                return
            job_info = self.get_job_info()
            app_id = job_info['appId']
            app_url = job_info['appUrl']
            if app_id and app_url:
                ex_conf['app_id'] = app_id
                ex_conf['app_url'] = app_url

                ti.save_external_conf(ex_conf=ex_conf)
        except:
            self.log.error('save webui and sparkid failed')
            self.log.error(traceback.format_exc())



    def save_app_log(self, offset, local_log_file, cloud_file, cloud_object,overwrite=False):
        '''
            通过接口获取日志，如果获取为空则退出
        '''
        # if self.batch_type == 'hive' or self.batch_type == 'spark':
        #     # hive/spark任务不需要保存日志
        #     return
        size = 2000
        try:
            if overwrite:
                with open(local_log_file, 'w') as f:
                    f.write("Task starting...")
                offset = 0
                size = 5000

            with open(local_log_file, 'a') as f:
                last_time = time.time()
                while True:
                    log_arr, row_cnt = self.get_log(index=offset, size=size)
                    offset += row_cnt
                    if row_cnt == 0:
                        time.sleep(1)
                        break
                    # write log
                    for row in log_arr:
                        f.write(row)
                        f.write('\n')
                    # 距离上次上传日志超过2*60s，上传日志
                    if time.time() - last_time > 2*60:
                        cloud_object.upload_with_content_type(cloud_file, local_log_file,content_type='text/plain')
                        last_time = time.time()
                    time.sleep(0.5)

            if offset > 0:
                cloud_object.upload_with_content_type(cloud_file, local_log_file,content_type='text/plain')
            if overwrite:
                os.remove(local_log_file)
        except (AirflowTaskTimeout, AirflowException) as e:
                self.log.error(str(e))
                raise
        except Exception as e:
            self.log.error(traceback.format_exc())
            self.log.error('get log failed ' + str(e))
        return offset
