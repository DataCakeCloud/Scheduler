# -*- coding: utf-8 -*-
import base64
import binascii
import json
import os
import re
import time
import traceback

import jaydebeapi
import pyhive.exc
import six
import requests
import subprocess

from TCLIService.ttypes import TOperationState
from pyhive import hive
from thrift.transport.TTransport import TTransportException

from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException, AirflowTaskTimeout
from airflow.shareit.constant.kyuubi_state import KyuubiState
from airflow.shareit.utils.cloud.cloud_utli import CloudUtil
from airflow.shareit.utils.s3_util import S3
from airflow.shareit.utils.state import State
from airflow.configuration import conf as ds_conf

SPARK = 'spark'
GET_LOG = 'get_log'
KILL_JOB = 'kill_job'
GET_INFO = 'get_info'


class KyuubiJdbcHook(BaseHook):
    """
    任务提交给datacake - gateway。 gateway是基于kyuubi实现的
    """

    def __init__(self,
                 conn_id='kyuubi_jdbc',
                 batch_type=None,
                 batch_id=None,
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
        self.batch_id = batch_id

    def submit_job(self,
                   ti,
                   batch_type=None,
                   conf=None,
                   sql=None,
                   default_db=None,
                   owner=None,
                   region=None,
                   ):
        self.owner = owner
        self.sql = sql
        self.db = default_db
        self.conf = conf
        self.ti = ti
        error_content = []
        self.init_log()
        # 先上传一次文件，防止用户打开链接时没有数据就变成下载文件了
        self.append_local_log(['Task starting...\n', 'Execute sql: {}\n'.format(sql)])
        self.push_log()
        self.log.info('configure:{}'.format(json.dumps(conf)))

        try:
            if ti:
                ex_conf = ti.get_external_conf()
                ex_conf = ti.format_external_conf(ex_conf)
                ex_conf['is_kyuubi'] = True
                if ex_conf is not None:
                    ti.save_external_conf(ex_conf=ex_conf)

            if ti:
                ex_conf = ti.get_external_conf()
                ex_conf = ti.format_external_conf(ex_conf)
                ex_conf['batch_id'] = self.batch_id
                ex_conf['is_kyuubi'] = True
                ex_conf['region'] = region
                ex_conf['new_log'] = True
                ex_conf['is_tfjob'] = True if batch_type == 'tfjob' else False
                ex_conf['is_spark'] = True if batch_type == 'spark' else False
                ti.save_external_conf(ex_conf=ex_conf)
            state = self.execute()
            self.log.info('Job finished,state:{}'.format(state))
            return state
        except AirflowTaskTimeout as e:
            error_content.append('Job timed out...')
        except AirflowException as e:
            error_content.append('Job has been killed...')
        except Exception as e:
            self.log.error('Job failed ,error:{} '.format(str(e)))
            error_content.append(str(e))
            raise
        finally:
            self.append_local_log(error_content)
            self.push_log()

    def execute(self):
        conn = hive.connect(
            scheme='jdbc:hive2',
            host=self._conn.host,
            port=10009,
            database=self.db,
            auth='NOSASL',
            username=self.owner,
            configuration=self.conf
        )
        try:
            self.log.info('secret:'+binascii.hexlify(conn.sessionHandle.sessionId.secret).decode('utf-8'))
            self.log.info('guid:'+binascii.hexlify(conn.sessionHandle.sessionId.guid).decode('utf-8'))
            last_push_time = time.time()
            new_sql = re.sub(r'^\s*--.*?$', '', self.sql, flags=re.MULTILINE).strip()
            with conn.cursor() as curs:
                for sql in new_sql.split(';'):
                    # sql = re.sub(r'^\s*--.*?$', '', sql, flags=re.MULTILINE).strip()
                    if not sql.strip():
                        continue
                    state = self.execute_with_retry(curs,sql,last_push_time)
                    if state == KyuubiState.KYUUBI_ERROR:
                        return state
                    # curs.execute(sql.strip(), async_=True)
                    # resp = ''
                    # while True:
                    #     resp = curs.poll()
                    #     try:
                    #         logs = curs.fetch_logs()
                    #         self.append_local_log(logs)
                    #     except TypeError:
                    #         # pyhive可能会因为response.results.columns为空值报type error，目前还不知道是什么原因，可能是kyuubi也可能是pyhive本身的bug
                    #         self.log.error(traceback.format_exc())
                    #         self.log.info(resp)
                    #         # 如果是set语句出现none type的问题，直接执行下一条sql
                    #         if sql.strip().lower().startswith('set '):
                    #             break
                    #         raise
                    #     except Exception:
                    #         raise
                    #     current_time = time.time()
                    #     # 30s 上传一次日志
                    #     if current_time - last_push_time >= 30:
                    #         self.push_log()
                    #         last_push_time = current_time
                    #     if resp.operationState in [TOperationState.ERROR_STATE, TOperationState.FINISHED_STATE,
                    #                                TOperationState.CLOSED_STATE, TOperationState.CANCELED_STATE]:
                    #         if resp.operationState != TOperationState.FINISHED_STATE:
                    #             self.log.warn('Job failed, jdbc state:{}'.format(str(resp.operationState)))
                    #             return KyuubiState.KYUUBI_ERROR
                    #         break
                    #     time.sleep(10)
            return KyuubiState.KYUUBI_FINISHED
        except Exception:
            raise
        finally:
            try:
                conn.close()
            except (AirflowTaskTimeout,AirflowException):
                raise
            except Exception as e :
                self.log.error('connection close failed,error:{}'.format(str(e)))
                pass

    def execute_with_retry(self,curs,sql,last_push_time,max_retries=3, base_delay=5, max_delay=300):
        retries = 0
        resp = ''
        while retries < max_retries:
            try:
                curs.execute(sql.strip(), async_=True)
                while True:
                    resp = curs.poll()
                    try:
                        logs = curs.fetch_logs()
                        self.append_local_log(logs)
                        current_time = time.time()
                        # 15s 上传一次日志
                        if current_time - last_push_time >= 15:
                            self.push_log()
                            last_push_time = current_time
                    except TypeError as e:
                        self.log.error(e)
                        pass
                    except Exception:
                        raise
                    if resp.operationState in [TOperationState.ERROR_STATE, TOperationState.FINISHED_STATE,
                                               TOperationState.CLOSED_STATE, TOperationState.CANCELED_STATE]:
                        if resp.operationState != TOperationState.FINISHED_STATE:
                            self.log.warn('Job failed, jdbc state:{}'.format(str(resp.operationState)))
                            return KyuubiState.KYUUBI_ERROR
                        return KyuubiState.KYUUBI_FINISHED
                    self.log.info('resp.operationState: {}'.format(str(resp.operationState)))
                    time.sleep(10)
            # except TypeError:
            #     self.log.error(traceback.format_exc())
            #     self.log.info(resp)
            #     retries += 1
            #     if retries == max_retries:
            #         raise
            #     # 计算退避延迟
            #     delay = min(base_delay * (2 ** retries), max_delay)
            #     self.log.info("None type error appears, retrying in {} seconds...".format(str(delay)))
            #     time.sleep(delay)
            except Exception:
                raise


    def init_log(self):
        dag_folder = ds_conf.get('core', 'base_log_folder')
        # 本地文件
        self.app_log_file = '{dag_folder}/{dag_id}/{task_id}/{time}/{batch_id}'.format(dag_folder=dag_folder,
                                                                                  dag_id=self.ti.dag_id, task_id=self.ti.task_id,
                                                                                  time=self.ti.execution_date.isoformat(),
                                                                                  batch_id=self.batch_id)
        # 上传到云存储的桶
        log_cloud_home = ds_conf.get('job_log', 'log_cloud_file_location').strip()
        # 云存储的文件
        self.log_cloud_file = '{log_cloud_home}/BDP/logs/pipeline_log/{time}/{batch_id}.txt'.format(log_cloud_home=log_cloud_home,
                                                                                      time=self.ti.start_date.strftime(
                                                                                          '%Y%m%d'),
                                                                                      batch_id=self.batch_id)
        self.log.info('local file location: {}'.format(self.app_log_file))
        self.log.info('cloud file location: {}'.format(self.log_cloud_file))
        log_provider = ds_conf.get('job_log', 'log_provider')
        log_region = ds_conf.get('job_log', 'log_region')
        self.cloud_object = CloudUtil(provider=log_provider, region=log_region)

    def append_local_log(self,logs):
        if not logs:
            return
        with open(self.app_log_file, 'a') as f:
            for line in logs:
                f.write(line+"\n")

    def push_log(self):
        self.cloud_object.upload_with_content_type(self.log_cloud_file, self.app_log_file, content_type='text/plain')
        pass

    def kill_job(self):
        pass