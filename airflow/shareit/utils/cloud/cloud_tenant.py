# -*- coding: utf-8 -*-
import logging
import random

import requests

from airflow import secrets

from airflow.shareit.utils.gs_util import GCS
from airflow.shareit.utils.obs_util import Obs
from airflow.shareit.utils.s3_util import S3

class CloudTenant:
    def __init__(self, provider=None, region=None,tenant_id=1):
        self.tenant_id = tenant_id
        self.provider = provider
        self.region = region
        self._conn = self.get_connection('cloud_host')
        # 这里直接初始化资源
        self._s3 = S3(self.get_role_name())
        self._obs = Obs()
        self._gcs = GCS()


    @classmethod
    def get_connections(cls, conn_id):
        return secrets.get_connections(conn_id)

    @classmethod
    def get_connection(cls, conn_id):
        conn = random.choice(list(cls.get_connections(conn_id)))
        if conn.host:
            logging.info("Using connection to: %s", conn.log_info())
        return conn

    def get_role_name(self):
        if self.provider and self.provider == 'aws':
            host = self._conn.host
            url = '{}/cluster-service/cloud/resource/getRoleName'.format(host)
            params = {'provider': self.provider, 'region': self.region}
            hearders = {'current_login_user':'{"tenantId":1}'}
            resp = requests.get(url, params, headers=hearders)
            if resp.status_code != 200:
                raise ValueError('connect to {} failed'.format(url))
            resp_json = resp.json()
            return resp_json['data']
        else:
            # 除了aws都不支持
            return ''

    @property
    def s3(self):
        if not self._s3.s3_resource:
            if hasattr(self._s3, 'init_error'):
                logging.error(str(self._s3.init_error))
            raise ValueError("S3 init failed,please check your S3 config")
        return self._s3

    @property
    def obs(self):
        if not self._obs.obs:
            if hasattr(self._obs, 'init_error'):
                logging.error(str(self._obs.init_error))
            raise ValueError("OBS init failed,please check your OBS config")
        return self._obs

    @property
    def gcs(self):
        if not self._gcs.gcs:
            if hasattr(self._gcs, 'init_error'):
                logging.error(str(self._gcs.init_error))
            raise ValueError("GCS init failed,please check your GCS config")
        return self._gcs