# -*- coding: utf-8 -*-
import logging
import random
import requests
from airflow.configuration import conf

from airflow import secrets

from airflow.shareit.utils.gs_util import GCS
from airflow.shareit.utils.ks3_util import KS3
from airflow.shareit.utils.obs_util import Obs
from airflow.shareit.utils.s3_util import S3


class CloudUtil(object):
    def __init__(self, provider=None, region=None,tenant_id=1):
        self.tenant_id = tenant_id
        self.provider = provider
        self.region = region
        self._conn = None
        # self._conn = self.get_connection('cloud_host')
        # 这里直接初始化资源
        # self._s3 = S3(self.get_role_name())
        # self._obs = Obs()
        # self._gcs = GCS()


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
        use_cloud_authority = conf.getboolean('core', 'use_cloud_authority')
        if self.provider and self.provider == 'aws' and use_cloud_authority:
            host = self._conn.host
            url = '{}/cluster-service/cloud/resource/getRoleName'.format(host)
            params = {'provider': self.provider, 'region': self.region}
            hearders = {'current_login_user':'{{"tenantId":{}}}'.format(self.tenant_id)}
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
        if not hasattr(self,'_s3'):
            self._s3 = S3(self.get_role_name())
        if not self._s3.s3_resource:
            if hasattr(self._s3, 'init_error'):
                logging.error(str(self._s3.init_error))
            raise ValueError("S3 init failed,please check your S3 config")
        return self._s3

    @property
    def obs(self):
        if not hasattr(self,'_obs'):
            self._obs = Obs()
        if not self._obs.obs:
            if hasattr(self._obs, 'init_error'):
                logging.error(str(self._obs.init_error))
            raise ValueError("OBS init failed,please check your OBS config")
        return self._obs

    @property
    def gcs(self):
        if not hasattr(self,'_gcs'):
            self._gcs = GCS()
        if not self._gcs.gcs:
            if hasattr(self._gcs, 'init_error'):
                logging.error(str(self._gcs.init_error))
            raise ValueError("GCS init failed,please check your GCS config")
        return self._gcs

    @property
    def ks3(self):
        if not hasattr(self,'_ks3'):
            self._ks3 = KS3()
        if not self._ks3.ks3:
            if hasattr(self._ks3, 'init_error'):
                logging.error(str(self._ks3.init_error))
            raise ValueError("KS3 init failed,please check your KS3 config")
        return self._ks3


    def get_buckeck_and_file(self, cloud_path):
        bucket = ''
        file = ''
        cloud_path = cloud_path.strip()
        if cloud_path.startswith('s3://'):
            # AWS S3
            cloud_path = cloud_path.replace('s3://', '')
            parts = cloud_path.split('/', 1)
            bucket = parts[0]
            file = parts[1] if len(parts) > 1 else ''
        elif cloud_path.startswith('gs://'):
            # Google Cloud Storage
            cloud_path = cloud_path.replace('gs://', '')
            parts = cloud_path.split('/', 1)
            bucket = parts[0]
            file = parts[1] if len(parts) > 1 else ''

        elif cloud_path.startswith('obs://'):
            # Huawei Cloud OBS
            cloud_path = cloud_path.replace('obs://', '')
            parts = cloud_path.split('/', 1)
            bucket = parts[0]
            file = parts[1] if len(parts) > 1 else ''
        elif cloud_path.startswith('ks3://'):
            # KS3
            cloud_path = cloud_path.replace('ks3://', '')
            parts = cloud_path.split('/', 1)
            bucket = parts[0]
            file = parts[1] if len(parts) > 1 else ''

        return bucket, file

    def upload_to_cloud(self, cloud_path, local_path):
        bucket, file = self.get_buckeck_and_file(cloud_path)
        cloud_path=cloud_path.strip()
        if cloud_path.startswith('s3://'):
            # AWS S3
            self.s3.upload_file(bucket, file, local_path)
        elif cloud_path.startswith('gs://'):
            # Google Cloud Storage
            self.gcs.upload_file(bucket, local_path, file)

        elif cloud_path.startswith('obs://'):
            # Huawei Cloud OBS
            self.obs.upload_file(bucket, file, local_path)
        elif cloud_path.startswith('ks3://'):
            # KS3
            self.ks3.upload_file(bucket, file, local_path)

    def put_content_to_cloud(self, cloud_path, content):
        bucket, file = self.get_buckeck_and_file(cloud_path)
        cloud_path = cloud_path.strip()
        if cloud_path.startswith('s3://'):
            # AWS S3
            self.s3.put_content(bucket, file, content)
        elif cloud_path.startswith('gs://'):
            self.gcs.put_content(bucket, file, content)

        elif cloud_path.startswith('obs://'):
            self.obs.put_content(bucket, file, content)
        elif cloud_path.startswith('ks3://'):
            self.ks3.put_content(bucket, file, content)

    def list_object(self, cloud_path):
        bucket, file = self.get_buckeck_and_file(cloud_path)
        cloud_path = cloud_path.strip()
        if cloud_path.startswith('s3://'):
            # AWS S3
            return self.s3.list_s3_files(bucket, file)
        elif cloud_path.startswith('gs://'):
            return self.gcs.list_file_gcs(bucket, file)

        elif cloud_path.startswith('obs://'):
            return self.obs.list_obs_files(bucket, file)
        elif cloud_path.startswith('ks3://'):
            return self.ks3.list_ks3_files(bucket, file)
        else:
            raise ValueError("cant support this cloud path:{}".format(cloud_path))

    def upload_with_content_type(self, cloud_path, local_path, content_type='text/plain'):
        bucket, file = self.get_buckeck_and_file(cloud_path)
        cloud_path = cloud_path.strip()
        if cloud_path.startswith('s3://'):
            self.s3.upload_with_content_type(bucket, file, local_path,
                                               content_type=content_type)
        elif cloud_path.startswith('gs://'):
            self.gcs.upload_with_content_type(bucket, file, local_path,
                                                content_type=content_type)
        elif cloud_path.startswith('obs://'):
            self.obs.list_obs_files(bucket, file)
        elif cloud_path.startswith('ks3://'):
            self.ks3.upload_with_content_type(bucket, file, local_path,
                                                content_type=content_type)
        else:
            raise ValueError("cant support this cloud path:{}".format(cloud_path))


    def check_file_or_dir(self, cloud_path):
        bucket, file = self.get_buckeck_and_file(cloud_path)
        cloud_path = cloud_path.strip()
        if cloud_path.startswith('s3://'):
            return self.s3.check_file_or_dir(bucket, file)
        elif cloud_path.startswith('gs://'):
            return self.gcs.check_file_or_dir(bucket, file)
        elif cloud_path.startswith('obs://'):
            return self.obs.check_file_or_dir(bucket, file)
        elif cloud_path.startswith('ks3://'):
            return self.ks3.check_file_or_dir(bucket, file)
        else:
            raise ValueError("cant support this cloud path:{}".format(cloud_path))

    def upload_empty_file_to_cloud(self, cloud_path, empty_file_name):
        '''
        将一个空文件上传到云端
        '''
        bucket, file = self.get_buckeck_and_file(cloud_path)
        cloud_path = cloud_path.strip()
        if cloud_path.startswith('s3://'):
            # AWS S3
            self.s3.upload_empty_file(bucket, file, empty_file_name)
        elif cloud_path.startswith('gs://'):
            # Google Cloud Storage
            self.gcs.upload_empty_file(bucket, file,empty_file_name)

        elif cloud_path.startswith('obs://'):
            # Huawei Cloud OBS
            self.obs.upload_empty_file(bucket, file, empty_file_name)
        elif cloud_path.startswith('ks3://'):
            # KS3
            self.ks3.upload_empty_file(bucket, file, empty_file_name)
