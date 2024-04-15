# -*- coding: utf-8 -*-
import re

import requests
import six
import time
import datetime
import time

import pendulum
from airflow.configuration import conf
from airflow.shareit.utils.gs_util import GCS
from airflow.shareit.utils.obs_util import Obs
from airflow.shareit.utils.s3_util import S3

from airflow.utils import timezone
from airflow.utils.log.logging_mixin import LoggingMixin

'''
    用来放一些不知道该放到哪里的util方法.....
'''
class NormalUtil(LoggingMixin):

    '''
        获取无序数值类型数组的中位数,很骚的实现,主要利用了下标取反的原理
    '''
    @staticmethod
    def get_list_mid(one_list):
        if len(one_list) == 0:
            return 0
        # 用sorted是为了不改变原list对象的内容
        one_list = sorted(one_list)
        half = len(one_list) / 2
        return float(one_list[half] + one_list[~half]) / 2

    '''
        把数组按size分块
    '''
    @staticmethod
    def get_split_block_list(one_list,size):
        return [one_list[i:i + size] for i in range(0, len(one_list), size)]


    @staticmethod
    def split_check_path(input_path):
        # 切分路径的bucket和key
        bucket = ""
        key = ""
        if input_path is None or input_path == "":
            return "", ""
        input_path = input_path.strip()
        if input_path.startswith("https://s3.amazonaws.com/"):
            tmp_path = input_path.split("https://s3.amazonaws.com/")[1]
            bucket = tmp_path.split('/')[0]
            key = tmp_path.split(bucket + '/')[1]
            return bucket, key
        elif input_path.startswith("http"):
            if "amazonaws.com" in input_path:
                tmp = input_path.split("?")[0].split("://")[1]
                bucket = ".".join(tmp.split("/")[0].split(".")[0:-4])
                key = "/".join(tmp.split("/")[1:])
                return bucket, key
            elif "myhuaweicloud.com" in input_path:
                tmp = input_path.split("?")[0].split("://")[1]
                bucket = ".".join(tmp.split("/")[0].split(".")[0:-4])
                key = "/".join(tmp.split("/")[1:])
                return bucket, key
        elif input_path.startswith("obs://") or input_path.startswith("s3://") or input_path.startswith("s3a://"):
            tmp_path = input_path.split("://")[1]
            bucket = tmp_path.split('/')[0]
            key = tmp_path.split(bucket + '/')[1]
        else:
            raise ValueError("依赖路径有误: " + str(input_path))
        return bucket, key

    @staticmethod
    def get_extremely_important_task_list():
        tasks = conf.get("core","extremely_important_task")
        task_list = tasks.split(",")
        return task_list


    @staticmethod
    def parse_cluster_tag(cluster_tag):
        if not cluster_tag:
            return '',''
        pattern = re.compile("region:([^,:]+)")
        search_region = re.search(pattern, cluster_tag)
        if search_region:
            region = search_region.group(1)
        else:
            region = ''

        pattern = re.compile("provider:([^,:]+)")
        search_provider = re.search(pattern, cluster_tag)
        if search_provider:
            provider = search_provider.group(1)
        else:
            provider = ''
        return region, provider

    @staticmethod
    def cluster_tag_mapping_replace(cluster_tag):
        # use_cloud_authority = conf.getboolean('core','use_cloud_authority')
        # if not use_cloud_authority:
        #     return cluster_tag
        if not cluster_tag:
            return ''
        pattern = re.compile("region:([^,:]+)")
        search_region = re.search(pattern, cluster_tag)
        if search_region:
            region = search_region.group(1)
        else:
            region = ''

        if region == 'sg1':
            cluster_tag = cluster_tag.replace('region:sg1','region:ap-southeast-1') + ',provider:aws'
        elif region == 'sg2':
            cluster_tag = cluster_tag.replace('region:sg2','region:ap-southeast-3') + ',provider:huaweicloud'
        elif region == 'ue1':
            cluster_tag = cluster_tag.replace('region:ue1','region:us-east-1') + ',provider:aws'
        return cluster_tag

    @staticmethod
    def callback_task_state(task_desc, state):
        headers = {"current_login_user": '{{"tenantName": "{}","tenantId": {}, "isAdmin": true}}'.format(
            task_desc.user_group.get('tenantName', ''), task_desc.tenant_id)}
        url = conf.get("core", "default_task_url")
        data = {"taskName": task_desc.ds_task_name, "status": state}
        requests.put(url=url, data=data, headers=headers)

    @staticmethod
    def template_code_transform(template_code):
        template_code = template_code.strip()
        if template_code == 'Hive2Hive':
            return 'Lakehouse->Lakehouse'
        elif template_code == 'SPARKJAR':
            return 'SparkJar'
        elif template_code == 'Mysql2Hive':
            return 'DB->Lakehouse'
        elif template_code == 'PythonShell':
            return 'Python/Shell'
        elif template_code == 'Hive2File':
            return 'Lakehouse->File'
        elif template_code == 'Hive2Doris':
            return 'Lakehouse->Doris'
        elif template_code == 'Hive2Mysql':
            return 'Lakehouse->Mysql'
        elif template_code == 'File2Lakehouse':
            return 'File->Lakehouse'
        elif template_code == 'QueryEdit':
            return '数据分析任务'
        else:
            return template_code
