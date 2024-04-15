# -*- coding: utf-8 -*-
import unittest

from airflow.shareit.utils.cloud.cloud_utli import CloudUtil


class CloudTest(unittest.TestCase):
    def test_check_file(self):
        cloud_util = CloudUtil(tenant_id=1,provider='',region='us-east-1')
        cloud_path='ks3://ninebot/test/_SUCCESS'
        print cloud_util.check_file_or_dir(cloud_path)

    def test_upload_to_cloud(self):
        cloud_util = CloudUtil(tenant_id=1,provider='',region='us-east-1')
        local_path='abc.txt'
        cloud_path='ks3://ninebot/test/1.txt'
        print cloud_util.upload_to_cloud(cloud_path,local_path)


    def test_upload_empty_file_to_cloud(self):
        cloud_util = CloudUtil(tenant_id=1,provider='',region='us-east-1')
        local_path='_SUCCESS'
        cloud_path='ks3://ninebot/test/_SUCCESS'
        print cloud_util.upload_empty_file_to_cloud(cloud_path,local_path)

    def test_put_content_to_cloud(self):
        cloud_util = CloudUtil(tenant_id=1,provider='',region='us-east-1')
        content='''
        test put content
        good
        
        genshinimpact
        sssss
        '''
        cloud_path='ks3://ninebot/test/putContent.txt'
        print cloud_util.put_content_to_cloud(cloud_path,content)

    def test_upload_with_content_type(self):
        cloud_util = CloudUtil(tenant_id=1, provider='', region='us-east-1')
        local_path='abc.txt'
        cloud_path='ks3://ninebot/test/plainText.txt'
        print cloud_util.upload_with_content_type(cloud_path, local_path)

    def test_list_ks3_files(self):
        cloud_util = CloudUtil(tenant_id=1,provider='',region='us-east-1')
        cloud_path='ks3://ninebot/test/'
        print cloud_util.list_object(cloud_path)