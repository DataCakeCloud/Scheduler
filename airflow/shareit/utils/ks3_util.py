# -*- coding: utf-8 -*-
import logging
import os

from ks3.connection import Connection
from airflow.configuration import conf

class KS3(object):
    def __init__(self):
        try:
            AK = conf.get('cloud_authority','ks3_ak',fallback='')
            SK = conf.get('cloud_authority', 'ks3_sk',fallback='')
            server = conf.get('cloud_authority', 'ks3_server',fabllback='')
            self.ks3 = Connection(AK, SK, host=server)
        except Exception as e:
            self.init_error = e
            self.ks3 = None

    def upload_file(self,bucket,new_file,file):
        try:
            b = self.ks3.get_bucket(bucket)
            k = b.new_key(new_file)
            resp = k.set_contents_from_filename(file)
            if resp and resp.status == 200:
                logging.info("上传成功")
            else:
                if resp:
                    # 输出错误码
                    logging.info('上传失败,status:{}，reason:{}', str(resp.status),resp.reason)
        except Exception as e:
            raise e
        finally:
            os.remove(file)


    def list_ks3_files(self,bucket,file):
        b = self.ks3.get_bucket(bucket)
        resp = b.listObjects(prefix=file)
        # 没异常就是成功
        return True

    def put_content(self,bucket,file_name,content):
        b = self.ks3.get_bucket(bucket)
        k = b.new_key(file_name)
        resp = k.set_contents_from_string(content, headers=None)
        if resp and resp.status == 200:
            logging.info("上传成功")
        else:
            if resp:
                # 输出错误码
                logging.info('上传失败,status:{}，reason:{}', str(resp.status), resp.reason)

    def check_file_or_dir(self, bucket, key):
        b = self.ks3.get_bucket(bucket)
        try:
            resp = b.listObjects(prefix=key)
            if not key or key.endswith('/'):
                return True
            else:
                if key in resp:
                    return True
                else :
                    return False
        except :
            raise

    def upload_empty_file(self,bucket,key,file_name):
        # local_path = "/tmp/"+file_name
        local_path = file_name
        open(local_path, 'w').close()
        try:
            b = self.ks3.get_bucket(bucket)
            k = b.new_key(key)
            resp = k.set_contents_from_filename(local_path)
            if resp and resp.status == 200:
                logging.info("上传成功")
            else:
                if resp:
                    # 输出错误码
                    logging.info('上传失败,status:{}，reason:{}', str(resp.status),resp.reason)
        except :
            raise
        finally:
            os.remove(local_path)

    def upload_with_content_type(self, bucket,file_name, local_file,content_type=None,delete_local=False):
        """
        上传本地文件到ks3指定文件夹下,并且指定content_type
        """
        try:
            b = self.ks3.get_bucket(bucket)
            k = b.new_key(file_name)
            headers = {'Content-Type': content_type}
            resp = k.set_contents_from_filename(local_file,headers=headers)
            if resp and resp.status == 200:
                logging.info("上传成功")
            else:
                if resp:
                    # 输出错误码
                    logging.info('上传失败,status:{}，reason:{}', str(resp.status),resp.reason)
        except Exception as e:
            raise e
        finally:
            if delete_local:
                os.remove(local_file)
