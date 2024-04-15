# -*- coding: utf-8 -*-
import logging
import traceback

from obs import ObsClient

import os

from airflow.configuration import conf

class Obs(object):
    def __init__(self):
        try:
            AK = conf.get('cloud_authority','obs_ak',fallback='')
            SK = conf.get('cloud_authority', 'obs_sk',fallback='')
            server = conf.get('cloud_authority', 'obs_server',fabllback='')
            self.obs = ObsClient(
                access_key_id=AK,
                secret_access_key=SK,
                server=server
            )
        except Exception as e:
            self.init_error = e
            self.obs = None

    def upload_file(self,bucket,new_file,file):
        try:
            resp = self.obs.putFile(bucket, new_file, file_path=file)
            if resp.status < 300:
                print('requestId:', resp.requestId)
            else:
                # 输出错误码
                print('errorCode:', resp.errorCode)
                # 输出错误信息
                print('errorMessage:', resp.errorMessage)
        except Exception as e:
            print('出错了：' + str(e))
            raise Exception("obs上文件生成失败")
        finally:
            os.remove(file)


    def list_obs_files(self,bucket,file):
        ss = self.obs.listObjects(bucketName=bucket, delimiter='/', encoding_type='url',
                                           prefix=file)
        if ss.status < 300:
            print ss.status
            return True
        else:
            raise Exception("没有权限访问该路径,obs://{}/{}".format(bucket,file))

    def put_content(self,buckect,file_name,content):
        self.obs.putContent(buckect,file_name,content)

    def check_file_or_dir(self, bucket, key):
        if key.endswith("/"):
            resp = self.obs.listObjects(bucketName=bucket, delimiter='/', max_keys=1,
                                        prefix=key)
            # 目录下必须有对象我们才能认为它成功
            if len(resp.body.contents) == 0:
                return False
            else:
                return True
        else:
            ss = self.obs.headObject(bucket, key)
            if ss.status == 200:
                return True
            else:
                return False

    def upload_empty_file(self,bucket,key,file_name):
        local_path = "/tmp/"+file_name
        open(local_path, 'w').close()
        try:
            resp = self.obs.putFile(bucket,key,file_path=local_path)
            if resp.status < 300:
                print('requestId:', resp.requestId)
            else:
                # 输出错误码
                print('errorCode:', resp.errorCode)
                # 输出错误信息
                print('errorMessage:', resp.errorMessage)
        except :
            raise
        finally:
            os.remove(local_path)