# -*- coding: utf-8 -*-
import logging
import traceback

import boto3
import os

class S3(object):
    def __init__(self,role_name):
        try:
            if role_name:
                sts_client = boto3.client('sts')
                assumed_role_object = sts_client.assume_role(
                    RoleArn=role_name,
                    RoleSessionName="session1"
                )
                credentials = assumed_role_object['Credentials']
                self.s3_client = boto3.client(
                    's3',
                    aws_access_key_id=credentials['AccessKeyId'],
                    aws_secret_access_key=credentials['SecretAccessKey'],
                    aws_session_token=credentials['SessionToken'],
                )
                self.s3_resource = boto3.resource(
                    's3',
                    aws_access_key_id=credentials['AccessKeyId'],
                    aws_secret_access_key=credentials['SecretAccessKey'],
                    aws_session_token=credentials['SessionToken'],
                )

            else:
                self.s3_resource = boto3.resource("s3")
                self.s3_client = boto3.client("s3")
        except Exception as e:
            self.init_error = e
            self.s3_resource = None
            self.s3_client = None

    def upload_with_content_type(self, bucket,file_name, local_file,content_type=None,delete_local=False):
        """
        上传本地文件到s3指定文件夹下,并且指定content_type,最后不删除本地文件
        """
        try:
            obj = self.s3_resource.Object(bucket,file_name)
            obj.upload_file(local_file)
            if content_type:
                obj.copy_from(CopySource={'Bucket': bucket,
                                          'Key': file_name},
                              MetadataDirective="REPLACE",
                              ContentType=content_type)

        except Exception as e:
            print('出错了：{},buckeck:{},file_name:{}'.format(str(e),bucket,file_name))
            raise e
        finally:
            if delete_local:
                os.remove(local_file)

    def upload_file_s3(self, bucket,new_file, file_name):
        """
        上传本地空文件到s3指定文件夹下，会删除本地文件
        """
        file_name = "/tmp/"+file_name
        open(file_name,'w').close()
        try:
            self.s3_resource.Object(bucket,new_file).upload_file(file_name)
            print "生产临时文件"
        except Exception as e:
            print('出错了：' + str(e))
            raise e
        finally:
            os.remove(file_name)

    def upload_file(self, bucket,new_file, file):
        """
        上传本地文件到s3指定文件夹下，会删除本地文件
        """
        try:
            self.s3_resource.Object(bucket,new_file).upload_file(file)
        except Exception as e:
            print('出错了：' + str(e))
            raise e
        finally:
            os.remove(file)

    def list_s3_files(self,bucket,file):
        try:
            if file.endswith("/"):
                response = self.s3_client.list_objects(Bucket=bucket, Delimiter='/', EncodingType='url',
                                           Prefix=file)
                if "Contents" in response.keys():
                    return True
                else:
                    return False
        except Exception as e:
            raise Exception("没有权限访问该路径,s3://{}/{}".format(bucket,file))

    def put_content(self,buckect,file_name,content):
        self.s3_resource.Object(buckect, file_name).put(Body=content)

    def check_file_or_dir(self, bucket, key):
        try:
            if key.endswith("/"):
                response = self.s3_client.list_objects(Bucket=bucket, Delimiter='/', EncodingType='url',
                                                       Prefix=key)
                if "Contents" in response.keys():
                    return True
                else:
                    return False
            else:
                self.s3_client.head_object(Bucket=bucket, Key=key)
                return True
        except:
            raise

    def upload_empty_file(self, bucket,key, file_name):
        """
        上传本地空文件到s3指定文件夹下，会删除本地文件
        """
        local_path = "/tmp/"+file_name
        open(local_path,'w').close()
        try:
            self.s3_resource.Object(bucket,key).upload_file(local_path)
        except :
            raise
        finally:
            os.remove(local_path)