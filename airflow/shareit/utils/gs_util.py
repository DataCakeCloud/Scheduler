
# -*- coding: utf-8 -*-
import logging
import os
import traceback

from gcloud import storage

class GCS(object):
    def __init__(self):
        try:
            self.gcs = storage.Client()
        except Exception as e:
            self.init_error = e
            self.gcs = None

    def upload_file_gs(self,bucket_name, source_file, destination_file):
        source_file = "/tmp/" + source_file
        open(source_file, 'w').close()
        print('source_file=', source_file)
        print('destination_file=', destination_file)
        try:
            bucket = self.gcs.bucket(bucket_name)
            blob = bucket.blob(destination_file)
            blob.upload_from_filename(source_file)
            print("成功上传success文件至gcp: {}".format(source_file))
        except Exception as e:
            raise Exception('上传success文件至gcp出错了：' + str(e))
        finally:
            os.remove(source_file)


    def upload_file(self,bucket_name,source_file, destination_file):

        try:
            bucket = self.gcs.bucket(bucket_name)
            blob = bucket.blob(destination_file)
            blob.upload_from_filename(source_file)
            print("成功上传success文件至gcp: {}".format(source_file))
        except Exception as e:
            raise Exception('上传success文件至gcp出错了：' + str(e))
        finally:
            os.remove(source_file)

    def download_file_gcs(self,bucket_name, source_blob_name, destination_file_name):
        try:
            bucket = self.gcs.bucket(bucket_name)
            blob = bucket.blob(source_blob_name)
            blob.download_to_filename(destination_file_name)

            print(
                "Downloaded storage object {} from bucket {} to local file {}.".format(
                    source_blob_name, bucket_name, destination_file_name
                )
            )
        except Exception as e:
            raise Exception('从gcp下载success文件至出错了：' + str(e))

    def list_file_gcs(self,bucket,file):
        try:
            blobs = self.gcs.get_bucket(bucket)
            paths = blobs.list_blobs(prefix=file)
            for blob in paths:
                if blob.name == file:
                    return True
                if file.endswith('/') and blob.name.startswith(file):
                    return True
            return False
        except Exception as e:
            print('出错了：' + str(e))
            raise Exception("没有权限访问该路径,gs://{}/{}".format(bucket, file))

    def put_content(self,buckect,file_name,content):
        bucket = self.gcs.get_bucket(buckect)
        blob = bucket.blob(file_name)
        blob.upload_from_string(content)

    def upload_with_content_type(self,bucket_name,file_name, local_file, content_type=None, delete_local=False):
        try:
            # 获取存储桶对象
            bucket = self.gcs.get_bucket(bucket_name)
            # 创建一个 Blob 对象
            blob = bucket.blob(file_name)
            # 设置 Blob 的 ContentType
            blob.content_type = content_type
            # 上传本地文件到 GCS
            blob.upload_from_filename(local_file)
            #
            # with open(local_file, 'rb') as f:
            #     f.seek(6)
            #     blob.upload_from_file(f)
        except :
            raise
        finally:
            if delete_local:
                os.remove(local_file)

    def check_file_or_dir(self, bucket, key):
        # bucket, file_path = NormalUtil.split_check_path(gs_path)
        try:
            blobs = self.gcs.get_bucket(bucket)
            paths = blobs.list_blobs(prefix=key)
            for blob in paths:
                if blob.name == key:
                    return True
                if key.endswith('/') and blob.name.startswith(key):
                    return True
            return False
        except :
            raise

    def upload_empty_file(self,bucket_name, key, file_name):
        local_path = "/tmp/" + file_name
        open(local_path, 'w').close()
        try:
            bucket = self.gcs.bucket(bucket_name)
            blob = bucket.blob(key)
            blob.upload_from_filename(local_path)
        except :
            raise
        finally:
            os.remove(local_path)

