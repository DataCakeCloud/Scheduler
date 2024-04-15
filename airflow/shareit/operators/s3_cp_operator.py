# -*- coding: utf-8 -*-
"""
Author: zhangtao
CreateDate: 2021/1/27
"""
import boto3
from time import sleep
from datetime import datetime, timedelta
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


def copy_file(s3path=None, targetDir=None):
    if s3path is None or targetDir is None:
        raise ValueError("[Error] File path is none ")
    if not s3path.startswith("s3") or not targetDir.startswith("s3"):
        raise ValueError(
            "[Error] Input path mast be start with s3 or s3a [ From {FROM} to {TO} ]".format(FROM=str(s3path),
                                                                                             TO=str(targetDir)))
    bucket = s3path.split('/')[2]
    key = s3path.split(bucket)[-1][1:]
    if targetDir.endswith('/'):
        fileName = targetDir + '/' + key.split('/')[-1]
    else:
        fileName = targetDir
    target_bucket = fileName.split('/')[2]
    target_key = fileName.split(target_bucket)[-1][1:]
    error = None
    for i in range(3):
        try:
            s3 = boto3.resource('s3')
            copy_source = {
                'Bucket': bucket,
                'Key': key
            }
            s3.meta.client.copy(copy_source, target_bucket, target_key)
            print("Copy success! ")
            print("[ From {FROM} to {TO} ] ".format(FROM=s3path, TO=fileName))
            return
        except Exception as e:
            error = e
            print("Copy maybe failed! Will try again later...")
            print("[From {FROM} to {TO} ]".format(FROM=s3path, TO=fileName))
            sleep(10)
    if error is not None:
        raise error


def get_source_file(s3path=None, timeout=3 * 60 * 60, poke_interval=120):
    start = datetime.utcnow()
    if s3path is None:
        raise ValueError("[error] S3path should not be None!")
    bucket = s3path.split('/')[2]
    prefix = s3path.split(bucket)[-1][1:]
    error = None
    if isinstance(timeout, int):
        timeout = timedelta(seconds=timeout)
    elif isinstance(timeout, timedelta):
        pass
    else:
        raise ValueError("Param timeout should be int or timedelta!")
    if not isinstance(poke_interval, int):
        poke_interval = 120
    while datetime.utcnow() - start < timeout:
        try:
            keyList = []
            client = boto3.client('s3')
            response = client.list_objects(Bucket=bucket, Prefix=prefix)
            for key in response['Contents']:
                fileName = key["Key"].split('/')[-1]
                print(fileName)
                if key["Key"][-1:] != '/' and fileName != "success" and fileName != "_SUCCESS":
                    keyList.append(key["Key"])
        except Exception as e:
            error = e
            print("Can not get correct file name! Will try again later...")
            print(e)
            sleep(10)
        if len(keyList) > 1:
            raise ValueError(
                "There are more than one file in {PATH} (Not contain success or _SUCCESS)".format(PATH=s3path))
        elif len(keyList) == 0:
            print("There is no correct file now, Will try again later...")
            sleep(poke_interval)
            error = ValueError("File does not exist until timeout！")
        elif len(keyList) == 1:
            return "s3://" + bucket + "/" + keyList[0]
    if error is not None:
        raise error


class S3CPOperator(BaseOperator):
    template_fields = ('source_s3_path', 'dest_s3_path')
    ui_color = '#f9c915'

    @apply_defaults
    def __init__(
        self,
        source_s3_path,
        dest_s3_path,
        timeout=3 * 60 * 60,
        poke_interval=60,
        *args,
        **kwargs
    ):
        super(S3CPOperator, self).__init__(*args, **kwargs)
        self.source_s3_path = source_s3_path
        self.dest_s3_path = dest_s3_path
        self.timeout = timeout
        self.poke_interval = poke_interval
        pass

    def execute(self, context):
        full_source_path = get_source_file(s3path=self.source_s3_path,
                                           timeout=self.timeout,
                                           poke_interval=self.poke_interval)
        if full_source_path:
            copy_file(s3path=full_source_path,targetDir=self.dest_s3_path)
        else:
            raise ValueError("Can not get correct source file path!")
