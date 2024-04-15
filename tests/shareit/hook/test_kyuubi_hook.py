# -*- coding: utf-8 -*-
import datetime
import unittest

import pendulum

from airflow.hooks.genie_hook import GenieHook
from airflow.models import TaskInstance
from airflow.shareit.hooks.kyuubi_hook import KyuubiHook
from airflow.shareit.hooks.message.wechat_hook import WechatHook
from airflow.shareit.jobs.alert_job import AlertJob, callback_processor
from airflow.utils import timezone
from airflow.utils.timezone import beijing


class kyuubiHookTest(unittest.TestCase):

    def test_send_text(self):
        WechatHook(robot_key='075a7fbc-1b89-4bf8-bf14-f81e9a1f19e3').send_markdown('# 123 ',mentioned_list=["wuhaorui","wuhaorui"])

    def test_kyuubi_hook(self):
        class_name = 'com.ushareit.sql.SparkSubmitSql'
        resource = 's3://shareit.deploy.us-east-1/BDP/spark-submit-sql-1.0-SNAPSHOT.jar'
        job_name = 'test_kyuubi'
        conf = {
            "spark.hadoop.fs.s3.impl": "com.amazon.ws.emr.hadoop.fs.EmrFileSystem",
            "spark.hadoop.fs.s3a.impl": "com.amazon.ws.emr.hadoop.fs.EmrFileSystem",
            "spark.executorEnv.AWS_REGION": "us-east-1",
            "spark.kubernetes.driverEnv.AWS_REGION": "us-east-1",
            "spark.hadoop.fs.s3.customAWSCredentialsProvider": "com.ushareit.credentialsprovider.CustomWebIdentityTokenCredentialsProvider",
            "spark.hadoop.fs.s3a.customAWSCredentialsProvider": "com.ushareit.credentialsprovider.CustomWebIdentityTokenCredentialsProvider",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "com.ushareit.credentialsprovider.CustomWebIdentityTokenCredentialsProvider",
            "spark.kubernetes.container.image": "848318613114.dkr.ecr.us-east-1.amazonaws.com/bdp/spark:spark-3.2.2.3-lakecat-probe-v1.01-amzn-1-iceberg-0.14.0.1-eks",
            "spark.kubernetes.file.upload.path": "s3://shareit.deploy.us-east-1/BDP/BDP-spark-stagingdir/test/BDP-eks-bigdata-use1-test/",
            "spark.hadoop.aws.region": "us-east-1",
            "spark.hadoop.fs.s3.buffer.dir": "/tmp/s3",
            "spark.master": "k8s://https://CE481C337C8C36B5DFD2B17B02D67BD3.gr7.us-east-1.eks.amazonaws.com",
            "spark.submit.deployMode": "cluster",
            "spark.driver.extraClassPath": ":/opt/extraClassPath/*:/opt/conf/",
            "spark.executor.extraClassPath": ":/opt/extraClassPath/*:opt/conf/",
            "spark.kubernetes.namespace": "bdp",
            "spark.kubernetes.authenticate.driver.serviceAccountName": "genie",
            "spark.kubernetes.authenticate.serviceAccountName": "genie",
            "spark.kubernetes.driver.node.selector.lifecycle": "OnDemand",
            "spark.kubernetes.driver.node.selector.workload": "spark_driver",
            "spark.kubernetes.executor.node.selector.lifecycle": "Ec2Spot",
            "spark.kubernetes.executor.node.selector.workload": "general",
            "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version.emr_internal_use_only.EmrFileSystem": "2",
            "spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored.emr_internal_use_only.EmrFileSystem": "true",
            "spark.sql.parquet.compression.codec": "gzip",
            "spark.sql.parquet.output.committer.class": "com.amazon.emr.committer.EmrOptimizedSparkSqlParquetOutputCommitter",
            "spark.sql.parquet.fs.optimized.committer.optimization-enabled": "true",
            "spark.kubernetes.ui.proxyUrlSchema": "http",
            "spark.kubernetes.ui.proxyUrlSuffix": "spark-ui.bdp-eks-bigdata-use1-prod.ushareit.org",
            "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkSessionCatalog",
            "spark.sql.catalog.iceberg.type": "hive",
            "spark.sql.catalog.iceberg.cache-enabled": "false",
            "spark.sql.catalog.iceberg.uri": "thrift://hivemetastore-ue1-prod.ushareit.org:9083",
            "spark.hadoop.hive.metastore.uris": "thrift://hms-ue1.ushareit.org:9083",
            "spark.sql.defaultCatalog": "iceberg",
            "spark.sql.extensions": "io.lakecat.probe.spark.SparkTablePermissonExtensions,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        }
        args = ["-format","text", "-colType", "true", "-e", "select 1", "-o", "s3://shareit.bigdata.olap.us-east-1.prod/Default/aws-6c66893a5c277077218126fd8d44d382"]
        execution_date = pendulum.parse('2023-02-26 00:00:00')
        dag_id = '1_22540'
        tis = TaskInstance.find_by_execution_dates(dag_id=dag_id,execution_dates=[execution_date])
        ti = tis[0]
        hook = KyuubiHook()
        status = hook.submit_job(ti=ti,
                                    batch_type='spark',
                                    job_name=job_name,
                                    class_name=class_name,
                                    conf=conf,
                                    args=args,
                                    resource=resource)
        print status
