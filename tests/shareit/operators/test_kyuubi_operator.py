import datetime
import unittest

import pendulum

from airflow.hooks.genie_hook import GenieHook
from airflow.models import TaskInstance
from airflow.shareit.hooks.kyuubi_hook import KyuubiHook
from airflow.shareit.jobs.alert_job import AlertJob, callback_processor
from airflow.shareit.operators.kyuubi_operator import KyuubiOperator
from airflow.shareit.utils.task_manager import get_dag
from airflow.utils import timezone
from airflow.utils.timezone import beijing


class kyuubiHookTest(unittest.TestCase):

    def test_kyuubi_hook(self):
        command = '''
                --conf spark.hadoop.fs.s3.impl=com.amazon.ws.emr.hadoop.fs.EmrFileSystem
    --conf spark.hadoop.fs.s3a.impl=com.amazon.ws.emr.hadoop.fs.EmrFileSystem
    --conf spark.executorEnv.AWS_REGION=us-east-1
    --conf spark.kubernetes.driverEnv.AWS_REGION=us-east-1
    --conf spark.hadoop.fs.s3.customAWSCredentialsProvider=com.ushareit.credentialsprovider.CustomWebIdentityTokenCredentialsProvider
    --conf spark.hadoop.fs.s3a.customAWSCredentialsProvider=com.ushareit.credentialsprovider.CustomWebIdentityTokenCredentialsProvider
    --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.ushareit.credentialsprovider.CustomWebIdentityTokenCredentialsProvider
    --conf spark.kubernetes.container.image=848318613114.dkr.ecr.us-east-1.amazonaws.com/bdp/spark=spark-3.2.2.3-lakecat-probe-v1.01-amzn-1-iceberg-0.14.0.1-eks
    --conf spark.kubernetes.file.upload.path=s3://shareit.deploy.us-east-1/BDP/BDP-spark-stagingdir/test/BDP-eks-bigdata-use1-test/
    --conf spark.hadoop.aws.region=us-east-1
    --conf spark.hadoop.fs.s3.buffer.dir=/tmp/s3
    --conf spark.master = k8s://https://CE481C337C8C36B5DFD2B17B02D67BD3.gr7.us-east-1.eks.amazonaws.com
    --conf spark.submit.deployMode=cluster
    --conf spark.driver.extraClassPath=/opt/extraClassPath/*:/opt/conf/
    --conf spark.executor.extraClassPath=/opt/extraClassPath/*:opt/conf/
    --conf spark.kubernetes.namespace=bdp
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=genie
    --conf spark.kubernetes.authenticate.serviceAccountName=genie
    --conf spark.kubernetes.driver.node.selector.lifecycle=OnDemand
    --conf spark.kubernetes.driver.node.selector.workload=spark_driver
    --conf spark.kubernetes.executor.node.selector.lifecycle=Ec2Spot
    --conf spark.kubernetes.executor.node.selector.workload=general
    --conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version.emr_internal_use_only.EmrFileSystem=2
    --conf spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored.emr_internal_use_only.EmrFileSystem=true
    --conf spark.sql.parquet.compression.codec=gzip
    --conf spark.sql.parquet.output.committer.class=com.amazon.emr.committer.EmrOptimizedSparkSqlParquetOutputCommitter
    --conf spark.sql.parquet.fs.optimized.committer.optimization-enabled=true
    --conf spark.kubernetes.ui.proxyUrlSchema=http
    --conf spark.kubernetes.ui.proxyUrlSuffix=spark-ui.bdp-eks-bigdata-use1-prod.ushareit.org
    --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkSessionCatalog
    --conf spark.sql.catalog.iceberg.type=hive
    --conf spark.sql.catalog.iceberg.cache-enabled=false
    --conf spark.sql.catalog.iceberg.uri=thrift=//hivemetastore-ue1-prod.ushareit.org=9083
    --conf spark.hadoop.hive.metastore.uris=thrift=//hms-ue1.ushareit.org=9083
    --conf spark.sql.defaultCatalog=iceberg
    --conf spark.sql.extensions=io.lakecat.probe.spark.SparkTablePermissonExtensionsorg.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
    --class com.ushareit.sql.SparkSubmitSql
	s3=//shareit.deploy.us-east-1/BDP/spark-submit-sql-1.0-SNAPSHOT.jar  -format text -colType true -e 'select 1' '''
        operator = KyuubiOperator(
            batch_type='spark',
            job_name='test_kyuubi',
            command=command,
            cluster_tags='type:k8s,region:ue1,sla:normal',
            command_tags='spark-submit-ds"',
            task_id='test_kyuubi'
        )
        operator.my_init()
        execution_date = pendulum.parse('2023-02-26 00:00:00')
        dag_id = '1_22540'
        tis = TaskInstance.find_by_execution_dates(dag_id=dag_id,execution_dates=[execution_date])
        ti = tis[0]
        context = {"ti":ti}
        operator.execute(context)

    def test_kyuubi_op(self):
        dag = get_dag('1_22560')
        task = dag.tasks[0]
        owner = task.owner.split(',')[0] if task.owner else ''
        execution_date = pendulum.parse('2023-03-27 00:00:00')
        dag_id = '1_22560'
        tis = TaskInstance.find_by_execution_dates(dag_id=dag_id,execution_dates=[execution_date])
        ti = tis[0]
        context = {"ti": ti}
        task.execute(context)

    def test_trino_op(self):
        dag_id = "1_22543"
        dag = get_dag(dag_id)
        task = dag.tasks[0]
        owner = task.owner.split(',')[0] if task.owner else ''
        execution_date = pendulum.parse('2023-04-25 00:00:00')
        tis = TaskInstance.find_by_execution_dates(dag_id=dag_id, execution_dates=[execution_date])
        ti = tis[0]
        context = {"ti": ti}
        context["owner"] = owner
        task.execute(context)

