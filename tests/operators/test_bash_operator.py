# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
import time
import unittest

import jaydebeapi

from airflow.configuration import conf

from airflow.models import TaskInstance
from airflow.shareit.operators.bash_operator import BashOperator
from airflow.shareit.utils import task_manager
from airflow.shareit.utils.cloud.cloud_utli import CloudUtil
from airflow.shareit.utils.constant_config_util import ConstantConfigUtil
from airflow.shareit.utils.normal_util import NormalUtil

from tests.compat import mock
from datetime import datetime, timedelta
from tempfile import NamedTemporaryFile

from airflow import DAG
from airflow.utils import timezone
from airflow.utils.state import State

DEFAULT_DATE = datetime(2016, 1, 1, tzinfo=timezone.utc)
END_DATE = datetime(2016, 1, 2, tzinfo=timezone.utc)
INTERVAL = timedelta(hours=12)


class TestBashOperator(unittest.TestCase):

    def test_echo_env_variables(self):
        """
        Test that env variables are exported correctly to the
        task bash environment.
        """
        now = datetime.utcnow()
        now = now.replace(tzinfo=timezone.utc)

        self.dag = DAG(
            dag_id='bash_op_test', default_args={
                'owner': 'airflow',
                'retries': 100,
                'start_date': DEFAULT_DATE
            },
            schedule_interval='@daily',
            dagrun_timeout=timedelta(minutes=60))

        self.dag.create_dagrun(
            run_id='manual__' + DEFAULT_DATE.isoformat(),
            execution_date=DEFAULT_DATE,
            start_date=now,
            state=State.RUNNING,
            external_trigger=False,
        )

        with NamedTemporaryFile() as tmp_file:
            task = BashOperator(
                task_id='echo_env_vars',
                dag=self.dag,
                bash_command='echo $AIRFLOW_HOME>> {0};'
                             'echo $PYTHONPATH>> {0};'
                             'echo $AIRFLOW_CTX_DAG_ID >> {0};'
                             'echo $AIRFLOW_CTX_TASK_ID>> {0};'
                             'echo $AIRFLOW_CTX_EXECUTION_DATE>> {0};'
                             'echo $AIRFLOW_CTX_DAG_RUN_ID>> {0};'.format(tmp_file.name)
            )

            with mock.patch.dict('os.environ', {
                'AIRFLOW_HOME': 'MY_PATH_TO_AIRFLOW_HOME',
                'PYTHONPATH': 'AWESOME_PYTHONPATH'
            }):
                task.run(DEFAULT_DATE, DEFAULT_DATE,
                         ignore_first_depends_on_past=True, ignore_ti_state=True)

            with open(tmp_file.name, 'r') as file:
                output = ''.join(file.readlines())
                self.assertIn('MY_PATH_TO_AIRFLOW_HOME', output)
                # exported in run-tests as part of PYTHONPATH
                self.assertIn('AWESOME_PYTHONPATH', output)
                self.assertIn('bash_op_test', output)
                self.assertIn('echo_env_vars', output)
                self.assertIn(DEFAULT_DATE.isoformat(), output)
                self.assertIn('manual__' + DEFAULT_DATE.isoformat(), output)

    def test_return_value(self):
        bash_operator = BashOperator(
            bash_command='echo "stdout"',
            task_id='test_return_value',
            xcom_push=True,
            dag=None
        )
        return_value = bash_operator.execute(context={})

        self.assertEqual(return_value, 'stdout')

    def test_task_retries(self):
        bash_operator = BashOperator(
            bash_command='echo "stdout"',
            task_id='test_task_retries',
            retries=2,
            dag=None
        )

        self.assertEqual(bash_operator.retries, 2)

    def test_default_retries(self):
        bash_operator = BashOperator(
            scripts=['''echo hello'''],
            task_id='1_23613',
            type="shell",
            ssh_command="",
            dag=None,
            files=[],
            workdir="/Users/shareit/work"
        )
        ti = TaskInstance.get_taskinstance_durations_by_task_name(dag_id="1_23613", task_id="1_23613")[0]
        print ti
        context = {}
        context["execution_date"] = ti.execution_date
        context["ti"] = ti
        bash_operator.pre_execute(context=context)
        bash_operator.execute(context=context)


    def test_default_retries(self):
        dag_id = "1_23689"
        ti = TaskInstance.get_taskinstance_durations_by_task_name(dag_id=dag_id, task_id=dag_id)[0]
        print ti
        dag = task_manager.get_dag(dag_id=dag_id)
        task = dag.get_task(task_id=dag_id)
        context = {}
        context["execution_date"] = ti.execution_date
        context["ti"] = ti
        task.pre_execute(context=context)
        task.execute(context=context)

    def test_cloud_get(self):
        region, provider = NormalUtil.parse_cluster_tag("type:k8s,region:ue1,sla:normal,rbac.cluster:bdp-prod,provider:aws")
        artifact_home = ConstantConfigUtil.get_string('artifact_home',
                                                      '{},{}'.format(provider, region))
        cloud_path = "{}/{}".format(artifact_home.rstrip("/"), "123123123.txt")
        print cloud_path
        log_provider = conf.get('job_log', 'log_provider')
        log_region = conf.get('job_log', 'log_region')
        cloud = CloudUtil(provider=log_provider, region=log_region)
        cloud.s3.upload_with_content_type("asd", "asda", "Asdad", content_type='text/plain')


    def test_olap(self):
        conn = jaydebeapi.connect("com.facebook.presto.jdbc.PrestoDriver", "jdbc:presto://olap-service-direct-prod.ushareit.org:443/hive/default",
                                      ["",""],"/Users/shareit/datastudio-pipeline/scripts/ci/kubernetes/docker/kyuubi-hive-jdbc-shaded-1.7.0.jar")
        cursor = conn.cursor()
        sql = '''--conf bdp-query-engine=spark-submit-sql-3\n CREATE TABLE IF NOT EXISTS iceberg.zll_test.datacake_activetable_has_pk (activeName STRING COMMENT '', activeType STRING COMMENT '', creatTime STRING COMMENT '', id INT COMMENT '', activeName1 STRING COMMENT '') USING iceberg LOCATION "s3://ads-cold-ue1/table_location/ue1/zll_test/datacake_activetable_has_pk"'''
        cursor.execute(sql)
        result_set = cursor.fetchall()
        print result_set
        cursor.close()
        conn.close()

    def test_other_jpype(self):
        import jpype
        jdbc_driver_path = '/Users/shareit/datastudio-pipeline/scripts/ci/kubernetes/docker/presto-jdbc-346.jar'
        jdbc_username = ''
        jdbc_password = ''
        jpype.startJVM(jpype.getDefaultJVMPath(), '-Djava.class.path=' + jdbc_driver_path)
        java_sql_DriverManager = jpype.java.sql.DriverManager
        jdbc_url = ''
        conn = java_sql_DriverManager.getConnection(jdbc_url, jdbc_username, jdbc_password)
        sql = '''--conf bdp-query-engine=spark-submit-sql-3\n CREATE TABLE IF NOT EXISTS iceberg.zll_test.datacake_activetable_has_pk (activeName STRING COMMENT '', activeType STRING COMMENT '', creatTime STRING COMMENT '', id INT COMMENT '', activeName1 STRING COMMENT '') USING iceberg LOCATION "s3://ads-cold-ue1/table_location/ue1/zll_test/datacake_activetable_has_pk"'''
        stmt = conn.createStatement()
        stmt.executeQuery(sql)

        conn.close()
        jpype.shutdownJVM()

















