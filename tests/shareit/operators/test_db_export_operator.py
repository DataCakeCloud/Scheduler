# -*- coding: utf-8 -*-
"""
author: zhangtao
Copyright (c) 2020/8/26 Shareit.com Co., Ltd. All Rights Reserved.
"""

import unittest
from datetime import datetime, timedelta

from airflow.operators.import_sharestore_operator import ImportSharestoreOperator
from airflow.shareit.operators.db_export_operator import DBExportOperator
from airflow.utils import timezone

DEFAULT_DATE = datetime(2016, 1, 1, tzinfo=timezone.utc)
END_DATE = datetime(2016, 1, 2, tzinfo=timezone.utc)
INTERVAL = timedelta(hours=12)


class DBExportOperatorTest(unittest.TestCase):

    def test_task_Pool(self):
        test_task = DBExportOperator(
            task_id='test_db_export_operator',
            genie_conn_id='bdp_test',
            cluster_tags="type:k8s,region:us-east-1,provider:aws",
            command_tags="type:spark-submit",
            spark_conf="--conf spark.driver.memory=2G --conf spark.executor.instances=10 --conf "
                       "spark.executor.memory=4G --conf spark.executor.cores=2",
            k8s_config_map_name="XXX-XXX-config",
            table="XXXX.XXXXX",
            output="obs://XXX-XXX-XXX/XXX/XXX/XXX/20200816",
        )
        print test_task.command
        print test_task.genie_conn_id
