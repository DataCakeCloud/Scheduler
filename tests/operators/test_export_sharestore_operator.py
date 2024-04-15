# -*- coding: utf-8 -*-
"""
author: zhangtao
Copyright (c) 2020-02-20 Shareit.com Co., Ltd. All Rights Reserved.
"""

import unittest
from datetime import datetime, timedelta

from airflow.operators.export_sharestore_operator import ExportSharestoreOperator
from airflow.utils import timezone

DEFAULT_DATE = datetime(2016, 1, 1, tzinfo=timezone.utc)
END_DATE = datetime(2016, 1, 2, tzinfo=timezone.utc)
INTERVAL = timedelta(hours=12)


class ExportSharestoreOperatorTest(unittest.TestCase):

    def test_task_Pool(self):
        sharestore = ExportSharestoreOperator(
            task_id='sharestore_test',
            genie_conn_id='bdp_test',
            cluster='cluster',
            get_logs=True,
            is_delete_operator_pod=True,
            name='sharestore-test',
            namespace='bdp-test',
            image='swr.ap-southeast-3.myhuaweicloud.com/shareit-cbs/sharestore_admin:0.3',
            image_pull_secrets='default-secret',
            dag=None,
            configmaps=None,
            annotations=None,

            cluster_backup='sharestore-shareit',
            segment_backup='scas_item_counter_prod',
            backup_db='backup_db',
            hdfs_dir='/sharestore/backup/sharestore-shareit20200223-000200',
            zk_str='sg.zk1.sharestore.service:2181,sg.zk2.sharestore.service:2181,sg.zk3.sharestore.service:2181',
            rate_limit_mb='128',
            region='obs.ap-southeast-3.myhuaweicloud.com',
            load_mode='0',
            rest_endpoint='http://test.sharestore.cbs.sg2.helix/admin/v2/',
            sharestore_test_arg='test_arg'
        )
        # print sharestore.cmds
        self.assertEqual('sharestore-shareit', sharestore.pool)
