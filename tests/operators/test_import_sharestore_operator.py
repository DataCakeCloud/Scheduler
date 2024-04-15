# -*- coding: utf-8 -*-
"""
author: heshan
Copyright (c) 2020-02-14 Shareit.com Co., Ltd. All Rights Reserved.
"""

import unittest
from datetime import datetime, timedelta

from airflow.operators.import_sharestore_operator import ImportSharestoreOperator
from airflow.utils import timezone

DEFAULT_DATE = datetime(2016, 1, 1, tzinfo=timezone.utc)
END_DATE = datetime(2016, 1, 2, tzinfo=timezone.utc)
INTERVAL = timedelta(hours=12)


class ImportSharestoreOperatorTest(unittest.TestCase):

    def test_task_Pool(self):
        sharestore = ImportSharestoreOperator(
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
            startup_timeout_seconds=60 * 60 * 6,
            configmaps=None,
            annotations=None,
            cluster_load='sharestore-readonly',
            loadsst_version='load_sst_v2',
            segment_load='sprs_user_profile_recall_test',
            input_path='obs://XXXXXX/NeverDelete/XXXXX/import_sharestore/user_profile_parquet_test/datepart=20200220',
            pyfile="/data/sharestore_admin/sharestore_admin.py",
            rest_endpoint='http://test.sharestore.cbs.sg2.helix/admin/v2/',
            zk_str='test.sg.zk1.sharestore.service:2181,test.sg.zk2.sharestore.service:2181,test.sg.zk3.sharestore.service:2181',
            rate_limit_mb='128',
            region='obs.ap-southeast-3.myhuaweicloud.com',
            load_mode='0',
            sharestore_test_arg='test_arg',
            should_compact=False
        )
        print sharestore.cmds
        self.assertEqual('sharestore-readonly', sharestore.pool)
