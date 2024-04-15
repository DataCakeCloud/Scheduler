# -*- coding: utf-8 -*-
"""
author: zhangtao
Copyright (c) 2020-02-19 Shareit.com Co., Ltd. All Rights Reserved.
"""
from datetime import timedelta
from airflow.operators.genie_pod_operator import GeniePodOperator
from airflow.utils.decorators import apply_defaults

try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse


class ExportSharestoreOperator(GeniePodOperator):
    """
    Export sharestore in a Kubernetes Pod

    """

    @apply_defaults
    def __init__(self,
                 cluster_backup,
                 segment_backup,
                 backup_db="backup_db_obj",
                 pyfile="/data/sharestore_admin/sharestore_admin.py",
                 zk_str=None,
                 rate_limit_mb=None,
                 rest_endpoint=None,
                 output_path=None,
                 output_platform=None,
                 output_region=None,
                 image_pull_policy=None,
                 terminationGracePeriodSeconds=3000,
                 preStop_exec_cmds=None,
                 release_lock_delay_sec=600,
                 *args,
                 **kwargs):
        super(ExportSharestoreOperator, self).__init__(*args, **kwargs)
        if not (cluster_backup and segment_backup and zk_str
                and rate_limit_mb and rest_endpoint and output_path
                and output_platform and output_region):
            raise ValueError("Missing ShareStore parameters")

        input_path_parse = urlparse(output_path)
        input_bucket = input_path_parse.netloc
        input_prefix = input_path_parse.path.lstrip('/')

        zk_str_list = ['--zk_str', zk_str]

        rate_limit_mb_list = ['--rate_limit_mb', rate_limit_mb]

        rest_endpoint_list = ["--rest_endpoint", rest_endpoint]
        output_platform_list = ["--platform", output_platform]
        output_region_list = ["--region", output_region]

        if image_pull_policy is None:
            image_pull_policy = 'Always'

        cmd = ["python", pyfile]

        extra_arg = []
        if len(kwargs) > 0:
            for key in kwargs:
                if key.startswith('sharestore_'):
                    extra_arg.append('--' + str(key[11:]))
                    extra_arg.append(kwargs[key])

        self.cmds = cmd + rest_endpoint_list + zk_str_list + [cluster_backup] + [backup_db] + [segment_backup] + [input_bucket] + [input_prefix] + rate_limit_mb_list + output_platform_list + output_region_list + extra_arg
        self.image_pull_policy = image_pull_policy
        self.pool = cluster_backup
        self.retry_delay = timedelta(seconds=release_lock_delay_sec + 180)
        self.terminationGracePeriodSeconds = terminationGracePeriodSeconds
        if not preStop_exec_cmds:
            self.preStop_exec_cmds = ["python",
                                      "/data/sharestore_admin/sharestore_admin.py",
                                      cluster_backup,
                                      "release_cluster",
                                      "--delay_sec ",
                                      release_lock_delay_sec]
