# -*- coding: utf-8 -*-
"""
author: heshan
Copyright (c) 2020-02-14 Shareit.com Co., Ltd. All Rights Reserved.
"""
import re
from datetime import timedelta
from airflow.operators.genie_pod_operator import GeniePodOperator
from airflow.utils.decorators import apply_defaults

try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse


class ImportSharestoreOperator(GeniePodOperator):
    """
    import sharestore in a Kubernetes Pod

    """

    @apply_defaults
    def __init__(self,
                 cluster_load=None,
                 loadsst_version=None,
                 segment_load=None,
                 input_path=None,
                 pyfile="/data/sharestore_admin/sharestore_admin.py",
                 rest_endpoint=None,
                 zk_str=None,
                 rate_limit_mb=None,
                 region=None,
                 image_pull_secrets="default-secret",
                 load_mode=None,
                 image_pull_policy=None,
                 should_compact=False,
                 terminationGracePeriodSeconds=3000,
                 preStop_exec_cmds=None,
                 release_lock_delay_sec=600,
                 *args,
                 **kwargs):
        """
        :param cluster_load: 业务sharestore集群名，需要根据业务调整
        :param loadsst_version:
        :param segment_load:业务sharestore表名，需要根据业务调整
        :param input_path:parquet数据所在目录，需要根据业务调整
        :param rest_endpoint:helix地址，需要根据业务调整
        :param pyfile: python file name
        :param zk_str:sharestore zk集群地址，需要根据业务调整
        :param rate_limit_mb:
        :param region:
        :param load_mode:0为增量导入，1为全量导入
        :param args:
        :param kwargs:
        """
        super(ImportSharestoreOperator, self).__init__(*args, **kwargs)

        self.image_pull_secrets = image_pull_secrets

        rest_endpoint_list = []
        zk_str_list = []
        rate_limit_mb_list = []
        region_list = []
        load_mode_list = []
        should_compact_list = ["--should_compact", str(should_compact)]
        if rest_endpoint:
            rest_endpoint_list = ['--rest_endpoint', rest_endpoint]

        if zk_str:
            zk_str_list = ['--zk_str', zk_str]

        if rate_limit_mb:
            rate_limit_mb_list = ['--rate_limit_mb', rate_limit_mb]

        if region:
            region_list = ['--region', region]

        if load_mode:
            load_mode_list = ['--load_mode', load_mode]

        if image_pull_policy is None:
            image_pull_policy = 'Always'

        extra_arg = []
        if len(kwargs) > 0:
            for key in kwargs:
                if key.startswith('sharestore_'):
                    extra_arg.append('--' + str(key[11:]))
                    extra_arg.append(kwargs[key])

        cmd = ["python", pyfile]
        if input_path:
            input_path_parse = urlparse(input_path)
            input_bucket = input_path_parse.netloc
            input_prefix = input_path_parse.path.lstrip('/')
            sharestore_info = [cluster_load, loadsst_version, segment_load, input_bucket, input_prefix]

            self.cmds = cmd + rest_endpoint_list + zk_str_list + sharestore_info + rate_limit_mb_list + \
                        region_list + load_mode_list + should_compact_list + extra_arg
            self.image_pull_policy = image_pull_policy
            cluster_region = self.get_region_from_cluster_tag()
            self.pool = '{}.{}'.format(self.cluster_load,cluster_region) if cluster_region else self.cluster_load
            self.retry_delay = timedelta(seconds=release_lock_delay_sec + 180)
            self.terminationGracePeriodSeconds = terminationGracePeriodSeconds
        if not preStop_exec_cmds:
            self.preStop_exec_cmds = ["python",
                                      "/data/sharestore_admin/sharestore_admin.py",
                                      cluster_load,
                                      "release_cluster",
                                      "--delay_sec ",
                                      release_lock_delay_sec]

    def my_init(self):
        rest_endpoint_list = []
        zk_str_list = []
        rate_limit_mb_list = []
        region_list = []
        load_mode_list = []
        try:
            should_compact = self.should_compact if self.should_compact is not None else False
        except AttributeError as e:
            should_compact = False
        try:
            terminationGracePeriodSeconds = self.terminationGracePeriodSeconds if self.terminationGracePeriodSeconds else 3000
        except AttributeError as e:
            terminationGracePeriodSeconds = 3000
        try:
            release_lock_delay_sec = self.release_lock_delay_sec if self.release_lock_delay_sec else 3000
        except AttributeError as e:
            release_lock_delay_sec = 3000
        try:
            pyfile = self.pyfile if self.pyfile else "/data/sharestore_admin/sharestore_admin.py"
        except AttributeError as e:
            pyfile = "/data/sharestore_admin/sharestore_admin.py"

        should_compact_list = ["--should_compact", str(should_compact)]
        try:
            input_path_parse = urlparse(self.input_path)
            input_bucket = input_path_parse.netloc
            input_prefix = input_path_parse.path.lstrip('/')
            if self.rest_endpoint:
                rest_endpoint_list = ['--rest_endpoint', self.rest_endpoint]

            if self.zk_str:
                zk_str_list = ['--zk_str', self.zk_str]

            if self.rate_limit_mb:
                rate_limit_mb_list = ['--rate_limit_mb', self.rate_limit_mb]

            if self.region:
                region_list = ['--region', self.region]

            if self.load_mode:
                load_mode_list = ['--load_mode', self.load_mode]

            if self.image_pull_policy is None:
                self.image_pull_policy = 'Always'

            cmd = ["python", pyfile]
            sharestore_info = [self.cluster_load, self.loadsst_version, self.segment_load, input_bucket, input_prefix]

            self.cmds = cmd + rest_endpoint_list + zk_str_list + sharestore_info + rate_limit_mb_list + \
                        region_list + load_mode_list + should_compact_list
            cluster_region = self.get_region_from_cluster_tag()
            self.pool = '{}.{}'.format(self.cluster_load,cluster_region) if cluster_region else self.cluster_load
            self.retry_delay = timedelta(seconds=release_lock_delay_sec + 180)
            self.terminationGracePeriodSeconds = terminationGracePeriodSeconds

            self.preStop_exec_cmds = ["python",
                                      "/data/sharestore_admin/sharestore_admin.py",
                                      self.cluster_load,
                                      "release_cluster",
                                      "--delay_sec ",
                                      release_lock_delay_sec]
        except AttributeError as e1:
            raise e1

    def get_region_from_cluster_tag(self):
        pattern = re.compile('region:([^\s,:]+)')
        search = re.search(pattern, self.cluster_tags)
        region = None
        if search:
            region = search.group(1)
        return region