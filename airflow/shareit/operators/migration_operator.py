# -*- coding: utf-8 -*-

#  Copyright (c) 2020 Shareit.com Co., Ltd. All Rights Reserved.

# !/usr/bin/env python

"""
@Project    :   Airflow of SHAREit
@File       :   db_import_operator.py
@Date       :   2020/7/8 7:26 下午
@Author     :   Yunqing Mo
@Concat     :   moyq@ushareit.com
@Company    :   SHAREit
@Desciption :   None
"""

from airflow.operators.genie_job_operator import GenieJobOperator
from airflow.utils.decorators import apply_defaults


class MigrationOperator(GenieJobOperator):

    @apply_defaults
    def __init__(self,
                 srcPath=None,
                 distPath=None,
                 spark_conf="",
                 s3Region="ap-southeast-1",
                 acrossCloud=True,
                 task_id="Migration-task",
                 *args, **kwargs):
        """
        data migration operator
        :param srcPath: source path
        :param distPath: dist path
        :param s3Region: s3 region
        :param acrossCloud: whether across cloud
        :param args: custom params
        :param kwargs: custom params
        """

        super(MigrationOperator, self).__init__(task_id=task_id, *args, **kwargs)
        self.command_type = 'spark-submit'
        if (acrossCloud):
            self.cluster_type = 'migration'
            self.cluster = None
            self.cluster_tags = None
            self.command_tags = None
        self.acrossCloud = acrossCloud
        self.srcPath = srcPath
        self.distPath = distPath
        self.s3Region = s3Region
        self.jar = "ushareit-bigdata-migration-1.0-SNAPSHOT.jar"
        self.main = "com.ushareit.data.migration.DistCpMigration"
        self.dependencies = ['s3://shareit.deploy.us-east-1/BDP/BDP-migration/{jar}'.format(jar=self.jar)]
        self.spark_conf = spark_conf
        self.command = self._build_command()


    def valid_params(self):
        """
        valid input params
        :return:
        """

        def do_valid(input_param, input_param_name):
            if input_param is None or input_param == "":
                raise Exception("param [%s] can not empty." % input_param_name)

        do_valid(self.srcPath, "srcPath")
        do_valid(self.distPath, "distPath")
        do_valid(self.s3Region, "s3Region")
        do_valid(self.genie_conn_id, "genie_conn_id")
        do_valid(self.cluster_type, "cluster_type")

    def _build_command(self):
        """
        generator command
        :return:
        """
        self.valid_params()
        if self.spark_conf is None:
            self.spark_conf = ""
        return """ --deploy-mode cluster --name migration-task {spark_conf} --class {main} {jar} --acrossCloud {across_cloud}  {srcPath}  {distPath}  {s3Region} """ \
            .format(jar=self.jar, main=self.main, across_cloud=self.acrossCloud, srcPath=self.srcPath,
                    distPath=self.distPath, s3Region=self.s3Region, spark_conf=self.spark_conf)
