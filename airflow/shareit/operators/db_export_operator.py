# -*- coding: utf-8 -*-
"""
author: zhangtao
Copyright (c) 2020-08-26 Shareit.com Co., Ltd. All Rights Reserved.
"""

from airflow.operators.genie_job_operator import GenieJobOperator
from airflow.utils.decorators import apply_defaults


class DBExportOperator(GenieJobOperator):
    """
    Export data from relational database
    """

    @apply_defaults
    def __init__(self,
                 k8s_config_map_name,
                 table,
                 output,
                 fetch_size=0,
                 spark_job_name=None,
                 spark_jar_path=None,
                 spark_conf="",
                 command_tags="type:spark-submit",
                 k8s_config_map_path='/mnt/configmap/jdbc/export/',
                 k8s_volume_config_map_name='jdbc-configmap',
                 k8s_config_map_file_name='config.properties',
                 *args,
                 **kwargs):
        """
        :param k8s_config_map_name: configmap name in cluster,which contains url driver user password of JDBC
        :param table:The JDBC table that should be read from,it support SQL query
        :param output: output path for exported data
        :param fetch_size:The JDBC fetch size, which determines how many rows to fetch per round trip. This can help
            performance on JDBC drivers which default to low fetch size (eg. Oracle with 10 rows). This option applies
            only to reading.
        :param spark_job_name:spark job name
        :param spark_jar_path:spark jar path
        :param spark_conf:spark conf ,eg:"--conf spark.driver.memory=2G --conf spark.executor.instances=10"
        :param k8s_config_map_path: mount path in work pod
        :param k8s_volume_config_map_name: spark conf argument.
        :param k8s_config_map_file_name:mount file name in pod
        """
        super(DBExportOperator, self).__init__(*args, **kwargs)
        self.command_tags = command_tags
        self.k8s_config_map_name = k8s_config_map_name
        self.table = table
        self.output = output
        self.fetch_size = fetch_size
        self.spark_job_name = spark_job_name
        self.spark_jar_path = spark_jar_path
        self.spark_conf = spark_conf
        self.k8s_config_map_path = k8s_config_map_path
        self.k8s_volume_config_map_name = k8s_volume_config_map_name
        self.k8s_config_map_file_name = k8s_config_map_file_name
        self.config_map_file_path = self.k8s_config_map_path + k8s_config_map_file_name
        if not spark_job_name:
            self.spark_job_name = "DBExporter"
        if not spark_jar_path:
            self.spark_jar_path = 'obs://bdp-deploy-sg/BDP/BDP-spark-db-etl/prod/db-etl-1.1.0-SNAPSHOT.jar'
        self.command = self._render_command()

    def _render_command(self):
        command = """ \
                        --deploy-mode cluster \
                        --name %s \
                        --conf spark.kubernetes.driver.volumes.configMap.%s.mount.path=%s \
                        --conf spark.kubernetes.driver.volumes.configMap.%s.options.configMapName=%s \
                        %s \
                        --class com.ushareit.data.etl.jdbc.DbExporter \
                        %s \
                        --table %s \
                        --fetchsize %s \
                        --configFile %s \
                        --output %s \
                """ % (self.spark_job_name,
                       self.k8s_volume_config_map_name,
                       self.k8s_config_map_path,
                       self.k8s_volume_config_map_name,
                       self.k8s_config_map_name,
                       self.spark_conf,
                       self.spark_jar_path,
                       self.table,
                       self.fetch_size,
                       self.config_map_file_path,
                       self.output
                       )
        return command
