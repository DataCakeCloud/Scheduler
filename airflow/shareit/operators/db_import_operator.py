# -*- coding: utf-8 -*-

#  Copyright (c) 2020 Shareit.com Co., Ltd. All Rights Reserved.

# !/usr/bin/env python

"""
@Project    :   Airflow of SHAREit
@File       :   db_import_operator.py
@Date       :   2020/6/18 7:26 下午
@Author     :   Yunqing Mo
@Concat     :   moyq@ushareit.com
@Company    :   SHAREit
@Desciption :   None
"""

from airflow.operators.genie_job_operator import GenieJobOperator
from airflow.utils.decorators import apply_defaults


class DBImportOperator(GenieJobOperator):
    """
    import from filesystem to relational database
    :param table: target table
    :param input_path: input path
    :param input_type: input file type only support [parquet|csv|json],default parquet
    :param batchsize: batch size,spark jdbc config,default 1000
    :param driver: relational database drive,default com.mysql.jdbc.Driver
    :param truncate: can truncate table,default false
    :param add_columns: add columns,you can add column to target table,such as "a=1||b=2||c='test'",default empty
    :param delete_columns: delete columns,you can delete column from input path, such as "a,b,c", default empty
    :param query_sql: import sql,you can specify sql when import to target table,
              such as "select a,b,c from target_table", default empty
    :param preSql: preSql will be executed before import default None
    :param postSql: postSql will be execute after import default None
    :param header: handle first row as header if header is "true" when csv input_type, default "true"
    :param schema: specify schema when import default None
    :param configmap: configmap of k8s you config
    :param configmap_path: configmap path,default "/mnt/configmap/jdbc/"
    :param configmap_file: configmap file,default "config.properties"
    :param spark_conf: spark conf, such as  "--conf spark.executor.instances=3",default empty
    :param args: custom args
    :param kwargs: custom kwargs
    """

    @apply_defaults
    def __init__(self,
                 table=None,
                 input_path=None,
                 basePath=None,
                 input_type="parquet",
                 batchsize=1000,
                 driver=None,
                 truncate=None,
                 add_columns=None,
                 delete_columns=None,
                 delimiter=None,
                 query_sql=None,
                 preSql=None,
                 postSql=None,
                 header="true",
                 schema=None,
                 configmap=None,
                 task_id="DBImorterEtl",
                 configmap_path="/mnt/configmap/jdbc/",
                 configmap_file="config.properties",
                 spark_conf="",
                 *args, **kwargs):
        super(DBImportOperator, self).__init__(task_id=task_id, *args, **kwargs)
        self.table = table
        self.input_path = input_path
        self.basePath = basePath
        self.input_type = input_type
        self.batchsize = batchsize
        self.driver = driver
        self.truncate = truncate
        self.add_columns = add_columns
        self.delete_columns = delete_columns
        self.delimiter = delimiter
        self.query_sql = query_sql
        self.spark_conf = spark_conf
        self.preSql = preSql
        self.postSql = postSql
        self.header = header if header and header != "" else 'true'
        self.schema = schema
        self.command_type = 'spark-submit'
        self.spark_jar_path = "db-etl-1.1.0-SNAPSHOT.jar"
        self.dependencies = "s3://shareit.deploy.us-east-1/BDP/BDP-spark-db-etl/prod/{jar}".format(
            jar=self.spark_jar_path)
        self.configmap = configmap
        self.configmap_path = configmap_path
        self.configmap_file = configmap_file
        self.command = self._build_command()

    def _add_param(self, param, value):
        """
        添加参数
        :param param: 参数名
        :param value: 参数值
        :return:
        """
        if value is None:
            return ""
        value = str(value).strip()
        if value == "":
            return ""
        return "--{param} {value} ".format(param=param, value=value)

    def valid_params(self):
        """
        验证输入参数
        :return:
        """

        def do_valid(input_param, input_param_name):
            if input_param is None or input_param == "":
                raise Exception("param [%s] can not empty." % input_param_name)

        do_valid(self.table, "table")
        do_valid(self.input_path, "input_path")
        do_valid(self.configmap, "configmap")
        do_valid(self.genie_conn_id, "genie_conn_id")
        if self.cluster_tags is None:
            do_valid(self.cluster, "cluster")
            do_valid(self.cluster_type, "cluster_type")

    def _build_command(self):
        """
        生存spark-submit命令
        :return:
        """
        def check_param(param):
            if isinstance(param,str):
                if param and not param.startswith("'") and not param.startswith('"') and not param.isspace() and param != "":
                    res = '"{0}"'.format(param)
                    return res
            return param

        _sql = check_param(self.query_sql)
        add_columns = check_param(self.add_columns)
        delete_columns = check_param(self.delete_columns)
        pre_sql = check_param(self.preSql)
        post_sql = check_param(self.postSql)
        schema = check_param(self.schema)
        delimiter = self.delimiter
        if self.delimiter is not None and not self.delimiter.startswith("'") \
            and not self.delimiter.startswith('"'):
            delimiter = '"{0}"'.format(self.delimiter)
            delimiter = delimiter.replace("\t", "\\t")
        self.valid_params()
        # config file
        conection_file = "%s%s" % (self.configmap_path, self.configmap_file)
        command_str = "{spark_conf} " \
                      "--deploy-mode cluster " \
                      "--conf spark.kubernetes.driver.volumes.configMap.jdbc-configmap.mount.path={configmap_path} " \
                      "--conf spark.kubernetes.driver.volumes.configMap.jdbc-configmap.options.configMapName={configmap} " \
                      "--name DBImporter " \
                      "--class com.ushareit.data.etl.jdbc.DBImporter " \
                      "{spark_jar_path} {table}" \
                      "{input_path}" \
                      "{basePath}" \
                      "{input_type}" \
                      "{batchsize}" \
                      "{drive}" \
                      "{truncate}" \
                      "{add_columns}" \
                      "{delete_columns}" \
                      "{delimiter}" \
                      "{query_sql}" \
                      "{preSql}" \
                      "{postSql}" \
                      "{header}" \
                      "{schema}" \
                      "{connection_file}".format(spark_conf=self.spark_conf,
                                                 configmap_path=self.configmap_path,
                                                 configmap=self.configmap,
                                                 spark_jar_path=self.spark_jar_path,
                                                 table=self._add_param("table", self.table),
                                                 input_path=self._add_param("input", self.input_path),
                                                 basePath=self._add_param("basePath", self.basePath),
                                                 input_type=self._add_param("inputtype", self.input_type),
                                                 batchsize=self._add_param("batchsize", self.batchsize),
                                                 drive=self._add_param("drive", self.driver),
                                                 truncate=self._add_param("truncate", self.truncate),
                                                 add_columns=self._add_param("add", add_columns),
                                                 delete_columns=self._add_param("delete", delete_columns),
                                                 delimiter=self._add_param("delimiter", delimiter),
                                                 query_sql=self._add_param("sql", _sql),
                                                 preSql=self._add_param("preSql", pre_sql),
                                                 postSql=self._add_param("postSql", post_sql),
                                                 header=self._add_param("header", self.header),
                                                 schema=self._add_param("schema", schema),
                                                 connection_file=self._add_param("configFile", conection_file)
                                                 )
        return command_str
