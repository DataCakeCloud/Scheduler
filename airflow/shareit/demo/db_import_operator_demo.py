# -*- coding: utf-8 -*-

#  Copyright (c) 2020 Yunqing Mo Inc.  All rights reserved.

#!/usr/bin/env python

"""
@Project    :   Airflow of SHAREit
@File       :   db_import_operator_demo.py
@Date       :   2020/6/22 8:03 下午
@Author     :   Yunqing Mo
@Concat     :   moyq@ushareit.com
@Company    :   SHAREit
@Desciption :   None
"""

from airflow.shareit.operators.db_import_operator import DBImportOperator

dag=None
genie_conn_id="game_genie_test_default"
cluster="game-eks-test-us"
cluster_type="k8s"
spark_conf="""
--conf spark.executor.instances=3
                        --conf spark.executor.memory=2G
                        --conf spark.executor.memoryOverhead=1G
                        --conf spark.executor.cores=1
                        --conf spark.dynamicAllocation.enabled=false
"""


# operator with full params
operator_full_params = DBImportOperator(
    dag=dag,
    genie_conn_id=genie_conn_id,  # genie connection必填写
    cluster=cluster,  # 集群必填
    cluster_type=cluster_type,  # 集群类型
    spark_conf=spark_conf,  # spark配置
    configmap="game-dev-configmap", # configmap must specify
    configmap_path="/mnt/configmap/jdbc/",
    configmap_file="config.properties",
    table="dws_open_cnt_clickevents_d",  # 待导入的目标表 必须填写
    input_path="s3://game.data.us-east-1/openFolder/dws_open_cnt_clickEvents_d/client_date_ymd=20200612", # 导入path 必填
    input_type="parquet",  # s3或者obs等数据源的数据类型
    batchsize=1000,  # spark jdbc 插入batchsize
    driver="com.mysql.jdbc.Driver", # 数据库驱动
    truncate="false",  # 是否需要清空target表
    add_columns="client_date_ymd=20200612",  # 需要数据列, such as "a=1||b=2||c='test'"
    # delete_columns=None,  # 需要删除s3或者obs上对应的数据列, such as "a,b,c"
    query_sql="",  # 导入查询语句 such as "select a,b,c from my_table"
)


# operator with simple params
operator_simple_params = DBImportOperator(
    dag=dag,
    genie_conn_id=genie_conn_id,  # genie connection必填写
    cluster=cluster,  # 集群必填
    cluster_type=cluster_type,  # 集群类型
    configmap="game-dev-configmap", # configmap must specify
    table="dws_open_cnt_clickevents_d",  # 待导入的目标表 必须填写
    input_path="s3://game.data.us-east-1/openFolder/dws_open_cnt_clickEvents_d/client_date_ymd=20200612", # 导入path 必填
)
