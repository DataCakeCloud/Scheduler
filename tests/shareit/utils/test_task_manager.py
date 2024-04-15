# -*- coding: utf-8 -*-
"""
author: zhangtao
Copyright (c) 2021/5/24 Shareit.com Co., Ltd. All Rights Reserved.
"""
import time

import pendulum

from airflow.models import TaskInstance
from airflow.shareit.utils.task_manager import analysis_task, analysis_dag, format_dag_params, get_dag, \
    get_dag_by_disguiser
import unittest
from datetime import datetime, timedelta


"""

"""
task_dict = {
    "name": "DataPipeline_test_task",  # 全局唯一
    "emails": "zhangtao@ushareit.com,si_data@ushareit.com",  # 报警邮件邮箱
    "owner": "zhangtao",
    "retries": 3,
    "input_datasets": [
        {
            "id": 1,  # 从元数据模块查询出的元数据ID
            "ready_time": "",  # 内部数据不需要，为空
            "granularity": "HOUR",  # HOUR/DAY/WEEK/MONTH/YEAR/OTHER
            "offset": 1  # 偏移量
        },
        {
            "id": 2,
            "ready_time": "30 * * * *",  # 仅外部数据需要
            "granularity": "DAY",
            "offset": 15
        }
    ],
    "output_datasets": [4, 5],
    "property_weight": 3,  # 优先级权重
    "deadline": "0 9 * * *",  # 用户设定的最晚完成时间，后续开发使用
    "extra_params": {},  # 其余参数，需要的时候传递，KV
    "task_items": [
        {
            "command": "aaaaa",
            "cluster_tags": "type:k8s,region:us-east-1,provider:aws,sla:normal,user:zhangtao",
            "command_tags": "type:hadoop",
            "task_type": "GenieJobOperator"
        },
        {
            "task_id": "dqc_task",
            "command": "bbbbb",
            "cluster_tags": "type:k8s,region:us-east-1,provider:aws,sla:normal,user:zhangtao",
            "command_tags": "type:hadoop",
            "task_type": "GenieJobOperator"
        },
        {
            "image": "swr.ap-southeast-3.myhuaweicloud.com/shareit-bdp/airflow-business:email_test2",
            "cluster_tags": "type:k8s,region:us-east-1,provider:aws",
            "task_id": "test_task",
            "task_type": "ScriptOperator",
            "scripts": """python shell.py -d 's3://XXXX,s3://XXXX' -cmds 'sh a.sh && echo hello'""",
            "files": ["obs://asdfasd/asdfawdsfa.test", "obs://asdfasdf", "http://dsfzhahfgaouhsfgdo/asdfh.test"],
            "volume_mounts": [{
                "name": "ldap-configmap",
                "mountPath": "/etc/ldap/ldap.conf",
                "subPath": "ldap.conf"
            },
                {
                    "name": "rbac-configmap",
                    "mountPath": "/work/airflow/webserver_config.py",
                    "subPath": "webserver_config.py"
                }],
            "volumes": [{
                "name": "rbac-configmap",
                "configMap": {
                    "name": "rbac-configmap"
                }
            },
                {
                    "name": "ldap-configmap",
                    "configMap": {
                        "name": "ldap-configmap"
                    }
                }],
            "env_vars": "",
            "configmaps": "",
            "labels": "",
            "annotations": "",
            "arguments": "",
            "affinity": "",
            "node_selectors": "",
            "tolerations": "",
            "image_pull_policy": 'IfNotPresent',
        }
    ]
}


class TaskManagerTest(unittest.TestCase):

    def test_get_gray_dag(self):
        dag = get_dag('1_23604')
        tasks = dag.tasks
        for t in tasks:
            print t.cluster_tags

    def test_get_dag(self):
        # a = time.clock()
        exe_date = pendulum.parse('2023-02-14 10:49:15.962000000')
        actual_dag_id,is_disguiser = TaskInstance.get_actual_dag_id(task_name='1_17393',execution_date=exe_date)
        print actual_dag_id
        print is_disguiser
        if is_disguiser:
            dag = get_dag_by_disguiser(actual_dag_id)
        else :
            dag = get_dag(actual_dag_id)
        # b = time.clock()
        tasks = dag.tasks
        print str(tasks[0])
        print tasks[0].job_name

    def test_analysis_dag(self):
        a = time.clock()
        dag = analysis_dag(task_dict)
        b = time.clock()
        print (a, b, b - a)
        # print (dag.dag_id)
        # print (dag.task_dict)

    def test_generate_extra_dataset(self):
        # from airflow.shareit.models.dag_dataset_relation import DagDatasetRelation
        # res = DagDatasetRelation.get_all_outer_dataset()
        # print (res)
        from airflow.shareit.utils.task_util import TaskUtil
        tu = TaskUtil()
        tu.check_outer_dataset()

    def test_DSP_get_latest_external_data(self):
        from airflow.shareit.models.dataset_partation_status import DatasetPartitionStatus
        res = DatasetPartitionStatus.get_latest_external_data(metadata_id="8ea6ccf6-5c66-49b9-afb5-8dc5adcbdb74")
        print res

# def largestRectangleArea(self, heights):
#     if len(heights) <=0:
#         return 0
#     stack = []
#     for i,height in enumerate(heights):
#
# if __name__ == "__main__":
#     from airflow.shareit.operators.email_operator import EmailOperator
#     from airflow.operators.genie_job_operator import GenieJobOperator
#     op = __import__("airflow.shareit.operators.email_operator")
#     import importlib
#     op = importlib.import_module("airflow.shareit.operators.email_operator")
#     class_t = getattr(op,"EmailOperator")
#     setattr(class_t,"task_id","this_is_a_test_id")
#     setattr(class_t,"namespace","this_is_a_namespae")
#     setattr(class_t,"aaabbbczhangt","test a test ")
#     arg = {}
#     class_t.__init__()
#     print("helo")

    def test_anaylise_date(self):
        a='2022-08-14 20:00:00'
        # b = analysis_datetime(a)
        # print b