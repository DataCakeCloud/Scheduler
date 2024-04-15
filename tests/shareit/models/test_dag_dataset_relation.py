# -*- coding: utf-8 -*-
"""
author: zhangtao
Copyright (c) 2021/6/29 Shareit.com Co., Ltd. All Rights Reserved.
"""
import time

from airflow.shareit.utils.task_manager import analysis_task, analysis_dag, format_dag_params
import unittest
from datetime import datetime, timedelta
from airflow.shareit.models.dag_dataset_relation import DagDatasetRelation


class DagDatasetRelationTest(unittest.TestCase):

    def test_get_self_reliance_dataset(self):
        res = DagDatasetRelation.get_self_reliance_dataset()
        print (res)


