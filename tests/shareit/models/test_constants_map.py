# -*- coding: utf-8 -*-
"""
author: zhangtao
Copyright (c) 2021/8/5 Shareit.com Co., Ltd. All Rights Reserved.
"""
import unittest


class ContantsMapTest(unittest.TestCase):

    def test_get_value(self):
        from airflow.shareit.models.constants_map import ConstantsMap
        ConstantsMap.get_value(_type="log_url", key="us-east-1")

    def test_get_value_to_dict(self):
        from airflow.shareit.models.constants_map import ConstantsMap
        print ConstantsMap.get_value_to_dict(_type="spark_appurl")