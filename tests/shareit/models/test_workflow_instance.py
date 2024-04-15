# -*- coding: utf-8 -*-
"""
author: zhangtao
Copyright (c) 2021/8/5 Shareit.com Co., Ltd. All Rights Reserved.
"""
import unittest

from airflow.shareit.models.workflow_instance import WorkflowInstance


class WorkflowInstanceTest(unittest.TestCase):

    def test_check_has_instance(self):
        print WorkflowInstance.check_has_instance(24)
