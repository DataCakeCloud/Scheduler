# -*- coding: utf-8 -*-

import unittest
from datetime import datetime

from airflow.models import DagRun
from airflow.shareit.models.gray_test import GrayTest
from airflow.utils import timezone


class GrayTestTest(unittest.TestCase):

    def test_close_gray_test(self):
        GrayTest.close_gray_test('1_23149')

    def test_update_task_name(self):
        GrayTest.update_task_name('1_23149','1_23140')

    def test_open_gray_test(self):
        GrayTest.open_gray_test('1_16623')