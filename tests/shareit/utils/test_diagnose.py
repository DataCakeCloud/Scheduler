# -*- coding: utf-8 -*-
import json
import unittest

import pendulum

from airflow.shareit.service.diagnosis_service import DiagnosisService


class DiagnoseTest(unittest.TestCase):

    def test_diagnose(self):
        task_id = 'test_time_scheduler_month_1'
        start_date = pendulum.parse("2022-05-12 02:00:00")
        new = DiagnosisService().diagnose(task_id, start_date, 'waiting')
        print json.dumps(new,ensure_ascii=False)


    def test_diagnose_waiting(self):
        task_id = 'test_dag_lock_1'
        start_date = pendulum.parse("2022-03-31 00:00:00")
        new = DiagnosisService().diagnose(task_id, start_date, 'waiting')
        print json.dumps(new,ensure_ascii=False)

        # task_id = 'test_diagnose_v2_root'
        # start_date = pendulum.parse("2022-03-21 00:00:00")
        # new = DiagnosisService().diagnose(task_id, start_date, 'waiting')
        # print json.dumps(new,ensure_ascii=False)

    def test_diagnose_failed(self):
        task_id = 'test_diagnose_v2_root'
        start_date = pendulum.parse("2022-03-16 00:00:00")
        new = DiagnosisService().diagnose(task_id, start_date, 'failed')
        print json.dumps(new,ensure_ascii=False)

    def test_diagnose_termination(self):
        task_id = 'test_diagnose_v2_root'
        start_date = pendulum.parse("2022-03-14 00:00:00")
        new = DiagnosisService().diagnose(task_id, start_date, 'termination')
        print json.dumps(new,ensure_ascii=False)

    def test_diagnose_success(self):
        task_id = 'test_diagnose_v2_root'
        start_date = pendulum.parse("2022-03-18 00:00:00")
        new = DiagnosisService().diagnose(task_id, start_date, 'success')
        print json.dumps(new,ensure_ascii=False)