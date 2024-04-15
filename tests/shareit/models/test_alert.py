# -*- coding: utf-8 -*-
import unittest
from datetime import datetime

import pendulum

from airflow.shareit.models.alert import Alert
from airflow.utils import timezone
from airflow.utils.timezone import beijing


class alertTest(unittest.TestCase):

    def test_add(self):
        execution_date = timezone.utcnow().replace(tzinfo=beijing)
        print execution_date
        # execution_date = pendulum.parse(execution_date).replace(tzinfo=pendulum.timezone("Asia/Shanghai"))
        Alert.add_alert_job('diagnose_daily_03_1',execution_date)

    def test_reopen_alert_job(self):
        exe_date = datetime(year=2022, month=10, day=10, hour=0,minute=0,second=0,tzinfo=timezone.beijing)
        Alert.reopen_alert_job('diagnose_daily_03_1',exe_date)

    def test_list_all_open_alert_job(self):
        all_job = Alert.list_all_open_alert_job()
        for j in all_job:
            print j.id

    def test_sync(self):
        exe_date = datetime(year=2022, month=10, day=10, hour=00).replace(tzinfo=timezone.beijing)
        Alert.sync_state('diagnose_daily_03_1',exe_date, state='finished')