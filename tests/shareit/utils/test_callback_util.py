# -*- coding: utf-8 -*-

import unittest
import pendulum

from datetime import datetime, timedelta
from airflow.shareit.utils.alarm_adapter import build_backfill_process_reminder_content, build_backfill_handle_content
from airflow.shareit.utils.callback_util import CallbackUtil
from airflow.utils import timezone
from airflow.utils.timezone import beijing


class CallbackTest(unittest.TestCase):
    def test_callback_post(self):
        execution_date = '2022-10-19 16:23:22.591'
        execution_date=pendulum.parse(execution_date)
        url="https://ds-pipeline-test.ushareit.org/pipeline/test/callback"
        data = {"state":"running","args":"1111"}
        print CallbackUtil.callback_post_guarantee(url,data,"diagnose_daily_03_1",execution_date)
