# -*- coding: utf-8 -*-
import json
import unittest
from datetime import datetime

import pendulum

from airflow.shareit.models.alert import Alert
from airflow.shareit.models.callback import Callback
from airflow.utils import timezone
from airflow.utils.timezone import beijing


class alertTest(unittest.TestCase):

    def test_add(self):
        body = {"state":"running","args":"12313"}
        Callback.add_callback("https://ds-pipeline-test.ushareit.org/pipeline/test/callback",json.dumps(body),"test_update_and_execute","123")

    def test_list(self):
        res = Callback.list_running_callback()
        print res[0].callback_url

    def test_update(self):
        Callback.update_callback(id=45,state='failed',nums=10)
