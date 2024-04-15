# -*- coding: utf-8 -*-
import json
import unittest

import pendulum

from airflow.shareit.models.event_trigger_mapping import EventTriggerMapping
from airflow.shareit.utils.uid.uuid_generator import UUIDGenerator
from airflow.utils.timezone import beijing


class EventTriggerMappingTest(unittest.TestCase):
    def test_add(self):
        task_name = '123456'
        uuid = UUIDGenerator.generate_uuid_no_index()
        template_params = json.dumps({'abc':'asdasd'})
        callback_info = json.dumps({'abc':'asdasd'})
        EventTriggerMapping.add(task_name, uuid,callback_info=callback_info,template_params=template_params)

    def test_update(self):
        uuid = '00390970-fdfb-11ed-8d28-401c83759c42'
        is_execute = True
        execution_date = pendulum.parse('2021-09-01 00:00:00.123456').replace(tzinfo=beijing)
        EventTriggerMapping.update(uuid,is_execute=is_execute,execution_date=execution_date)
