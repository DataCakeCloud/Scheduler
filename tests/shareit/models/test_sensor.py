import unittest

import pendulum

from airflow.shareit.models.sensor_info import SensorInfo


class SensorTest(unittest.TestCase):

    def test_close(self):
        SensorInfo.close_sensors('32_56245',pendulum.parse('2024-01-04 00:00:00'))