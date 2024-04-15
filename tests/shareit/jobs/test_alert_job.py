import unittest

from airflow.shareit.jobs.alert_job import AlertJob, callback_processor


class alertJobTest(unittest.TestCase):
    def test_alert_job(self):
        AlertJob().run()

    def test_callback_job(self):
        callback_processor()

    def test_scheduled_task_notify(self):
        job = AlertJob()
