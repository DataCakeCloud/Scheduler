import unittest

from airflow.jobs import SchedulerJob, BaseJob


class baseJobTest(unittest.TestCase):
    def test_schedulerjob(self):
        BaseJob().reset_state_for_queued_tasks()
