import unittest

from airflow.jobs import SchedulerJob


class schedulerJobTest(unittest.TestCase):
    def test_schedulerjob(self):
        SchedulerJob().run()

    def test_dict(self):
        # task_dict = {"a":1,"b":2}
        # print len(task_dict)
        a='xxxx_import_sharestore'
        if a.endswith('_import_sharestore'):
            print 1111