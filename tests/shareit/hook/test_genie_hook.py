import datetime
import unittest

import pendulum

from airflow.hooks.genie_hook import GenieHook
from airflow.shareit.jobs.alert_job import AlertJob, callback_processor
from airflow.utils import timezone
from airflow.utils.timezone import beijing


class genieHookTest(unittest.TestCase):

    def test_get_job(self):
        gh = GenieHook()
        job = gh.get_job_info('70b46f44-7126-11ed-aa14-36f4ef4cd4e7')
        print job
        # status = job['status']
        # updated = job['updated']
        # updated = pendulum.parse(updated).replace(tzinfo=beijing) + datetime.timedelta(hours=8)
        # now_date = timezone.utcnow().replace(tzinfo=beijing)
        # print (now_date - updated).total_seconds()
        # print updated
