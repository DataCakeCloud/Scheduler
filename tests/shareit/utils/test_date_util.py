import datetime
import time
import unittest

import pendulum

from airflow.shareit.utils.date_util import DateUtil
from airflow.shareit.utils.normal_util import NormalUtil
from airflow.utils import timezone
from airflow.utils.timezone import beijing


class TestDateUtil(unittest.TestCase):
    def test_date_transform(self):
        date = pendulum.parse("2022/11/02 19:37:35.810").replace(tzinfo=beijing)
        # now = timezone.utcnow().replace(tzinfo=beijing)
        # print now.microsecond
        # print DateUtil.format_millisecond(now)
        # now = now.replace(microsecond=111111)
        # print now.strftime('%f')
        # now = datetime.datetime.strptime(now.strftime('%Y-%m-%d %H:%M:%S.%f'),'%Y-%m-%d %H:%M:%S.%f')
        # print now.microsecond
        # print now.strftime('%f')
        # date = date.replace(tzinfo=pendulum.timezone('Asia/Shanghai'))
        # print date.timetuple()
        # print int(time.mktime(date.timetuple()) * 1000 + date.microsecond / 1000)
        # return int((date - datetime.datetime(1970, 1, 1, tzinfo=pendulum.timezone('UTC'))).total_seconds()) * 1000
        print DateUtil.get_timestamp(date)

    # def test_date_compare(self):
    #     a=
    #     DateUtil.date_compare()

    def test_format_millisecond_random(self):
        date = DateUtil.format_millisecond(timezone.utcnow().replace(tzinfo=beijing))
        a = DateUtil.get_timestamp(DateUtil.format_millisecond(date))
        print a
        # a = DateUtil.get_timestamp(DateUtil.format_millisecond_random(date))
        # print a
        # a = DateUtil.get_timestamp(DateUtil.format_millisecond_random(date))
        # print a
        #

    def test_cluster_tag_mapping_replace(self):
       print  NormalUtil.cluster_tag_mapping_replace("type:k8s,region:ue1,sla:normal,provider:aws,rbac.cluster:bdp-prod")

    def test_date_format_ready_time_copy(self):
        date = DateUtil.format_millisecond(timezone.utcnow().replace(tzinfo=beijing))
        print DateUtil.date_format_ready_time_copy(date,'00:00','7')

    def test_split_date_range(self):
        start_date = pendulum.parse("2023/01/02 00:00:00")
        end_date = pendulum.parse("2023/12/03 00:00:00")
        for start,end in DateUtil.split_date_range(start_date,end_date):
            print start,end