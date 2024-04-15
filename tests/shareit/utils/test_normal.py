# -*- coding: utf-8 -*-
"""
author: zhangtao
Copyright (c) 2021/6/8 Shareit.com Co., Ltd. All Rights Reserved.
"""
import base64
import datetime
import json
import urllib

import requests

from airflow.utils.db import provide_session
from airflow.models.log import Log


# db.session.query(User.name).filter(User.email.like('%'+email+'%')).limit(page_size).offset((page_index-1)*page_size)

class TreeNode:
    def __init__(self, val, left, right):
        self.val = val
        self.left = left
        self.right = right


@provide_session
def test_sqlalchemy_split_by_page(session=None):
    page_size = 50
    page_index = 200
    res = session.query(Log).filter(Log.owner == "zhangtao").order_by(Log.dttm.desc()).limit(page_size).offset(
        (page_index - 1) * page_size).all()
    print(res)


def tes_datatime():
    from airflow.utils.timezone import beijing
    from datetime import datetime
    dd = datetime.utcnow()
    cc = datetime(year=2021, month=6, day=15, hour=3, minute=15, second=18)
    sec = (dd - cc).total_seconds()
    s = sec % 60
    q = sec // 60
    mi = int(q % 60)
    hou = int(q // 60)
    print ("{} h {} m {} s".format(hou, mi, s))
    print("end")


def test_json():
    test_dict = {"test_msg": u"hello world!"}
    aa = json.dumps(test_dict)
    print (isinstance(aa, str))


def test_scheduler_job():
    from airflow.jobs.scheduler_job import SchedulerJob
    tt = SchedulerJob()
    tt.process_file("DataStudio Pipeline", [])


def test_decode():
    strT = "asdfa识xxx/*这是一个4568451"
    res = strT.split("/*这是一个编码标识*/")
    print res


def test_external_data_processing():
    from airflow.shareit.utils.task_util import TaskUtil
    from airflow.utils.timezone import utcnow
    from airflow.shareit.utils.granularity import Granularity
    tu = TaskUtil()
    res = tu._get_indicate_datatime("00 22 * * *", datetime.datetime(year=2021, month=8, day=11, hour=21))
    print res


def test_main_scheduler_process():
    a = 'ZW52JTIwJTdCJTBBJTIwJTIwJTIwJTIwJTIyY2hlY2twb2ludC5pbnRlcnZhbCUyMiUzRDEwMDAwJTBBJTIwJTIwJTIwJTIwJTIyZXhlY3V0aW9uLnBhcmFsbGVsaXNtJTIyJTNEMSUwQSUyMCUyMCUyMCUyMCUyMmpvYi5tb2RlJTIyJTNEQkFUQ0glMEElN0QlMEFzaW5rJTIwJTdCJTBBJTIwJTIwJTIwJTIwSmRiYyUyMCU3QiUwQSUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMGRyaXZlciUzRCUyMmNvbS5teXNxbC5jai5qZGJjLkRyaXZlciUyMiUwQSUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMm1heF9yZXRyaWVzJTIyJTNEMCUwQSUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMHBhc3N3b3JkJTNEJTIycXdlMyUyNm5qTTY1JTIyJTBBJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIyc291cmNlX3RhYmxlX25hbWUlMjIlM0QlMjJvZHNfZHNfdXNlcl9iZWhhdmlvciUyMiUwQSUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMHVybCUzRCUyMmpkYmMlM0FteXNxbCUzQSUyRiUyRjEwLjMyLjEzLjEwOSUzQTkwMzAlMkZkZW1vJTIyJTBBJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwdXNlciUzRHRlc3QwNjA2JTBBJTIwJTIwJTIwJTIwJTdEJTBBJTdEJTBBc291cmNlJTIwJTdCJTBBJTIwJTIwJTIwJTIwSWNlYmVyZyUyMCU3QiUwQSUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMmNhdGFsb2dfbmFtZSUyMiUzRHNlYXR1bm5lbCUwQSUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMmNhdGFsb2dfdHlwZSUyMiUzRGhpdmUlMEElMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjBuYW1lc3BhY2UlM0RkYXRhY2FrZSUwQSUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMHF1ZXJ5JTNEJTIyc2VsZWN0JTIwdXNlcmlkJTJDZXZlbnQlMkNhcHBpZCUyQ3VybCUyQ3RpbWUlMkNldmVudF9pZCUyQ2V2ZW50X25hbWUlMkNyZXF1ZXN0X3VybCUyMGZyb20lMjBvZHNfZHNfdXNlcl9iZWhhdmlvcl90ZXN0JTIyJTBBJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIycmVzdWx0X3RhYmxlX25hbWUlMjIlM0QlMjJvZHNfZHNfdXNlcl9iZWhhdmlvcl90ZXN0JTIyJTBBJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwdGFibGUlM0QlMjJvZHNfZHNfdXNlcl9iZWhhdmlvcl90ZXN0JTIyJTBBJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwdXJsJTNEJTIydGhyaWZ0JTNBJTJGJTJGaG1zLXVlMS51c2hhcmVpdC5vcmclM0E5MDgzJTIyJTBBJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwd2FyZWhvdXNlJTNEJTIyczNhJTNBJTJGJTJGY2JzLmZsaW5rLnVzLWVhc3QtMSUyRm1haW4tdWUxLXByb2QlMkZldmVudC10cmFjay1sb2ctdGVzdCUyMiUwQSUyMCUyMCUyMCUyMCU3RCUwQSU3RCUwQXRyYW5zZm9ybSUyMCU3QiUwQSUyMCUyMCUyMCUyMHNxbCUyMCU3QiUwQSUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMHF1ZXJ5JTNEJTIyc2VsZWN0JTIwdXNlcmlkJTJDZXZlbnQlMkNhcHBpZCUyQ3VybCUyQ3RpbWUlMkNldmVudF9pZCUyQ2V2ZW50X25hbWUlMkNyZXF1ZXN0X3VybCUyMGZyb20lMjBvZHNfZHNfdXNlcl9iZWhhdmlvcl90ZXN0JTIwJTIyJTBBJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIycmVzdWx0X3RhYmxlX25hbWUlMjIlM0QlMjJvZHNfZHNfdXNlcl9iZWhhdmlvciUyMiUwQSUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMnNvdXJjZV90YWJsZV9uYW1lJTIyJTNEJTIyb2RzX2RzX3VzZXJfYmVoYXZpb3JfdGVzdCUyMiUwQSUyMCUyMCUyMCUyMCU3RCUwQSU3RCUwQQ=='
    base64Decode = urllib.unquote(base64.b64decode(a))
    print base64Decode
    b = base64.b64encode(urllib.quote(base64Decode))
    print b


if __name__ == "__main__":
    pass
