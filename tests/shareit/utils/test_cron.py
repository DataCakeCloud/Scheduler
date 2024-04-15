import json
import re
import unittest
from datetime import datetime,timedelta

import jinja2

from airflow import macros
from airflow.models import TaskInstance
from airflow.shareit.utils.task_cron_util import TaskCronUtil

from airflow.utils import timezone
import pendulum
from croniter import croniter,croniter_range
import time
# from airflow.shareit.service.diagnosis_service import DiagnosisService
from airflow.utils.timezone import utcnow, make_naive, my_make_naive, beijing


class CronTest(unittest.TestCase):

    def testCrontab(self):
        from airflow import macros
        from datetime import datetime, timedelta
        beijing = pendulum.timezone("Asia/Shanghai")
        # crontab
        # execution_date = datetime(year=2022, month=5, day=16, hour=0, minute=0, second=0, tzinfo=beijing)
        # print execution_date  - macros.timedelta(hours=24) - macros.timedelta(hours=8) + macros.timedelta(hours=(20+8)%24)

        # next = cron.get_next(datetime)
        # next1 = cron.get_next(datetime)
        # next2 = cron.get_next(datetime)
        # print next
        # print next1
        # print next2
        # print prev
        # diff =  nowDate - startDate
        # print diff.total_seconds()
        startDate = datetime(year=2022, month=11, day=18, hour=0, minute=0, second=0, tzinfo=beijing)
        nowDate = datetime(year=2022, month=11, day=18 ,hour=23, minute=0, second=0, tzinfo=beijing)
        aaa = croniter_range(startDate,nowDate,'0 0/1 * * *')
        bbb = []
        for i in aaa :
            print i
            bbb.append(i)

        # def ccc(c):
        #     print 111
        #     print c
        #     for i in c :
        #         print i
        # ccc(bbb)

        # print  nowDate - timedelta(days=1)  if (nowDate - startDate).total_seconds() > 86400 else startDate - timedelta(seconds=1)
        # a={}
        # a['a']=1
        # a['b']=2
        # print len(a)
        # loop_start_time = time.time()
        # time.sleep(5)
        # loop_end_time = time.time()
        #
        # print loop_end_time - loop_start_time

    def test_exec(self):
        d1 = datetime(year=2022, month=12,day=1)
        # d2 = d1 + timedelta(months=+2)

        d1str = d1.strftime('%Y-%m-%d %H%M%S')

        print  datetime.strptime(d1str,'%Y-%m-%d %H%M%S')


        # crontab = '0 * * * *'
        # # d2 = d1.replace(day=2,hour=23)
        # cron = croniter(crontab, utcnow())
        # cron.tzinfo = pendulum.timezone("Asia/Shanghai")
        # # print cron.get_prev(datetime)
        # # print 1007/7+1
        # print make_naive(utcnow())
        # print 1+-1
        #
        #
        # re_object = re.match(r'L\s*((\+|-)+\s*[0-9]+)', 'L - 13',re.I)
        #
        # if re_object:
        #     print re_object.group(1)
        #     print re_object.group(2)
        #
        # print int('+ 2')

    def test_get_all_date_in_one_cycle(self):
        # exe_date = datetime(year=2022, month=06, day=21)
        # print TaskCronUtil.get_all_date_in_one_cycle('test_new_scheduler',exe_date)
        import random

        print timezone.utcnow().replace(tzinfo=beijing)
        time.sleep(random.random())
        print timezone.utcnow().replace(tzinfo=beijing)
        time.sleep(random.random())
        print timezone.utcnow().replace(tzinfo=beijing)


    def test_cron_times(self):
        start_date = pendulum.parse("2022-07-10 00:00:00")
        end_date = pendulum.parse("2022-07-10 23:59:59")
        times = croniter_range(start_date,end_date,"00 15 * * *")
        for exec_time in times:
            print my_make_naive(exec_time)
            print my_make_naive(utcnow())
            print my_make_naive(exec_time)>my_make_naive(utcnow())


    def test_aaa(self):
        # execution_date = datetime(year=2022, month=06, day=30,hour=8,minute=0,second=0)
        execution_date = '2022-07-01 00:00:00'
        execution_date = pendulum.parse(execution_date)
        execution_date.replace(tzinfo=pendulum.timezone("Asia/Shanghai"))

        eeexecution_date = datetime(year=2022,month=7,day=1,hour=0)
        eeexecution_date = pendulum.instance(eeexecution_date)
        # print execution_date.int_timestamp
        print eeexecution_date.int_timestamp
        print execution_date.int_timestamp
        eeexecution_date = eeexecution_date.replace(tzinfo=pendulum.timezone("Asia/Shanghai"))
        # eeexecution_date.tzinfo=pendulum.timezone("Asia/Shanghai")
        print eeexecution_date.int_timestamp
        # from airflow import macros
        # a='hahahah {{ (execution_date ).int_timestamp }} {{ (eeexecution_date ).int_timestamp }}'
        # context = {}
        # context["execution_date"] = execution_date
        # context["eeexecution_date"] = eeexecution_date
        # context["macros"] = macros
        #
        # jinja_env = jinja2.Environment(cache_size=0)
        # b= jinja_env.from_string(a).render(**context)
        # print b

    def test_bbbb(self):

        a=True
        if not a:
            print 111

    def test_ger_prev_execution_date_by_task(self):
        task_name = 'dws_user_group_detail_di_batch_si'
        cur_date = timezone.utcnow().replace(tzinfo=timezone.beijing)
        TaskCronUtil.ger_prev_execution_date_by_task(task_name,cur_date)
        # start_date= pendulum.parse('2022-09-08 00:00:00')
        # end_date = pendulum.parse('2022-09-08 16:59:00')

        execution_date = pendulum.parse('2022-09-08 15:00:00.176148000')
        # tis = TaskInstance.find_with_date_range(dag_id='scheduler_improve_external_down_copy_copy',task_id='scheduler_improve_external_down_copy_copy',
        #                                   start_date=start_date,end_date=end_date)
        tis = TaskInstance.find_by_execution_dates(dag_id='scheduler_improve_external_down_copy_copy',task_id='scheduler_improve_external_down_copy_copy',
                                          execution_dates=[execution_date])
        for ti in tis:
            print ti.execution_date.strftime('%Y-%m-%d %H:%M:%S.%f')
            print int(time.mktime(ti.execution_date.timetuple())*1000 + ti.execution_date.microsecond)
        # execution_date = '2022-07-01 00:00:00.123'
        # execution_date = pendulum.parse(execution_date)
        # execution_date.replace(tzinfo=pendulum.timezone("Asia/Shanghai"))
        # print execution_date.strftime('%Y-%m-%d %H:%M:%S.%f')
