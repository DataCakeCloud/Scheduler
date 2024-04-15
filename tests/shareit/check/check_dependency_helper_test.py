# -*- coding: utf-8 -*-
import unittest

import pendulum

from airflow.shareit.scheduler.scheduler_helper import SchedulerManager
from airflow.utils import timezone

from airflow.shareit.check.check_dependency_helper import CheckDependecyHelper
from airflow.shareit.jobs.cron_job import DsCronJob
from airflow.shareit.scheduler.cron_scheduler_helper import CronScheduelrManager
from airflow.utils.timezone import datetime


class TestCheckDependency(unittest.TestCase):
    def test111(self):
        # m_date = datetime(year=2022,month=6,day=24,hour=1,minute=0)
        # try :
        #     m_date.replace(day=int(31))
        # except ValueError :
        #     print "日单位超过了当月的范围,月:{},日:{}".format(m_date.month,int(31))
        for i in range(1,25):
            print '"{}",'.format(i)

    def test_check_data_dependency(self):
        dag_id = 'check_trigger_test_4'
        execution_date = datetime(year=2022,month=6,day=1,hour=0,minute=0)
        # print CheckDependecyHelper.check_data_dependency(dag_id, execution_date,
        #                                            is_backfill=False)
        # e= CheckDependecyHelper.list_event_dependency_result_tuple(dag_id,execution_date,is_backfill=False)
        # for i in e[0]:
        #     print '检测到{}'.format(i)
        # for i in e[1]:
        #     print '没有的{}'.format(i)
        # print 1111111111111111111111

        a= CheckDependecyHelper.list_data_dependency_result_tuple(dag_id,execution_date,is_backfill=False)
        for i in a[0]:
            print '检测到{}'.format(i)
        for i in a[1]:
            print '没有的{}'.format(i)
        print 1111111111111111111111
        b= CheckDependecyHelper.old_list_data_dependency_result_tuple(dag_id,execution_date,is_backfill=False)
        for i in b[0]:
            print '检测到{}'.format(i)
        for i in b[1]:
            print '没有的{}'.format(i)
    def test_check(self):
        dag_id= '1_23179'
        execution_date = pendulum.parse('2023/04/06 00:00:00.000').replace(tzinfo=timezone.beijing)
        result = CheckDependecyHelper.check_dependency(dag_id,execution_date)
        # result = CheckDependecyHelper.list_dependency_result_tuple(dag_id, execution_date)
        print result

    def test_list_data_dependency(self):
        dag_id = '36_12528'
        execution_date = datetime(year=2023, month=1, day=13, hour=0, minute=0)
        result = CheckDependecyHelper.list_data_dependency(dag_id,execution_date)
        print result
