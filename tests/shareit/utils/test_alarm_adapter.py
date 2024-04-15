# -*- coding: utf-8 -*-
"""
Author: Tao Zhang
Date: 2022/6/10
If you are doing your best,you will not have to worry about failure.
"""
import unittest
import pendulum

from datetime import datetime, timedelta

from airflow.models import TaskInstance
from airflow.shareit.models.task_desc import TaskDesc
from airflow.shareit.utils.alarm_adapter import build_backfill_process_reminder_content, build_backfill_handle_content, \
    build_backfill_notify_content, send_ding_notify, ding_recovery_state, \
    send_email_by_notifyAPI
from airflow.shareit.utils.date_util import DateUtil
from airflow.shareit.utils.normal_util import NormalUtil
from airflow.utils.timezone import utcnow


class AlarmTest(unittest.TestCase):

    def test_build_backfill_process_reminder_content(self):
        """
        [{"dag_id":"","execution_date":"","state":""}]
        """

        instance_info_list = [{"dag_id":"ADS_dwb_ads_offerwall_click_log_hour","execution_date":"2022-06-22 23:33:33","state":"waiting"},
                              {"dag_id": "spark_nginx_hour_job", "execution_date": "2022-06-22 01:33:33", "state": "termination"},
                              {"dag_id": "spark_nginx_hour_job", "execution_date": "2022-06-22 22:33:33", "state": "waiting"},
                              {"dag_id": "spark_nginx_hour_job", "execution_date": "2022-06-22 18:33:33", "state": "waiting_queue"},
                              {"dag_id":"ADS_dwb_ads_offerwall_click_log_hour","execution_date":"2022-06-22 12:33:33","state":"termination"},
                              {"dag_id":"spark_nginx_hour_job","execution_date":"2022-06-22 16:33:33","state":"running"},
                              {"dag_id":"ADS_dwb_ads_offerwall_click_log_hour","execution_date":"2022-06-22 14:33:33","state":"success"},
                              {"dag_id":"BS_tmp_space_file","execution_date":"2022-06-22 18:33:33","state":"waiting"},
                              {"dag_id":"BS_tmp_space_file","execution_date":"2022-06-22 10:33:33","state":"termination"},
                              {"dag_id":"BS_tmp_space_file","execution_date":"2022-06-22 11:33:33","state":"failed"},
                              {"dag_id":"BS_tmp_space_file","execution_date":"2022-06-22 13:33:33","state":"up_for_retry"},

                              ]
        con = build_backfill_process_reminder_content(instance_info_list=instance_info_list)
        print(con)

    def test_build_backfill_handle_content(self):
        con = build_backfill_handle_content(operator="zhangtao",core_task="zhangtao_test_task1",
                                      start_date="2022-06-22 18:33:33",
                                      end_date="2022-06-23 18:33:33")
        print (con)

    def test_build_backfill_notify_content(self):
       content = build_backfill_notify_content(operator="wuhaorui",task_names="ceshi",upstream_tasks="123",
                                      start_date="2022-06-22 18:33:33",
                                      end_date="2022-06-23 18:33:33",connectors=None)

       send_ding_notify(title="DataStudio 提示信息", content=content, receiver=['wuhaorui'])


    def test_recovery(self):
        ding_recovery_state(task_name='1',state='1',app_state='1',end_time='11',ti_state='11',batch_id='1')

    def test_email(self):
        content = '您的数据分析任务已经运行成功。' \
                  '任务名称：{}，' \
                  '任务实例日期：{}。  <p><a href="{}" target="_blank">点击此处下载</a></p>'.format('1','1','1')
        # content = '<p><a href="https://www.example.com" target="_blank">Visit our Website</a></p>'
        send_email_by_notifyAPI(title='DataCake数据分析任务',content=content,receiver=['wuhaorui@ushareit.com'])