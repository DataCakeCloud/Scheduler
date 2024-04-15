# -*- coding: utf-8 -*-
"""
Author: zhangtao
CreateDate: 2020/9/10
"""

from airflow.operators.python_operator import PythonOperator
from airflow.utils.decorators import apply_defaults
from airflow.shareit.utils.statistics import StatisticsUtil


def default_python_callable(**kwargs):
    print("Argument python_callable error!")


class StatisticsTasksOperator(PythonOperator):

    @apply_defaults
    def __init__(self,
                 python_callable=default_python_callable,
                 dag_pattern=None,
                 task_pattern=None,
                 skipList=None,
                 is_alarm=True,
                 alarm_func=None,
                 alarm_isDetail=True,
                 alarm_group=None,
                 alarm_severity=None,
                 alarm_subject=None,
                 alarm_appendMessage=None,
                 *args, **kwargs):
        """
        statistics tasks status operator
        :param  dag_pattern: dag_id pattern
        :type str
        :param task_pattern: task_id pattern ,it can be omit
        :type str
        :param skipList: dag_id list which will be skipped
        :type str
        :param is_alarm: whether to send an alarm
        :type bool
        :param alarm_func: Judgment condition function
        :type function
        :param alarm_isDetail: alarm whether to send failed and unready task_id
        :type bool
        :param alarm_group: select the groups to send the alarm
        :type str
        :param alarm_severity: specify the alarm level when a task fails
                critical: alarm to email, dingding group ,dingding work notification and on call
                emergency: alarm to email, dingding work notification and dingding group
                warning: alarm to email only
                cite https://doc.ushareit.me/web/#/4?page_id=1483
        :type str
        :param alarm_subject: alarm subject
        :type str
        :param alarm_appendMessage: append message in alarm
        :type str
        """

        super(StatisticsTasksOperator, self).__init__(python_callable=python_callable, *args, **kwargs)
        self.dag_pattern = dag_pattern
        self.task_pattern = task_pattern
        self.skipList = skipList
        self.is_alarm = is_alarm
        self.alarm_func = alarm_func
        self.alarm_isDetail = alarm_isDetail
        self.alarm_group = alarm_group
        self.alarm_severity = alarm_severity
        self.alarm_subject = alarm_subject
        self.alarm_appendMessage = alarm_appendMessage

        self.python_callable = self.get_statistics

    def get_statistics(self, **kwargs):
        try:
            su = StatisticsUtil()
            res = su.get_statistics(dag_id=self.dag_pattern,
                                    task_id=self.task_pattern,
                                    skipList=self.skipList)
            print("########")
            for key in res:
                if "List" in key:
                    print(str(key) + ": " + ";".join(res[key]))
                else:
                    print (str(key) + ": " + str(res[key]))
            print("########")
            if self.is_alarm:
                su.send_SCMP_alarm(group=self.alarm_group,
                                   func=self.alarm_func,
                                   isDetail=self.alarm_isDetail,
                                   severity=self.alarm_severity,
                                   subject=self.alarm_subject,
                                   appendMessage=self.alarm_appendMessage)
        except Exception as e:
            print("Encountered some problems in the statistical process!")
            raise e
