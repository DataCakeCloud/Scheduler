import unittest
from airflow.shareit.utils.alarm_adapter import send_telephone_by_notifyAPI
from airflow.shareit.utils.task_manager import get_dag


class PhoneTest(unittest.TestCase):
    def test_send_telephone_by_notifyAPI(self):
        # dag = get_dag("schedule_improve")
        # task = dag.task_dict['schedule_improve']
        # print task.dingAlert
        # print task.phoneAlert
        send_telephone_by_notifyAPI(["wuhaorui","luhongyu","zhangrg"])