# -*- coding: utf-8 -*-
"""
Author: Tao Zhang
Date: 2022/2/23
If you are doing your best,you will not have to worry about failure.
"""
import unittest

from airflow.shareit.models.user_action import UserAction


class UserActionTest(unittest.TestCase):

    def test_add_user_action(self):
        user_action = UserAction.add_user_action(user_id="zhangtao",
                                                 action_type="test",
                                                 details={"owners": "dfasd", "instance_list": []}
                                                 )
        # user_action = UserAction.find_a_user_action(user_id=user_action.user_id,
        #                                             action_type=user_action.action_type,
        #                                             execution_time=user_action.execution_time)
        print (user_action.id)
