# -*- coding: utf-8 -*-
import unittest

from airflow.shareit.hooks.message.wechat_hook import WechatHook


class wechatHookTest(unittest.TestCase):

    def test_send_text(self):
        base_content = """
[DataCake 任务状态通知]123
任务: 13  
执行入参时间: 123   
[任务实例列表页链接](https://test.datacake.cloud/task/list)"""
        WechatHook(robot_key='90bd275d-743d-4e89-8d47-f35f79067db4').send_markdown(base_content,mentioned_list=['wuhaorui'])