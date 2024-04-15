# -*- coding: utf-8 -*-
import json

import requests

from airflow.hooks.base_hook import BaseHook


class WechatHook(BaseHook):
    def __init__(self, wechat_conn_id='wechat_default',robot_key=None):
        self._conn = self.get_connection(wechat_conn_id)
        self.host = self._conn.host
        self.robot_key = robot_key

    def send_text(self, text, mentioned_list=None, mentioned_mobile_list=None):
        """
        Send text message to wechat
        """
        url = self.host + '/cgi-bin/webhook/send?key=' + self.robot_key
        data = {
            "msgtype": "text",
            "text": {
                "content": text
            },
        }
        if mentioned_list is not None:
            data['text']['mentioned_list'] = mentioned_list if type(mentioned_list) == list else mentioned_list.split(',')
        if mentioned_mobile_list is not None:
            data['text']['mentioned_mobile_list'] = mentioned_mobile_list if type(mentioned_mobile_list) == list else mentioned_mobile_list.split(',')
        response = requests.post(url=url, data=json.dumps(data))
        return response.json()

    def send_markdown(self, markdown, mentioned_list=None):
        """
        Send markdown message to wechat
        """
        url = self.host + '/cgi-bin/webhook/send?key=' + self.robot_key
        header = {"Content-Type": "application/json"}

        # 微信的markown消息中 ，不能使用mentioned_list参数
        if mentioned_list is not None:
            for user in mentioned_list:
                markdown += '<@{}>'.format(user)
        data = {
            "msgtype": "markdown",
            "markdown": {
                "content": markdown
            },
        }

        response = requests.post(url=url, headers=header, data=json.dumps(data))
        return response.json()

    def send_image(self):
        return

    def send_news(self):
        return