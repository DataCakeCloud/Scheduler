# !/usr/bin/env python
# -*- coding: utf-8 -*-
# author: hongyg
# desc: Notification by message[dingding(notify/robot), phone, email]

import requests
requests.packages.urllib3.disable_warnings()
from airflow.shareit.constant.notify import *

from airflow.utils.log.logging_mixin import LoggingMixin


class Notify:

    def __init__(self):
        pass

    @staticmethod
    def __get_log():
        """
        get Context logger printer.
        :return:
        """
        return LoggingMixin().log

    @staticmethod
    def __md_template(title, msg):
        """
        markdown模板
        :param title:
        :param msg:
        :return:
        """
        return {
            "msgtype": "markdown",
            "markdown": {
                "title": title,
                "text": msg
            }
        }

    @staticmethod
    def __text_template(msg):
        """
        文本格式模板
        :param msg:
        :return:
        """
        return {
            "msgtype": "text",
            "text": {
                "content": msg
            }
        }

    @staticmethod
    def __get_header():
        return {'Content-Type': CONTENT_TYPE}

    def __post_url(self, url, headers, data):
        log = self.__get_log()
        try:
            log.info("url: %s, headers: %s, data: %s" % (url, str(headers), str(data)))
            session = requests.session()
            session.keep_alive = False
            resp = session.post(url, headers=headers, json=data, verify=False, timeout=1200)
            print(resp.text)
            session.close()
            log.info("notify success. url: %s" % url)
        except Exception as e:
            log.warn("require url: %s error, cause: %s." % (url, str(e)))

    def notify_md_by_robot(self, access_token, title, msg, is_at_all=False, at_mobiles=None):
        """
        机器人群通知
        :param access_token: webhook access token
        :param title:
        :param msg:
        :param is_at_all: 是否@所有人 [True|False]
        :param at_mobiles: 具体@的手机号, list
        :return:
        """
        if at_mobiles is None:
            at_mobiles = []
        data = self.__md_template(title, msg)
        data["at"] = self.__at_format(is_at_all, at_mobiles)
        self.__post_url(ROBOT_API + "?access_token=" + access_token, self.__get_header(), data)

    @staticmethod
    def __at_format(is_at_all, at_mobiles):
        ats = {"isAtAll": is_at_all}
        if at_mobiles is not None and len(at_mobiles)>0:
            ats["atMobiles"] = at_mobiles
        return ats

    def notify_text_by_robot(self, access_token, msg, is_at_all=False, at_mobiles=None):
        """
        机器人群通知
        :param access_token: webhook access token
        :param title:
        :param msg:
        :param is_at_all: 是否@所有人 [True|False]
        :param at_mobiles: 具体@的手机号, list
        :return:
        """
        data = self.__text_template(msg)
        data["at"] = self.__at_format(is_at_all, at_mobiles)
        self.__post_url(ROBOT_API + "?access_token=" + access_token, self.__get_header(), data)

    @staticmethod
    def __get_cert_header(appname, appkey):
        return {
            'Grpc-Metadata-appname': appname,
            'Grpc-Metadata-appkey': appkey,
            'Content-Type': CONTENT_TYPE,
            'Accept': CONTENT_TYPE
        }

    def notify_md_work_ding(self, ids, title, msg, appname, appkey):
        """
        钉钉个人工作通知,markdown
        :param ids: ['shareitid', 'xxx']
        :param title:
        :param msg:
        :param appname: 找曾宝德-Baode Zeng(SHAREIT-研发平台/R&D Platform-云平台/Cloud Platform)申请
        :param appkey: 找曾宝德-Baode Zeng(SHAREIT-研发平台/R&D Platform-云平台/Cloud Platform)申请
        :return:
        """
        if ids is None or len(ids) == 0:
            return
        data = self.__md_template(title, msg)
        data["receiver"] = ids
        self.__post_url(NOTIFY_SEND_API, self.__get_cert_header(appname, appkey), data)

    def notify_text_work_ding(self, ids, msg, appname, appkey):
        """
        钉钉个人工作通知,文本
        :param ids: ['shareitid', 'xxx']
        :param msg:
        :param appname: 找曾宝德-Baode Zeng(SHAREIT-研发平台/R&D Platform-云平台/Cloud Platform)申请
        :param appkey: 找曾宝德-Baode Zeng(SHAREIT-研发平台/R&D Platform-云平台/Cloud Platform)申请
        :return:
        """
        if ids is None or len(ids) == 0:
            return
        data = self.__text_template(msg)
        data["receiver"] = ids
        self.__post_url(NOTIFY_SEND_API, self.__get_cert_header(appname, appkey), data)
