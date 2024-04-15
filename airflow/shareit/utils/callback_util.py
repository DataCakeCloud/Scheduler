# -*- coding: utf-8 -*-
import json

from airflow import LoggingMixin
import requests
import time

from airflow.shareit.models.alert import Alert
from airflow.shareit.models.callback import Callback

log=LoggingMixin().logger
class CallbackUtil(LoggingMixin):
    @staticmethod
    def callback_post(url, data):
        tmp_err = None
        for _ in range(3):
            try:
                from airflow.utils.state import State
                result = requests.post(url=url, json=data)
                log.info("[DataStudio] callback resp:{}".format(result.content))
                rep_json = json.loads(result.content)
                return rep_json
            except Exception as e:
                time.sleep(1)
                tmp_err = e
        if tmp_err is not None:
            raise tmp_err

    '''
        为了保证回调一定成功，如果失败的话加入callbackjob中
    '''
    @staticmethod
    def callback_post_guarantee(url,data,task_name,execution_date,instance_id):
        try:
            resp = CallbackUtil.callback_post(url, data)
            # 为push平台定制的内容...
            if "resultCode" in resp.keys() and resp['resultCode'] == 200:
                # 成功后关闭回调
                Callback.close_callback(task_name,execution_date)
                return
            Callback.add_callback(url, json.dumps(data), task_name, execution_date, instance_id)
        except Exception as e:
            Callback.add_callback(url, json.dumps(data), task_name, execution_date, instance_id)
            raise e
