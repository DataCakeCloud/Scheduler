# -*- coding: utf-8 -*-

import json
from datetime import datetime, timedelta

from sqlalchemy import Column, String, Integer, DateTime, func, update
from sqlalchemy.dialects.mysql import MEDIUMTEXT

from airflow.models.base import Base, ID_LEN, OPTION_LEN, MESSAGE_LEN
from airflow.utils import timezone
from airflow.utils.db import provide_session
from airflow.shareit.utils.granularity import Granularity

'''
    为了保证回调一定成功，将一些失败的回调存储下来，定期重新调一次
    为push平台定制化的功能
'''


class Callback(Base):
    __tablename__ = "callback"

    id = Column(Integer, primary_key=True)
    callback_url = Column(String(ID_LEN))
    callback_body = Column(MEDIUMTEXT)
    relation_task_name = Column(String(ID_LEN))
    check_nums = Column(Integer, default=0)
    state = Column(String(OPTION_LEN), default='running')  # running，failed,success
    execution_date = Column(DateTime)  # genie_id
    taskinstance_id = Column(String(ID_LEN))
    create_date = Column(DateTime, server_default=func.now())
    update_date = Column(DateTime, server_default=func.now(), onupdate=func.now())

    @staticmethod
    @provide_session
    def list_running_callback(relation_task_name=None, session=None):
        qry = session.query(Callback).filter(Callback.state == 'running')
        if relation_task_name:
            qry = qry.filter(Callback.relation_task_name == relation_task_name)

        return qry.all()

    @staticmethod
    @provide_session
    def add_callback(callback_url, callback_body, relation_task_name, execution_date, instance_id,session=None):
        callback = session.query(Callback).filter(Callback.relation_task_name == relation_task_name).filter(
            Callback.execution_date == execution_date).one_or_none()
        if callback:
            callback.state = 'running'
            callback.check_nums = 0
            callback.callback_url = callback_url
            callback.callback_body = callback_body
            callback.taskinstance_id = instance_id
        else:
            callback = Callback()
            callback.callback_url = callback_url
            callback.callback_body = callback_body
            callback.relation_task_name = relation_task_name
            callback.execution_date = execution_date
            callback.taskinstance_id = instance_id
        session.merge(callback)
        session.commit()

    @staticmethod
    @provide_session
    def update_callback(id, state=None, nums=None, session=None):
        callback = session.query(Callback).filter(Callback.id == id).one_or_none()
        if callback:
            if state:
                callback.state = state
            callback.check_nums = nums
            session.merge(callback)
            session.commit()

    @staticmethod
    @provide_session
    def close_callback(relation_task_name,execution_date,session=None):
        callback = session.query(Callback).filter(Callback.relation_task_name == relation_task_name).filter(
            Callback.execution_date == execution_date).one_or_none()
        if callback:
            callback.state = 'success'
            session.merge(callback)
            session.commit()