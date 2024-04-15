# -*- coding: utf-8 -*-
from sqlalchemy import Column, String, Integer, Boolean, DateTime,func

from airflow.models.base import Base, ID_LEN, MESSAGE_LEN, OPTION_LEN
from airflow.shareit.utils.constant import Constant
from airflow.utils import timezone
from airflow.utils.db import provide_session
from datetime import datetime,timedelta
from sqlalchemy.dialects.mysql import MEDIUMTEXT

class EventTriggerMapping(Base):
    __tablename__ = "event_trigger_mapping"

    task_name = Column(String(ID_LEN), primary_key=True)
    uuid = Column(String(ID_LEN), primary_key=True)
    callback_info = Column(MEDIUMTEXT)
    template_params = Column(MEDIUMTEXT)
    execution_date = Column(DateTime)
    is_execute = Column(Boolean, default=False)
    create_date = Column(DateTime,server_default=func.now())
    update_date = Column(DateTime,server_default=func.now(),onupdate=func.now())

    @staticmethod
    @provide_session
    def add(task_name, uuid, callback_info=None, template_params=None, execution_date=None,session=None):
        etm = EventTriggerMapping()
        etm.task_name = task_name
        etm.uuid = uuid
        if callback_info:
            etm.callback_info = callback_info
        if template_params:
            etm.template_params = template_params
        if execution_date:
            etm.execution_date = execution_date
        session.add(etm)
        session.commit()

    @staticmethod
    @provide_session
    def update(uuid, callback_info=None, template_params=None, execution_date=None,is_execute=None,session=None):
        etm = session.query(EventTriggerMapping).filter(EventTriggerMapping.uuid == uuid).first()
        if etm:
            if callback_info:
                etm.callback_info = callback_info
            if template_params:
                etm.template_params = template_params
            if execution_date:
                etm.execution_date = execution_date
            if is_execute:
                etm.is_execute = is_execute
            session.merge(etm)
            session.commit()


    @staticmethod
    @provide_session
    def get_all_waiting(session=None):
        etms = session.query(EventTriggerMapping).filter(EventTriggerMapping.is_execute == False).all()
        return etms

    @staticmethod
    @provide_session
    def get_template_params(uuid,session=None):
        etm = session.query(EventTriggerMapping).filter(EventTriggerMapping.uuid == uuid).first()
        if etm:
            return etm.template_params
        else:
            return None


    @staticmethod
    @provide_session
    def get_etm(uuid,session=None):
        etm = session.query(EventTriggerMapping).filter(EventTriggerMapping.uuid == uuid).first()
        if etm:
            return etm
        else:
            return None
