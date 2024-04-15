# -*- coding: utf-8 -*-

import json
from datetime import datetime, timedelta

from sqlalchemy import Column, String, Integer, DateTime, func, update
from sqlalchemy.dialects.mysql import MEDIUMTEXT

from airflow.models.base import Base, ID_LEN, OPTION_LEN, MESSAGE_LEN
from airflow.utils import timezone
from airflow.utils.db import provide_session
from airflow.shareit.utils.granularity import Granularity


class Alert(Base):
    __tablename__ = "alert_job"

    id = Column(Integer, primary_key=True)
    task_name = Column(String(ID_LEN))
    execution_date = Column(DateTime)
    state = Column(String(OPTION_LEN)) # checking，finished
    create_date = Column(DateTime, server_default=func.now())
    update_date = Column(DateTime, server_default=func.now(), onupdate=func.now())

    @staticmethod
    @provide_session
    def list_all_open_alert_job(session=None):
        result = session.query(Alert).filter(Alert.state =='checking').all()
        return result

    @staticmethod
    @provide_session
    def add_alert_job(task_name,execution_date,transaction=False,session=None):
        alert = session.query(Alert).filter(Alert.task_name == task_name).filter(Alert.execution_date == execution_date).all()
        if alert is not None and len(alert) > 0:
            alert_job = alert[0]
            alert_job.state = 'checking'
        else:
            alert_job = Alert()
            alert_job.task_name = task_name
            alert_job.execution_date = execution_date
            alert_job.state = 'checking'
        session.merge(alert_job)
        if not transaction:
            session.commit()

    @staticmethod
    @provide_session
    def reopen_alert_job(task_name,execution_date,session=None):
        alert = session.query(Alert).filter(Alert.task_name == task_name).filter(Alert.execution_date == execution_date).all()
        if alert is not None and len(alert) > 0:
            alert_job = alert[0]
            alert_job.state = 'checking'
            session.merge(alert_job)
            session.commit()
        else:
            return


    @staticmethod
    @provide_session
    def sync_state(task_name,execution_date,state,session=None):
        alert = session.query(Alert).filter(Alert.task_name == task_name).filter(Alert.execution_date == execution_date).all()
        if alert is not None and len(alert) > 0:
            alert_job = alert[0]
            alert_job.state = state
            session.merge(alert_job)
            session.commit()