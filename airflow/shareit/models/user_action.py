# -*- coding: utf-8 -*-
"""
Author: Tao Zhang
Date: 2022/2/16
"""
import json

from sqlalchemy import Column, String, Integer, DateTime
from sqlalchemy.dialects.mysql import MEDIUMTEXT

from airflow.models.base import Base, ID_LEN, OPTION_LEN
from airflow.utils.db import provide_session
from airflow.utils.timezone import utcnow


class UserAction(Base):
    __tablename__ = "user_action"

    id = Column(Integer, primary_key=True)
    user_id = Column(String(ID_LEN))
    action_type = Column(String(OPTION_LEN))
    execution_time = Column(DateTime)
    details = Column(MEDIUMTEXT)

    @staticmethod
    @provide_session
    def add_user_action(user_id=None, action_type=None, execution_time=None, details=None, commit=True,
                        session=None):
        user_action = UserAction()
        user_action.user_id = user_id
        user_action.action_type = action_type
        user_action.execution_time = execution_time
        if execution_time is None:
            user_action.execution_time = utcnow()
        if details is not None:
            if isinstance(details, dict):
                details = json.dumps(details)
            user_action.details = details
        session.merge(user_action)
        if commit:
            session.commit()
        # user_action中id为空
        newUA = UserAction.find_a_user_action(user_id=user_action.user_id,
                                              action_type=user_action.action_type,
                                              details=user_action.details)
        return newUA

    def get_details(self):
        if isinstance(self.details, str):
            try:
                return json.loads(self.details)
            except Exception:
                pass
        return self.details

    @staticmethod
    @provide_session
    def find_a_user_action(user_action_id=None, user_id=None, action_type=None, execution_time=None, details=None,
                           session=None):
        session.commit()
        qry = session.query(UserAction)
        if user_action_id:
            qry = qry.filter(UserAction.id == user_action_id)
        if user_id:
            qry = qry.filter(UserAction.user_id == user_id)
        if action_type:
            qry = qry.filter(UserAction.action_type == action_type)
        if execution_time:
            qry = qry.filter(UserAction.execution_time == execution_time)
        if details:
            qry = qry.filter(UserAction.details == details)
        return qry.first()
