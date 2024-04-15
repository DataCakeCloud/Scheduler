# -*- coding: utf-8 -*-
from airflow.utils.timezone import make_naive, utcnow
from sqlalchemy import Column, String, Integer, DateTime, Boolean
from airflow.models.base import Base, ID_LEN, OPTION_LEN, MESSAGE_LEN
from airflow.utils.db import provide_session
from airflow.shareit.utils.state import State


class FastExecuteTask(Base):
    __tablename__ = "fast_execute_task"

    task_name = Column(String(ID_LEN), primary_key=True)
    real_task_name = Column(String(ID_LEN))
    is_executed = Column(Boolean, default=False)
    external_conf = Column(String)

    @staticmethod
    @provide_session
    def find_all_waiting_task(session=None):
        result = session.query(FastExecuteTask).filter(FastExecuteTask.is_executed == False).all()
        return result

    @staticmethod
    @provide_session
    def add_and_update(task_name, real_task_name, external_conf,session=None):
        task = session.query(FastExecuteTask).filter(FastExecuteTask.task_name == task_name).first()
        if task:
            task.is_executed = False
            task.external_conf = external_conf
        else:
            task = FastExecuteTask()
            task.is_executed = False
            task.task_name = task_name
            task.real_task_name = real_task_name
            task.external_conf = external_conf

        session.merge(task)
        session.commit()

    @staticmethod
    @provide_session
    def close_task(task_name, transaction=False, session=None):
        task = session.query(FastExecuteTask).filter(FastExecuteTask.task_name == task_name).first()
        if task:
            task.is_executed = True
            session.merge(task)
            if not transaction:
                session.commit()
