from sqlalchemy import Column, String, Integer, DateTime, func, update, Boolean
from sqlalchemy.dialects.mysql import MEDIUMTEXT
from airflow.models.base import Base, ID_LEN, OPTION_LEN, MESSAGE_LEN
from airflow.utils.db import provide_session


class GrayTest(Base):
    __tablename__ = "gray_test"
    task_name = Column(String(ID_LEN),primary_key=True)
    open_test = Column(Boolean, default=False)
    task_type = Column(String())

    @staticmethod
    @provide_session
    def open_gray_test(task_name, session=None):
        gt = session.query(GrayTest).filter(GrayTest.task_name == task_name).first()
        if gt:
            gt.open_test = True
            session.commit()

    @staticmethod
    @provide_session
    def close_gray_test(task_name, session=None):
        gt = session.query(GrayTest).filter(GrayTest.task_name == task_name).first()
        if gt:
            gt.open_test = False
        else:
            return

    @staticmethod
    @provide_session
    def is_open(task_name,session=None):
        gt = session.query(GrayTest).filter(GrayTest.task_name == task_name).first()
        if gt and gt.open_test:
            return True
        else :
            return False

    @staticmethod
    @provide_session
    def update_task_name(old_task_name, new_task_name, transaction=None,session=None):
        gt = session.query(GrayTest).filter(GrayTest.task_name == old_task_name).first()
        if gt:
            gt.task_name = new_task_name
            session.merge(gt)
            if not transaction:
                session.commit()
