# -*- coding: utf-8 -*-
from sqlalchemy import Column, String, Integer, DateTime, Boolean, func
from sqlalchemy.dialects.mysql import MEDIUMTEXT, TEXT

from airflow.models.base import Base, ID_LEN, MESSAGE_LEN, OPTION_LEN
from airflow.utils.db import provide_session


class BackfillJob(Base):
    __tablename__ = "backfill_job"

    id = Column(Integer, primary_key=True)
    description = Column(String(MESSAGE_LEN))
    core_task_name = Column(String(ID_LEN))
    sub_task_names = Column(TEXT)
    state = Column(String(OPTION_LEN))
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    all_ti = Column(MEDIUMTEXT)
    operator = Column(String(ID_LEN))
    ignore_check = Column(Boolean)
    is_notify = Column(Boolean)
    create_date = Column(DateTime, server_default=func.now())
    update_date = Column(DateTime, server_default=func.now(), onupdate=func.now())
    label = Column(String)

    @property
    def sub_task_name_list(self):
        if self.sub_task_names:
            return self.sub_task_names.split(',')
        else:
            return []

    @staticmethod
    @provide_session
    def find(id, session=None):
        return session.query(BackfillJob).filter(BackfillJob.id == id).one_or_none()

    @staticmethod
    @provide_session
    def find_waiting_jobs(session=None):
        return session.query(BackfillJob).filter(BackfillJob.state == 'waiting').order_by(BackfillJob.create_date).all()

    @staticmethod
    @provide_session
    def add(description=None, core_task_name=None, sub_task_names=None, start_date=None, end_date=None,
            operator=None, ignore_check=False, is_notify=False,label=None,session=None):
        job = BackfillJob()
        job.description = description
        job.core_task_name = core_task_name
        if sub_task_names:
            job.sub_task_names = sub_task_names
        job.start_date = start_date
        job.end_date = end_date
        job.operator = operator
        job.ignore_check = ignore_check
        job.is_notify = is_notify
        job.state = 'waiting'
        job.label = label
        session.merge(job)
        session.commit()

    @staticmethod
    @provide_session
    def start(id, all_ti, transaction=False, session=None):
        job = session.query(BackfillJob).filter(BackfillJob.id == id).one_or_none()
        job.state = 'success'
        job.all_ti = all_ti
        session.merge(job)
        if not transaction:
            session.commit()


    @staticmethod
    @provide_session
    def faild(id, transaction=False, session=None):
        job = session.query(BackfillJob).filter(BackfillJob.id == id).one_or_none()
        job.state = 'failed'
        session.merge(job)
        if not transaction:
            session.commit()
