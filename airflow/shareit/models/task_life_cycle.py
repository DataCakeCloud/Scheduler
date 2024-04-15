# -*- coding: utf-8 -*-
import json
from datetime import datetime, timedelta

from sqlalchemy import Column, String, Integer, DateTime, func, update, Boolean
from sqlalchemy.dialects.mysql import MEDIUMTEXT

from airflow.models.base import Base, ID_LEN, OPTION_LEN, MESSAGE_LEN
from airflow.utils import timezone
from airflow.utils.db import provide_session
from airflow.shareit.utils.granularity import Granularity


class TaskLifeCycle(Base):
    __tablename__ = "task_life_cycle"
    id = Column(Integer, primary_key=True)
    task_name = Column(String(ID_LEN))
    execution_date = Column(DateTime)
    first_pipeline_time = Column(Integer,default=0)
    ready_time = Column(Integer,default=0)
    backfill_time = Column(Integer,default=0)
    clear_time = Column(Integer,default=0)
    start_time = Column(Integer,default=0)
    is_export = Column(Boolean,default=False)
    create_date = Column(DateTime, server_default=func.now())
    update_date = Column(DateTime, server_default=func.now(), onupdate=func.now())

    @staticmethod
    @provide_session
    def find_cycle(task_name, execution_date, session=None):
        cycles = session.query(TaskLifeCycle).filter(TaskLifeCycle.task_name == task_name,
                                                     TaskLifeCycle.execution_date == execution_date).all()
        return cycles

    @staticmethod
    @provide_session
    def update_cycle_first(task_name, execution_date, start_time=None, ready_time=None, session=None):
        qry = session.query(TaskLifeCycle).filter(TaskLifeCycle.task_name == task_name,
                                                  TaskLifeCycle.execution_date == execution_date)
        cycle = qry.order_by(TaskLifeCycle.create_date.desc()).first()
        if start_time:
            if cycle.start_time:
                # 第一条,start_time已经存在 直接返回
                return
            cycle.start_time = start_time
        if ready_time:
            cycle.ready_time = ready_time
        session.merge(cycle)
        session.commit()

    @staticmethod
    @provide_session
    def add_cycle(task_name, execution_date, backfill_time=None, clear_time=None, first_pipeline_time=None,
                  ready_time=None, start_time=None, session=None):
        cycle = TaskLifeCycle()
        cycle.task_name = task_name
        cycle.execution_date = execution_date
        if backfill_time:
            cycle.backfill_time = backfill_time

        if clear_time:
            cycle.clear_time = clear_time

        if first_pipeline_time:
            cycle.first_pipeline_time = first_pipeline_time

        if ready_time:
            cycle.ready_time = ready_time

        if start_time:
            cycle.start_time = start_time

        session.add(cycle)
        session.commit()

    @staticmethod
    @provide_session
    def find_export_cycle(session=None):
        cycles = session.query(TaskLifeCycle).filter(TaskLifeCycle.is_export == 0,
                                                     TaskLifeCycle.start_time > 0).all()
        return cycles

    @staticmethod
    @provide_session
    def close_export(task_id, session=None):
        cycle = session.query(TaskLifeCycle).filter(TaskLifeCycle.id == task_id).one_or_none()
        if cycle:
            cycle.is_export = True
            session.merge(cycle)
            session.commit()