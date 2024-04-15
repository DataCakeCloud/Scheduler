# -*- coding: utf-8 -*-
import json

from sqlalchemy import Column, String, Integer, DateTime, func, update
from sqlalchemy.dialects.mysql import MEDIUMTEXT

from airflow.models.base import Base, ID_LEN, OPTION_LEN
from airflow.shareit.utils.state import State
from airflow.utils.db import provide_session
from airflow.utils.timezone import utcnow


class WorkflowInstance(Base):
    __tablename__ = "workflow_instance"

    id = Column(Integer, primary_key=True)
    workflow_id = Column(String(ID_LEN))
    # workflow_name = Column(String(ID_LEN))
    execution_date = Column(DateTime)
    state = Column(String(OPTION_LEN))
    version = Column(String(ID_LEN))
    duration = Column(Integer,default=0)
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    create_date = Column(DateTime, server_default=func.now())
    update_date = Column(DateTime, server_default=func.now(), onupdate=func.now())
    tenant_id = Column(Integer)

    @staticmethod
    @provide_session
    def find_workflow_instances_by_date_range(workflow_id=None, start_date=None, end_date=None, state=None, limit=None,tenant_id=1,session=None):
        qry = session.query(WorkflowInstance).filter(WorkflowInstance.tenant_id == tenant_id)
        if workflow_id:
            qry = qry.filter(WorkflowInstance.workflow_id == workflow_id)
        if start_date:
            qry = qry.filter(WorkflowInstance.execution_date >= start_date)
        if end_date:
            qry = qry.filter(WorkflowInstance.execution_date <= end_date)
        if state:
            qry = qry.filter(WorkflowInstance.state == state)
        if limit:
            qry = qry.order_by(WorkflowInstance.execution_date.desc()).limit(limit)
        else :
            qry = qry.order_by(WorkflowInstance.execution_date.desc())
        return qry.all()

    @staticmethod
    @provide_session
    def find_workflow_instance(workflow_id=None, execution_date=None, state=None,tenant_id=1, session=None):
        qry = session.query(WorkflowInstance).filter(WorkflowInstance.tenant_id == tenant_id)
        if workflow_id is not None:
            qry = qry.filter(WorkflowInstance.workflow_id == workflow_id)
        if execution_date is not None:
            qry = qry.filter(WorkflowInstance.execution_date == execution_date)
        if state is not None:
            qry = qry.filter(WorkflowInstance.state == state)
        return qry.one_or_none()

    @staticmethod
    @provide_session
    def get_latest_instance(workflow_id=None,num=1,tenant_id=1,session=None):
        res = session.query(WorkflowInstance).filter(WorkflowInstance.tenant_id == tenant_id) \
            .filter(WorkflowInstance.workflow_id == workflow_id) \
            .filter(WorkflowInstance.execution_date <= utcnow()) \
            .order_by(WorkflowInstance.execution_date.desc()).limit(num).all()
        return res

    @staticmethod
    @provide_session
    def check_has_instance(workflow_id=None,session=None):
        res = session.query(WorkflowInstance) \
            .filter(WorkflowInstance.workflow_id == workflow_id).limit(1).all()
        if res:
            return True
        return False

    @staticmethod
    @provide_session
    def list_instance(workflow_id, size=None, page=None, tenant_id=1, start_date=None, end_date=None, session=None):
        qry = session.query(WorkflowInstance).filter(WorkflowInstance.workflow_id == workflow_id,
                                                     WorkflowInstance.tenant_id == tenant_id)
        if start_date:
            qry = qry.filter(WorkflowInstance.execution_date >= start_date)
        if end_date:
            qry = qry.filter(WorkflowInstance.execution_date <= end_date)
        qry = qry.order_by(WorkflowInstance.execution_date.desc())
        if page and size:
            offset = (page - 1) * size
            return qry.limit(size).offset(offset).all()
        return qry.all()

    @staticmethod
    @provide_session
    def get_instance_count(workflow_id, tenant_id=1, start_date=None, end_date=None, session=None):
        qry = session.query(WorkflowInstance).filter(WorkflowInstance.workflow_id == workflow_id,
                                                     WorkflowInstance.tenant_id == tenant_id)
        if start_date:
            qry = qry.filter(WorkflowInstance.execution_date >= start_date)
        if end_date:
            qry = qry.filter(WorkflowInstance.execution_date <= end_date)
        return qry.count()

    @staticmethod
    @provide_session
    def get_last_instance(workflow_id,tenant_id=1,session=None):
        return session.query(WorkflowInstance).filter(WorkflowInstance.workflow_id == workflow_id,
                                                      WorkflowInstance.tenant_id == tenant_id).order_by(WorkflowInstance.execution_date.desc()).first()

    @staticmethod
    @provide_session
    def add_wi(workflow_id=None, execution_date=None, version=None, tenant_id=1,transaction=False,
                          session=None):
        if workflow_id is None or execution_date is None:
            return
        wi = WorkflowInstance.find_workflow_instance(workflow_id=workflow_id,
                                                     execution_date=execution_date, tenant_id=tenant_id,
                                                     session=session)
        if wi:
            return
        else:
            wi = WorkflowInstance()
            wi.workflow_id = workflow_id
            wi.execution_date = execution_date
            wi.version = version
            wi.state = State.WORKFLOW_WAITING
            wi.tenant_id = tenant_id
        session.merge(wi)
        if not transaction:
            session.commit()

    @staticmethod
    @provide_session
    def add_and_init_wi(workflow_id=None, execution_date=None, version=None,tenant_id=1, transaction=False,
                          session=None):
        if workflow_id is None or execution_date is None:
            return
        wi = WorkflowInstance.find_workflow_instance(workflow_id=workflow_id,tenant_id=tenant_id,
                                                     execution_date=execution_date)
        if wi:
            wi.duration = 0
            wi.start_date = None
            wi.end_date = None
        else:
            wi = WorkflowInstance()
            wi.workflow_id = workflow_id
            wi.execution_date = execution_date
            wi.tenant_id = tenant_id

        wi.version = version
        wi.state = State.WORKFLOW_WAITING
        session.merge(wi)
        if not transaction:
            session.commit()

    @staticmethod
    @provide_session
    def delete_by_execution_date_range(workflow_id, start_date, tenant_id=1, transaction=False, session=None):
        qry = session.query(WorkflowInstance) \
            .filter(WorkflowInstance.workflow_id == workflow_id) \
            .filter(WorkflowInstance.execution_date > start_date).filter(WorkflowInstance.tenant_id == tenant_id)
        qry.delete()
        if not transaction:
            session.commit()

    @staticmethod
    def stop_instance(workflow_id, tenant_id=1, transaction=False, session=None):
        instance_list = session.query(WorkflowInstance).filter(WorkflowInstance.tenant_id == tenant_id) \
            .filter(WorkflowInstance.workflow_id == workflow_id, WorkflowInstance.state.in_([State.WORKFLOW_WAITING,
                                                                                             State.WORKFLOW_RUNNING])) \
            .filter(WorkflowInstance.execution_date <= utcnow()) \
            .all()
        for instance in instance_list:
            instance.state = State.WORKFLOW_FAILED
            session.merge(instance)
        if not transaction:
            session.commit()
