# -*- coding: utf-8 -*-
import json

import six
from sqlalchemy import Column, String, Integer, Boolean, DateTime, func
from sqlalchemy.dialects.mysql import MEDIUMTEXT
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import synonym

from airflow.models.base import Base, ID_LEN
from airflow.utils.db import provide_session
from airflow.utils.timezone import utcnow,my_make_naive


class Workflow(Base):
    __tablename__ = "workflow"

    workflow_id = Column(Integer, primary_key=True)
    workflow_name = Column(String(ID_LEN))
    task_ids = Column('task_ids', MEDIUMTEXT)
    create_date = Column(DateTime, server_default=func.now())
    update_date = Column(DateTime, server_default=func.now(), onupdate=func.now())
    version = Column(String(ID_LEN))
    granularity = Column(String(50))
    crontab = Column(String(50))
    is_online = Column(Boolean)
    tenant_id = Column(Integer)

    @staticmethod
    @provide_session
    def get_all_online_workflow(session=None):
        return session.query(Workflow).filter(Workflow.is_online == True).all()

    def get_task_ids(self):
        return self.task_ids.split(",") if isinstance(self.task_ids, six.string_types) else self.task_ids

    def get_tasks(self,session=None):
        from airflow.shareit.models.task_desc import TaskDesc
        result = []
        task_ids = self.get_task_ids()
        if task_ids:
            for id in task_ids:
                if session:
                    task = TaskDesc.get_task_with_tenant(ds_task_id=id, tenant_id=self.tenant_id, session=session)
                else:
                    task = TaskDesc.get_task_with_tenant(ds_task_id=id, tenant_id=self.tenant_id)
                if task:
                    result.append(task)
        return result

    def get_root_task_ids(self):
        return self.root_task_ids.split(",") if isinstance(self.root_task_ids, six.string_types) else self.root_task_ids

    def get_root_tasks(self,session=None):
        from airflow.shareit.models.task_desc import TaskDesc
        result = []
        task_ids = self.get_root_task_ids()
        if task_ids:
            for id in task_ids:
                if session:
                    task = TaskDesc.get_task_with_tenant(ds_task_id=id, tenant_id=self.tenant_id, session=session)
                else:
                    task = TaskDesc.get_task_with_tenant(ds_task_id=id, tenant_id=self.tenant_id)
                if task:
                    result.append(task)
        return result

    def set_tasks(self, tasks):
        if isinstance(tasks, list):
            tasks = ",".join(tasks)
        if self._tasks != tasks:
            self._tasks = tasks


    @declared_attr
    def tasks(self):
        return synonym('_tasks',
                       descriptor=property(self.get_tasks, self.set_tasks))

    @staticmethod
    @provide_session
    def get_workflow(workflow_id=None,tenant_id=1,session=None):
        qry = session.query(Workflow).filter(Workflow.workflow_id == workflow_id, Workflow.tenant_id == tenant_id)
        return qry.one_or_none()

    @staticmethod
    @provide_session
    def find_a_workflow(workflow_id=None, workflow_name=None, tenant_id=1, session=None):
        qry = session.query(Workflow).filter(Workflow.tenant_id == tenant_id)
        if workflow_id is not None:
            qry = qry.filter(Workflow.id == workflow_id)
        if workflow_name is not None:
            qry = qry.filter(Workflow.name == workflow_name)
        return qry.one_or_none()

    @staticmethod
    @provide_session
    def add_and_update(workflow_id, workflow_name, task_ids=None, version=None, granularity=None, crontab=None,
                       tenant_id=1, transaction=False, session=None):
        workflow = Workflow.get_workflow(workflow_id=workflow_id,tenant_id=tenant_id,session=session)
        if workflow:
            workflow.task_ids = task_ids
            workflow.version = version
            workflow.granularity = workflow.granularity
            workflow.crontab = crontab
            workflow.is_online = True
            workflow.workflow_name = workflow_name
        else:
            workflow = Workflow()
            workflow.task_ids = task_ids
            workflow.workflow_name = workflow_name
            workflow.version = version
            workflow.granularity = granularity
            workflow.crontab = crontab
            workflow.workflow_id = workflow_id
            workflow.is_online = True
            workflow.tenant_id = tenant_id
        session.merge(workflow)
        if not transaction:
            session.commit()

    @provide_session
    def rename(self, new_name=None, transaction=True, session=None):
        if self.name != new_name:
            self.name = new_name
        session.merge(self)
        if transaction:
            session.commit()

    @staticmethod
    @provide_session
    def delete_workfow(workflow_id=None, tenant_id=1, transaction=False, session=None):
        qry = session.query(Workflow).filter(Workflow.workflow_id == workflow_id, Workflow.tenant_id == tenant_id)
        qry.delete(synchronize_session='fetch')
        if not transaction:
            session.commit()
