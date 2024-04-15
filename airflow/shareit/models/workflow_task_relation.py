# -*- coding: utf-8 -*-
import json

import six
from sqlalchemy import Column, String, Integer, DateTime, func, update
from sqlalchemy.dialects.mysql import MEDIUMTEXT,TEXT

from airflow.models.base import Base, ID_LEN, OPTION_LEN
from airflow.utils.db import provide_session
from airflow.utils.timezone import utcnow


class WorkflowTaskRelation(Base):
    __tablename__ = "workflow_task_relation"
    workflow_id = Column(Integer, primary_key=True)
    version = Column(Integer, primary_key=True)
    task_ids = Column('task_ids', MEDIUMTEXT)
    relations = Column(MEDIUMTEXT)
    root_task_ids = Column(TEXT)
    create_date = Column(DateTime, server_default=func.now())
    update_date = Column(DateTime, server_default=func.now(), onupdate=func.now())
    tenant_id = Column(Integer)

    @staticmethod
    @provide_session
    def get_workflow_task_relation_info(workflow_id, version, tenant_id=1, session=None):
        workflow_relation = session.query(WorkflowTaskRelation).filter(WorkflowTaskRelation.workflow_id == workflow_id,
                                                                       WorkflowTaskRelation.version == version,
                                                                       WorkflowTaskRelation.tenant_id == tenant_id).one_or_none()

        return workflow_relation

    def get_relation(self):
        return json.loads(self.relations)

    @staticmethod
    @provide_session
    def get_workflow_relation_with_version(workflow_id, version, tenant_id=1, session=None):
        workflow_relation = session.query(WorkflowTaskRelation.workflow_id == workflow_id,
                                          WorkflowTaskRelation.version == version,
                                          WorkflowTaskRelation.tenant_id == tenant_id).one_or_none()
        if workflow_relation:
            relations = workflow_relation.relations
            relations_dict = json.loads(relations)
            return relations_dict
        return None

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

    def get_task_ids(self):
        return self.task_ids.split(",") if isinstance(self.task_ids, six.string_types) else self.task_ids

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

    @staticmethod
    @provide_session
    def add_and_update(workflow_id, version, tenant_id=1, task_ids=None, relations=None, root_task_ids=None,
                       transaction=False, session=None):
        workflow_relation = WorkflowTaskRelation.get_workflow_task_relation_info(workflow_id=workflow_id,
                                                                                 version=version, tenant_id=tenant_id,
                                                                                 session=session)
        if workflow_relation:
            workflow_relation.task_ids = task_ids
            workflow_relation.relations = relations
            workflow_relation.root_task_ids = root_task_ids
        else:
            workflow_relation = WorkflowTaskRelation()
            workflow_relation.task_ids = task_ids
            workflow_relation.relations = relations
            workflow_relation.version = version
            workflow_relation.workflow_id = workflow_id
            workflow_relation.root_task_ids = root_task_ids
            workflow_relation.tenant_id = tenant_id
        session.merge(workflow_relation)
        if not transaction:
            session.commit()
