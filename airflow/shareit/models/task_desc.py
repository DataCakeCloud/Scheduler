# -*- coding: utf-8 -*-
"""
Author: zhangtao
CreateDate: 2021/4/15
"""
import json
import logging
import traceback
from datetime import datetime, timedelta

from sqlalchemy import Column, String, Integer, DateTime, func, update, Boolean
from sqlalchemy.dialects.mysql import MEDIUMTEXT,TEXT

from airflow.models.base import Base, ID_LEN, OPTION_LEN, MESSAGE_LEN
from airflow.shareit.utils.normal_util import NormalUtil
from airflow.utils import timezone
from airflow.utils.db import provide_session
from airflow.shareit.utils.granularity import Granularity
from airflow.configuration import conf

class TaskDesc(Base):
    __tablename__ = "task_desc"

    id = Column(Integer, primary_key=True)
    task_name = Column(String(ID_LEN))
    owner = Column(String(OPTION_LEN))
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    avg_run_time = Column(Integer)
    retries = Column(Integer)
    email = Column(String(MESSAGE_LEN))
    _input_datasets = Column(String(MESSAGE_LEN))
    _output_datasets = Column(String(MESSAGE_LEN))
    deadline = Column(String(ID_LEN))
    property_weight = Column(Integer)
    _extra_param = Column(MEDIUMTEXT)
    _tasks = Column(MEDIUMTEXT)
    last_update = Column(DateTime)
    _source_json = Column(MEDIUMTEXT)
    granularity = Column(String(50))
    create_date = Column(DateTime, server_default=func.now())
    update_date = Column(DateTime, server_default=func.now(), onupdate=func.now())
    crontab = Column(String, default=None)
    task_version = Column(Integer)
    workflow_name = Column(String(ID_LEN))
    is_temp = Column(Boolean, default=False)
    ds_task_id = Column(Integer)
    ds_task_name = Column(String(ID_LEN))
    tenant_id = Column(Integer)
    is_regular_alert = Column(Boolean, default=False)
    user_group_info = Column(String(300))
    template_code = Column(String(50))

    @property
    def input_datasets(self):
        return json.loads(self._input_datasets)

    @input_datasets.setter
    def input_datasets(self, value):
        self._input_datasets = json.dumps(value)

    def get_input_dataset_id(self):
        res = []
        for input_dateset in self.input_datasets:
            res.append(input_dateset['id'])
        return res

    @property
    def output_datasets(self):
        return json.loads(self._output_datasets)

    @output_datasets.setter
    def output_datasets(self, value):
        self._output_datasets = json.dumps(value)

    @property
    def extra_param(self):
        return json.loads(self._extra_param)

    @extra_param.setter
    def extra_param(self, value):
        self._extra_param = json.dumps(value)

    @property
    def tasks(self):
        return json.loads(self._tasks)

    @tasks.setter
    def tasks(self, value):
        self._tasks = json.dumps(value)

    @property
    def source_json(self):
        return json.loads(self._source_json)

    @source_json.setter
    def source_json(self, value):
        self._source_json = json.dumps(value)

    @provide_session
    def sync_task_desc_to_db(self,
                             task_name=None,
                             owner=None,
                             start_date=None,
                             end_date=None,
                             retries=None,
                             email=None,
                             input_datasets=None,
                             output_datasets=None,
                             deadline=None,
                             property_weight=None,
                             extra_param=None,
                             tasks=None,
                             source_json=None,
                             crontab=None,
                             task_version=None,
                             granularity=None,
                             workflow_name=None,
                             is_paused=None,
                             is_temp=False,
                             ds_task_id=None,
                             ds_task_name=None,
                             tenant_id=None,
                             is_regular_alert=False,
                             user_group_info=None,
                             template_code=None,
                             session=None
                             ):
        self.task_name = task_name
        self.owner = owner
        self.start_date = start_date
        self.end_date = end_date
        self.retries = retries
        self.email = email
        self.input_datasets = input_datasets
        self.output_datasets = output_datasets
        self.deadline = deadline
        self.property_weight = property_weight
        self.extra_param = extra_param
        self.tasks = tasks
        self.last_update = timezone.make_naive(timezone.utcnow())
        self.source_json = source_json
        self.granularity = granularity if granularity is not None else self.get_granularity()
        self.crontab = crontab
        self.task_version = task_version
        self.workflow_name = workflow_name
        self.is_temp = is_temp
        self.ds_task_id = ds_task_id
        self.ds_task_name = ds_task_name
        self.tenant_id = tenant_id
        self.is_regular_alert = is_regular_alert
        self.user_group_info = user_group_info
        self.template_code = template_code
        session.merge(self)

    @property
    def user_group(self):
        return json.loads(self.user_group_info)

    @staticmethod
    @provide_session
    def get_all_task(session=None):
        return session.query(TaskDesc).all()

    @staticmethod
    @provide_session
    def get_task_name_list(task_id=None, task_name=None, ds_task_name=None, granularity=None, template_code=None, tenant_id=None,owner=None,session=None):
        # 减少查询的数据量
        qry = session.query(TaskDesc.task_name, TaskDesc.user_group_info, TaskDesc.ds_task_name, TaskDesc.granularity,
                            TaskDesc.template_code,TaskDesc.owner).filter(TaskDesc.is_temp == False)
        if task_id:
            qry = qry.filter(TaskDesc.id == task_id)
        if task_name:
            qry = qry.filter(TaskDesc.task_name == task_name)
        if ds_task_name:
            qry = qry.filter(TaskDesc.ds_task_name.like('%'+ds_task_name+'%'))
        if owner:
            qry = qry.filter(TaskDesc.owner.like('%'+owner+'%'))
        if template_code:
            qry = qry.filter(TaskDesc.template_code == template_code)
        if granularity:
            qry = qry.filter(TaskDesc.granularity == granularity)
        if tenant_id:
            qry = qry.filter(TaskDesc.tenant_id == tenant_id)
        return qry.all()

    @staticmethod
    @provide_session
    def get_task(task_id=None, task_name=None, session=None):
        qry = session.query(TaskDesc)
        if task_id:
            qry = qry.filter(TaskDesc.id == task_id)
        if task_name:
            qry = qry.filter(TaskDesc.task_name == task_name)
        return qry.one_or_none()

    @staticmethod
    @provide_session
    def get_task_by_ds_task_id(ds_task_id=None, session=None):
        qry = session.query(TaskDesc)
        qry = qry.filter(TaskDesc.ds_task_id == ds_task_id, TaskDesc.is_temp == False)
        return qry.one_or_none()

    @staticmethod
    def get_task_with_tenant(tenant_id=1, ds_task_name=None, ds_task_id=None, contain_temp=False, specified_field=0, session=None):
        if session:
            try:
                result = TaskDesc.get_task_with_tenant_base(tenant_id=tenant_id, ds_task_name=ds_task_name,
                                                            ds_task_id=ds_task_id, contain_temp=contain_temp,
                                                            specified_field=specified_field, session=session)
            except Exception as e:
                logging.error(str(e))
                logging.error(traceback.format_exc())
                result = TaskDesc.get_task_with_tenant_base(tenant_id=tenant_id, ds_task_name=ds_task_name,
                                                            ds_task_id=ds_task_id, contain_temp=contain_temp,
                                                            specified_field=specified_field, session=session)
            return result
        else:
            try:
                result = TaskDesc.get_task_with_tenant_base(tenant_id=tenant_id, ds_task_name=ds_task_name,
                                                            ds_task_id=ds_task_id, contain_temp=contain_temp,
                                                            specified_field=specified_field)
            except Exception as e:
                logging.error(str(e))
                logging.error(traceback.format_exc())
                result = TaskDesc.get_task_with_tenant_base(tenant_id=tenant_id, ds_task_name=ds_task_name,
                                                            ds_task_id=ds_task_id, contain_temp=contain_temp,
                                                            specified_field=specified_field)
            return result

    @staticmethod
    @provide_session
    def get_task_with_tenant_base(tenant_id=1, ds_task_name=None, ds_task_id=None, contain_temp=False, specified_field=0, session=None):
        if specified_field == 1:
            qry = session.query(TaskDesc).with_entities(TaskDesc.task_name, TaskDesc.ds_task_name)
        else:
            qry = session.query(TaskDesc)
        if ds_task_name:
            qry = qry.filter(TaskDesc.ds_task_name == ds_task_name)
        if ds_task_id:
            qry = qry.filter(TaskDesc.ds_task_id == ds_task_id)
        # 是否允许查出临时任务
        if not contain_temp:
            qry = qry.filter(TaskDesc.is_temp == 0)
        qry = qry.filter(TaskDesc.tenant_id == tenant_id)
        return qry.one_or_none()

    @provide_session
    def delete_task(self, transaction=False, session=None):
        session.delete(self)
        if not transaction:
            session.commit()

    @staticmethod
    @provide_session
    def get_source_json(task_id=None, task_name=None, session=None):
        qry = session.query(TaskDesc)
        if task_id:
            qry = qry.filter(TaskDesc.id == task_id)
        if task_name:
            qry = qry.filter(TaskDesc.task_name == task_name)
        res = qry.one_or_none()
        return res.source_json if res else None

    def get_granularity(self):
        task_gra = -1
        for input_data in self.input_datasets:
            format_gra = Granularity.formatGranularity(input_data['granularity'])
            if not format_gra:
                raise ValueError("[DataPipeline] Illegal granularity in input dataset " + input_data['id'])
            if task_gra < Granularity.gra_priority_map[format_gra]:
                task_gra = Granularity.gra_priority_map[format_gra]
        self.granularity = task_gra
        return self.granularity

    def get_input_datasets(self):
        if isinstance(self.input_datasets, list):
            return self.input_datasets
        elif isinstance(self.input_datasets, str):
            return json.loads(self.input_datasets)
        else:
            return []

    def get_output_datasets(self):
        if isinstance(self.output_datasets, list):
            return self.output_datasets
        elif isinstance(self.output_datasets, str):
            return json.loads(self.output_datasets)
        else:
            return []

    @staticmethod
    def check_is_exists(task_name, session=None):
        if session:
            task = TaskDesc.get_task(task_name=task_name, session=session)
        else:
            task = TaskDesc.get_task(task_name=task_name)
        if task is None:
            return False
        return True

    @staticmethod
    def check_is_crontab_task(task_name):
        task_desc = TaskDesc.get_task(task_name=task_name)
        if task_desc.crontab is not None and task_desc.crontab != '':
            return True
        return False

    @staticmethod
    def update_name(old_name, new_name, transaction=False, session=None):
        task = session.query(TaskDesc).filter(TaskDesc.task_name == old_name).first()
        if task:
            task.task_name = new_name
            task_code = json.loads(task._source_json)
            task_code['name'] = new_name
            task._source_json = json.dumps(task_code, ensure_ascii=False)
            session.merge(task)
            if not transaction:
                session.commit()

    @staticmethod
    @provide_session
    def get_all_task_name(session=None):
        res = session.query(TaskDesc.task_name).all()
        return [name[0] for name in res]

    @staticmethod
    @provide_session
    def get_all_cron_task(session=None):
        res = session.query(TaskDesc).filter(TaskDesc.crontab is not None, TaskDesc.crontab != '',
                                             TaskDesc.is_temp == False).filter().all()
        return res

    @staticmethod
    @provide_session
    def get_task_version(task_name, session=None):
        qry = session.query(TaskDesc)
        res = qry.filter(TaskDesc.task_name == task_name).filter(TaskDesc.task_name == task_name).filter(
            TaskDesc.task_version is not None and TaskDesc.task_version != '').one_or_none()
        if res:
            return res.task_version

    @staticmethod
    @provide_session
    def get_success_file(id=None, task_name=None, session=None):
        qry = session.query(TaskDesc)
        if id:
            qry = qry.filter(TaskDesc.id == id)
        if task_name:
            qry = qry.filter(TaskDesc.task_name == task_name)
        td = qry.one_or_none()
        if td:
            output_datasets = td.source_json['output_datasets'][0]
            if output_datasets.has_key("fileName") and output_datasets.has_key("location"):
                fileName = output_datasets['fileName']
                if fileName:
                    if output_datasets['location'].endswith('/'):
                        return output_datasets['location'] + fileName
                    else:
                        return output_datasets['location'] + "/" + fileName

    @staticmethod
    @provide_session
    def get_success_dir(id=None, task_name=None, session=None):
        qry = session.query(TaskDesc)
        if id:
            qry = qry.filter(TaskDesc.id == id)
        if task_name:
            qry = qry.filter(TaskDesc.task_name == task_name)
        td = qry.one_or_none()
        if td:
            output_datasets = td.source_json['output_datasets'][0]
            if output_datasets.has_key("fileName") and output_datasets.has_key("location"):
                fileName = output_datasets['fileName']
                if fileName:
                    if output_datasets['location'].endswith('/'):
                        return output_datasets['location']
                    else:
                        return output_datasets['location'] + "/"

    @provide_session
    def remove_from_workflow(self, transaction=True, session=None):
        self.workflow_name = None
        session.merge(self)
        if transaction:
            session.commit()

    @staticmethod
    @provide_session
    def update_workflow_name(workflow_name=None, workflow_new_name=None, transaction=True, session=None):
        if workflow_name is not None and workflow_new_name is not None:
            session.execute(update(TaskDesc)
                            .where(TaskDesc.workflow_name == workflow_name)
                            .values(workflow_name=workflow_new_name)
                            )
        if transaction:
            session.commit()

    @staticmethod
    @provide_session
    def get_owner(task_name, session=None):
        task = session.query(TaskDesc).filter(TaskDesc.task_name == task_name).all()
        if task:
            return task[0].owner
        return ''

    @staticmethod
    @provide_session
    def get_owner_by_ds_task_name(ds_task_name, session=None):
        task = session.query(TaskDesc).filter(TaskDesc.ds_task_name == ds_task_name).all()
        if task:
            source_josn = json.loads(task[0]._source_json)
            notified_owner = None
            if source_josn.has_key("notified_owner"):
                notified_owner = source_josn["notified_owner"]
            if notified_owner:
                return notified_owner
            return task[0].owner
        else:
            task = session.query(TaskDesc).filter(TaskDesc.task_name == ds_task_name).all()
            if task:
                source_josn = json.loads(task[0]._source_json)
                notified_owner = None
                if source_josn.has_key("notified_owner"):
                    notified_owner = source_josn["notified_owner"]
                if notified_owner:
                    return notified_owner
                return task[0].owner
        return ''



    @staticmethod
    @provide_session
    def get_email(task_name, session=None):
        task = session.query(TaskDesc).filter(TaskDesc.task_name == task_name).all()
        if task:
            return task[0].email
        return ''

    @staticmethod
    @provide_session
    def get_by_dstask_id(ds_task_id,session=None):
       task = session.query(TaskDesc).filter(TaskDesc.ds_task_id == ds_task_id,TaskDesc.is_temp == False).one_or_none()
       return task

    @staticmethod
    @provide_session
    def remove_workflow_info(ds_task_id, tenant_id=1, transaction=False, session=None):
        task = session.query(TaskDesc).filter(TaskDesc.ds_task_id == ds_task_id,
                                              TaskDesc.tenant_id == tenant_id,TaskDesc.is_temp == False).one_or_none()
        task.workflow_name = ''
        session.merge(task)
        if not transaction:
            session.commit()

    @staticmethod
    @provide_session
    def get_version(dag_id, session=None):
        task = session.query(TaskDesc).filter(TaskDesc.task_name == dag_id).all()
        if task and task[0].task_version:
            return task[0].task_version
        else:
            return 0

    @staticmethod
    @provide_session
    def get_tenant_id(task_name,session=None):
        td = session.query(TaskDesc.tenant_id).filter(TaskDesc.task_name == task_name,TaskDesc.is_temp == False).first()
        if td:
            return td.tenant_id
        else:
            return 1

    @staticmethod
    @provide_session
    def get_regular_alert_all_task(session=None):
        task = session.query(TaskDesc).filter(TaskDesc.is_regular_alert == True).all()
        return task

    @staticmethod
    @provide_session
    def get_cluster_manager_info(task_name,session=None):
        td = session.query(TaskDesc).filter(TaskDesc.task_name == task_name,TaskDesc.is_temp == False).first()
        if td:
            try:
                tenant_id = td.tenant_id
                task_json_str = td._tasks
                task_json = json.loads(task_json_str)
                cluster_tags = task_json[0].get('cluster_tags','')
                cluster_tags = NormalUtil.cluster_tag_mapping_replace(cluster_tags)
                cluster_region, provider = NormalUtil.parse_cluster_tag(cluster_tags)
                return tenant_id,cluster_region,provider
            except:
                return 1,'',''
        else:
            return 1,'',''

    def get_check_expire_time(self):
        sj = self.source_json
        # 单位是h
        expire_time = sj.get('check_expiration_time', 0)
        if not expire_time:
            expire_time = conf.getint("core","default_check_expiration_time")
        return expire_time


