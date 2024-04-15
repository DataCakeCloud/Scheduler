# -*- coding: utf-8 -*-
"""
Author: zhangtao
CreateDate: 2021/4/7
"""
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from sqlalchemy import Column, String, Integer, Boolean, DateTime,func

from airflow.models.base import Base, ID_LEN, MESSAGE_LEN, OPTION_LEN
from airflow.shareit.utils.constant import Constant
from airflow.utils import timezone
from airflow.utils.db import provide_session
from datetime import datetime,timedelta

class EventDependsInfo(Base):
    __tablename__ = "event_depends_info"

    id = Column(Integer, primary_key=True)
    dag_id = Column(String(ID_LEN))
    type = Column(Integer)  # 1:任务成功事件
    depend_gra = Column(Integer) # 依赖此事件的粒度
    date_calculation_param = Column(String(MESSAGE_LEN))
    depend_id = Column(String(ID_LEN))
    create_date = Column(DateTime,server_default=func.now())
    update_date = Column(DateTime,server_default=func.now(),onupdate=func.now())
    tenant_id = Column(Integer)
    depend_ds_task_id = Column(Integer)

    def get_id(self):
        return str(self.id)

    @staticmethod
    @provide_session
    def get_dag_event_depends(dag_id, type=Constant.TASK_SUCCESS_EVENT, session=None):
        return session.query(EventDependsInfo).filter(EventDependsInfo.dag_id == dag_id).all()

    @staticmethod
    @provide_session
    def get_downstream_event_depends(dag_id=None,ds_task_id=None,type=None, down_task_id=None,tenant_id=1,session=None):
        qr = session.query(EventDependsInfo).filter(
            EventDependsInfo.depend_ds_task_id == ds_task_id,EventDependsInfo.tenant_id == tenant_id)

        if down_task_id is not None:
            qr = qr.filter(EventDependsInfo.dag_id == down_task_id)

        return qr.all()

    @provide_session
    def sync_event_depend_info_to_db(self,
                                     dag_id=None,
                                     type=1,
                                     date_calculation_param=None,
                                     depend_id=None,
                                     depend_gra=None,
                                     tenant_id=1,
                                     depend_ds_task_id=None,
                                     transaction=False,
                                     session=None):
        self.dag_id = dag_id
        self.type = type
        self.date_calculation_param = date_calculation_param
        self.depend_id = depend_id
        self.depend_gra = depend_gra
        self.tenant_id = tenant_id
        self.depend_ds_task_id = depend_ds_task_id
        session.merge(self)
        if not transaction:
            session.commit()

    @staticmethod
    @provide_session
    def delete(dag_id,transaction=False,session=None):
        qry = session.query(EventDependsInfo).filter(EventDependsInfo.dag_id == dag_id)
        qry.delete(synchronize_session='fetch')
        if not transaction:
            session.commit()
    @staticmethod
    @provide_session
    def update_depend_id(old_depend_id,new_depend_id,transaction=False,session=None):
        deps = session.query(EventDependsInfo).filter(EventDependsInfo.depend_id == old_depend_id).all()
        for dep in deps :
            dep.depend_id = new_depend_id
            session.merge(dep)
        if not transaction:
            session.commit()

    def get_depend_task_id(self):
        return '{}_{}'.format(self.tenant_id,self.depend_ds_task_id)
