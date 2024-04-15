# -*- coding: utf-8 -*-
"""
Author: zhangtao
CreateDate: 2021/4/13
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
from datetime import datetime,timedelta

from sqlalchemy import (
    Column, Integer, String, DateTime,func
)

from airflow.models.base import Base, ID_LEN, OPTION_LEN
from airflow.shareit.utils.state import State
from airflow.utils import timezone
from airflow.utils.db import provide_session


def make_naive(value):
    naive = datetime(value.year,
                     value.month,
                     value.day,
                     value.hour,
                     value.minute,
                     value.second,
                     value.microsecond)

    return naive


class DatasetPartitionStatus(Base):
    __tablename__ = "dataset_partition_status"

    id = Column(Integer, primary_key=True)
    metadata_id = Column(String(ID_LEN))
    dataset_partition_sign = Column(DateTime)  # execution_date
    state = Column(String(OPTION_LEN))
    run_id = Column(String(ID_LEN))
    create_date = Column(DateTime,server_default=func.now())
    update_date = Column(DateTime,server_default=func.now(),onupdate=func.now())


    @provide_session
    def sync_to_db(self, metadata_id, dataset_partition_sign, state, run_id, session=None):
        if isinstance(dataset_partition_sign, datetime):
            dataset_partition_sign = make_naive(dataset_partition_sign)
        self.metadata_id = metadata_id
        self.dataset_partition_sign = dataset_partition_sign
        self.state = state
        self.run_id = run_id
        session.merge(self)
        session.commit()

    def get_state(self):
        return self.state

    def set_state(self, state):
        if self.state != state and state in State.dataset_states:
            self.state = state

    @provide_session
    def update_state(self, state, session=None):
        if self.state != state and state in State.dataset_states:
            self.state = state
            session.merge(self)

    @staticmethod
    @provide_session
    def get_state(metadata_id=None, dataset_partition_sign=None, run_id=None, session=None):
        if isinstance(dataset_partition_sign, datetime):
            dataset_partition_sign = make_naive(dataset_partition_sign)
        qry = session.query(DatasetPartitionStatus)
        if metadata_id:
            qry = qry.filter(DatasetPartitionStatus.metadata_id == metadata_id)
        if dataset_partition_sign:
            qry = qry.filter(DatasetPartitionStatus.dataset_partition_sign == dataset_partition_sign)
        if run_id:
            qry = qry.filter(DatasetPartitionStatus.run_id == run_id)
        ds_res = qry.all()
        return ds_res

    @staticmethod
    @provide_session
    def check_dataset_status(metadata_id=None, start_time=None, end_time=None, state=None, session=None):
        from airflow.utils.timezone import make_naive
        try:
            start_time = make_naive(start_time)
            end_time = make_naive(end_time)
        except Exception:
            pass
        qry = session.query(DatasetPartitionStatus)
        if metadata_id:
            qry = qry.filter(DatasetPartitionStatus.metadata_id == metadata_id)
        if start_time:
            qry = qry.filter(DatasetPartitionStatus.dataset_partition_sign >= start_time)
        if end_time:
            qry = qry.filter(DatasetPartitionStatus.dataset_partition_sign < end_time)
        if state:
            def spilt_state(group):
                delimiters = [",", ";"]
                for delimiter in delimiters:
                    if delimiter in group:
                        return [group.strip() for group in group.split(delimiter)]
                return [group]

            state_list = list(State.dataset_states)
            if isinstance(state, basestring):
                state_list = spilt_state(state)
            elif isinstance(state, list):
                state_list = state
            qry = qry.filter(DatasetPartitionStatus.state.in_(state_list))
        return qry.all()

    @staticmethod
    @provide_session
    def check_dataset_status_single_date(metadata_id=None, dataset_partition_sign=None, state=None, session=None):
        from airflow.utils.timezone import make_naive
        try:
            dataset_partition_sign = make_naive(dataset_partition_sign)
        except Exception:
            pass
        qry = session.query(DatasetPartitionStatus)
        if metadata_id:
            qry = qry.filter(DatasetPartitionStatus.metadata_id == metadata_id)
        if dataset_partition_sign:
            qry = qry.filter(DatasetPartitionStatus.dataset_partition_sign == dataset_partition_sign)
        if state:
            def spilt_state(group):
                delimiters = [",", ";"]
                for delimiter in delimiters:
                    if delimiter in group:
                        return [group.strip() for group in group.split(delimiter)]
                return [group]

            state_list = list(State.dataset_states)
            if isinstance(state, basestring):
                state_list = spilt_state(state)
            elif isinstance(state, list):
                state_list = state
            qry = qry.filter(DatasetPartitionStatus.state.in_(state_list))
        return qry.all()

    @staticmethod
    @provide_session
    def sync_dps_state_without_session(metadata_id=None, dataset_partition_sign=None, run_id=None,
                                       state=None,session=None):
        if isinstance(dataset_partition_sign, datetime):
            dataset_partition_sign = make_naive(dataset_partition_sign)
        qry = session.query(DatasetPartitionStatus)
        if metadata_id:
            qry = qry.filter(DatasetPartitionStatus.metadata_id == metadata_id)
        if dataset_partition_sign:
            qry = qry.filter(DatasetPartitionStatus.dataset_partition_sign == dataset_partition_sign)
        dps = qry.one_or_none()
        if not dps:
            dps = DatasetPartitionStatus()
            dps.metadata_id = metadata_id
            dps.dataset_partition_sign = dataset_partition_sign
            dps.run_id = run_id
            dps.state = state
        elif dps.state != state and state in State.dataset_states:
            # backfilldp不可以覆盖掉backfilling
            if dps.state == State.BACKFILLING and state == State.BACKFILLDP:
                return
            dps.state = state
        else:
            return
        session.merge(dps)

    @staticmethod
    @provide_session
    def sync_state(metadata_id=None, dataset_partition_sign=None, run_id=None, state=None, transaction=False,
                   is_backfill_state=False,session=None):
        if isinstance(dataset_partition_sign, datetime):
            dataset_partition_sign = make_naive(dataset_partition_sign)
        qry = session.query(DatasetPartitionStatus)
        if metadata_id:
            qry = qry.filter(DatasetPartitionStatus.metadata_id == metadata_id)
        if dataset_partition_sign:
            qry = qry.filter(DatasetPartitionStatus.dataset_partition_sign == dataset_partition_sign)
        dps = qry.one_or_none()
        if not dps:
            dps = DatasetPartitionStatus()
            dps.metadata_id = metadata_id
            dps.dataset_partition_sign = dataset_partition_sign
            dps.run_id = run_id
            dps.state = state
        elif dps.state != state and state in State.dataset_states:
            if dps.state == State.BACKFILLING and is_backfill_state:
                return
            dps.state = state
        else:
            return
        session.merge(dps)
        if not transaction:
            session.commit()

    @staticmethod
    @provide_session
    def get_latest_external_data(metadata_id=None, session=None):
        res = session.query(DatasetPartitionStatus) \
            .filter(DatasetPartitionStatus.metadata_id == metadata_id,
                    DatasetPartitionStatus.run_id != "External_data_generator_backCalculation") \
            .order_by(DatasetPartitionStatus.dataset_partition_sign.desc()).first()
        return res

    @staticmethod
    @provide_session
    def sync_dps_to_db(metadata_id, dataset_partition_sign, state, run_id, session=None):
        if isinstance(dataset_partition_sign, datetime):
            dataset_partition_sign = make_naive(dataset_partition_sign)
        dps = session.query(DatasetPartitionStatus) \
            .filter(DatasetPartitionStatus.metadata_id == metadata_id,
                    DatasetPartitionStatus.dataset_partition_sign == dataset_partition_sign).one_or_none()
        if dps is None:
            dps = DatasetPartitionStatus()
        dps.metadata_id = metadata_id
        dps.dataset_partition_sign = dataset_partition_sign
        dps.state = state
        dps.run_id = run_id
        session.merge(dps)
        session.commit()

    @staticmethod
    @provide_session
    def find(metadata_id,dataset_partition_sign=None,state=None,session=None):
        qry = session.query(DatasetPartitionStatus)
        qry = qry.filter(DatasetPartitionStatus.metadata_id == metadata_id)
        if dataset_partition_sign is not None:
            qry = qry.filter(DatasetPartitionStatus.dataset_partition_sign == dataset_partition_sign)

        if state is not None:
            qry = qry.filter(DatasetPartitionStatus.state == state)

        return qry.all()
