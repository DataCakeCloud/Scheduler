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

from sqlalchemy import Column, String, Integer, Boolean

from airflow.models.base import Base, ID_LEN, MESSAGE_LEN, OPTION_LEN
from airflow.utils.db import provide_session


class Dataset(Base):
    __tablename__ = "dataset"

    id = Column(Integer, primary_key=True)
    metadata_id = Column(String(ID_LEN))
    name = Column(String(MESSAGE_LEN))
    dataset_type = Column(String(OPTION_LEN))
    granularity = Column(String(OPTION_LEN))
    offset = Column(Integer)

    def get_id(self):
        return str(self.id)

    @staticmethod
    @provide_session
    def get_dataset(dataset_id=None, metadata_id=None, session=None):
        qry = session.query(Dataset)
        if dataset_id:
            qry = qry.filter(Dataset.id == dataset_id)
        if metadata_id:
            qry = qry.filter(Dataset.metadata_id == metadata_id)
        return qry.one_or_none()

    @staticmethod
    @provide_session
    def get_all_datasets(metadata_id, session=None):
        return session.query(Dataset) \
            .filter(Dataset.metadata_id == metadata_id).all()

    @staticmethod
    @provide_session
    def sync_ds_to_db(metadata_id=None, name=None, dataset_type=None, granularity=None, offset=None, session=None):
        dataset = Dataset.get_dataset(metadata_id=metadata_id)
        if dataset:
            pass
        else:
            dataset = Dataset()
            dataset.dataset_type = dataset_type
            dataset.metadata_id = metadata_id
            dataset.name = name
            dataset.granularity = granularity
            dataset.offset = offset
        session.merge(dataset)
        session.commit()
