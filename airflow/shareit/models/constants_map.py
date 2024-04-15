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

from sqlalchemy import Column, String, Integer

from airflow.models.base import Base, MESSAGE_LEN
from airflow.utils.db import provide_session


class ConstantsMap(Base):
    __tablename__ = "constants_map"

    id = Column(Integer, primary_key=True)
    _type = Column(String(MESSAGE_LEN))
    key = Column(String(MESSAGE_LEN))
    value = Column(String(MESSAGE_LEN))

    @staticmethod
    @provide_session
    def get_value(_type=None, key=None, session=None):
        res = session.query(ConstantsMap.value) \
            .filter(ConstantsMap._type == _type, ConstantsMap.key == key) \
            .one_or_none()
        if res:
            return res[0]
        else:
            return ''

    @staticmethod
    @provide_session
    def get_value_to_dict(_type=None,session=None):
        res = session.query(ConstantsMap) \
            .filter(ConstantsMap._type == _type) \
            .all()
        if res:
            return {i.key:i.value for i in res}
        else :
            return {}
