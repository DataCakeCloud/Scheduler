# -*- coding: utf-8 -*-
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

from sqlalchemy import Column, String, Integer, Float

from airflow.models.base import Base
from airflow.utils.db import provide_session
from airflow.utils.sqlalchemy import UtcDateTime
from airflow.utils import timezone


class CpuMemory(Base):
    __tablename__ = "cpu_memory"

    id = Column(Integer, primary_key=True)
    hostname = Column(String(50), nullable=False)
    cpu = Column(Float, default=0.0)
    memory = Column(Float, default=0.0)
    create_time = Column(UtcDateTime, default=timezone.utcnow)

    def __repr__(self):
        return self.hostname

    @staticmethod
    @provide_session
    def get_resource(session=None):
        sql = """
            SELECT cm.id, cm.hostname, cm.cpu, cm.memory, cm.create_time
            FROM scheduler.cpu_memory cm
            JOIN (
              SELECT hostname, MAX(create_time) AS max_create_time
              FROM scheduler.cpu_memory
              GROUP BY hostname
            ) max_times ON cm.hostname = max_times.hostname AND cm.create_time = max_times.max_create_time
            ORDER BY cm.memory ASC
        """
        res = session.execute(sql).fetchall()
        session.close()
        return res


