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

from typing import Any
from sqlalchemy import MetaData
from sqlalchemy.ext.declarative import declarative_base

from airflow.configuration import conf

SQL_ALCHEMY_SCHEMA = conf.get("core", "SQL_ALCHEMY_SCHEMA")

metadata = (
    None
    if not SQL_ALCHEMY_SCHEMA or SQL_ALCHEMY_SCHEMA.isspace()
    else MetaData(schema=SQL_ALCHEMY_SCHEMA)
)
Base = declarative_base(metadata=metadata)  # type: Any

ID_LEN = 250
OPTION_LEN = 50
MESSAGE_LEN = 512
