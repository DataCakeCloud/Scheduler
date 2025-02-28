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
import six
import json
from typing import List
from jinja2 import Environment


def _inherited(cls):
    return set(cls.__subclasses__()).union(
        [s for c in cls.__subclasses__() for s in _inherited(c)]
    )


class DataSet(object):
    attributes = []  # type: List[str]
    type_name = "dataSet"

    def __init__(self, qualified_name=None, data=None, **kwargs):
        self._qualified_name = qualified_name
        self.context = None
        self._data = dict()

        self._data.update(dict((key, value) for key, value in six.iteritems(kwargs)
                               if key in set(self.attributes)))

        if data:
            if "qualifiedName" in data:
                self._qualified_name = data.pop("qualifiedName")

            self._data = dict((key, value) for key, value in six.iteritems(data)
                              if key in set(self.attributes))

    def set_context(self, context):
        self.context = context

    @property
    def qualified_name(self):
        if self.context:
            env = Environment()
            return env.from_string(self._qualified_name).render(**self.context)
        return self._qualified_name

    def __getattr__(self, attr):
        if attr in self.attributes:
            if self.context:
                env = Environment()
                # return env.from_string(self._data.get(attr)).render(**self.context)
                # dump to json here in order to be able to manage dicts and lists
                rendered = env.from_string(
                    json.dumps(self._data.get(attr))
                ).render(**self.context)
                return json.loads(rendered)

            return self._data.get(attr)

        raise AttributeError(attr)

    def __getitem__(self, item):
        return self.__getattr__(item)

    def __iter__(self):
        for key, value in six.iteritems(self._data):
            yield (key, value)

    def as_dict(self):
        attributes = dict(self._data)
        attributes.update({"qualifiedName": self.qualified_name})
        env = Environment()
        if self.context:
            for key, value in six.iteritems(attributes):
                # attributes[key] = env.from_string(value).render(**self.context)
                attributes[key] = json.loads(
                    env.from_string(json.dumps(value)).render(**self.context)
                )

        d = {
            "typeName": self.type_name,
            "attributes": attributes,
        }

        return d

    @staticmethod
    def map_type(name):
        for cls in _inherited(DataSet):
            if cls.type_name == name:
                return cls

        raise NotImplementedError("No known mapping for {}".format(name))


class DataBase(DataSet):
    type_name = "dbStore"
    attributes = ["dbStoreType", "storeUse", "source", "description", "userName",
                  "storeUri", "operation", "startTime", "endTime", "commandlineOpts",
                  "attribute_db"]


class File(DataSet):
    type_name = "fs_path"
    attributes = ["name", "path", "isFile", "isSymlink"]

    def __init__(self, name=None, data=None):
        super(File, self).__init__(name=name, data=data)

        self._qualified_name = 'file://' + self.name
        self._data['path'] = self.name


class S3Bucket(DataSet):
    attributes = ["name"]

    type_name = "aws_s3_bucket"

    def __init__(self, name=None, data=None):
        if name.startswith("s3://"):
            name = name[5:]
        name = name.split("/")[0]

        super(S3Bucket, self).__init__(name=name, data=data)

        self._qualified_name = self.name


class S3PseudoDir(DataSet):
    attributes = ["name", "objectPrefix", "bucket"]

    type_name = "aws_s3_pseudo_dir"

    def __init__(self, name=None, data=None):
        if name.startswith("s3://"):
            name = name[5:]
        name = name.split("/", 1)[-1]

        super(S3PseudoDir, self).__init__(name=name, data=data)

        self._qualified_name = self.name
        self._data['objectPrefix'] = self.name
        self._data['bucket'] = ""


class S3File(DataSet):
    pseudoDirectory = {}
    attributes = ["name"]

    type_name = "aws_s3_object"

    def __init__(self, name=None, data=None):
        super(S3File, self).__init__(name=name, data=data)

        self._qualified_name = self.name

        self._data['pseudoDirectory'] = self.pseudoDirectory


class HadoopFile(File):
    cluster_name = "none"
    attributes = ["name", "path", "clusterName"]

    type_name = "hdfs_file"

    def __init__(self, name=None, data=None):
        super(File, self).__init__(name=name, data=data)

        self._qualified_name = "{}@{}".format(self.name, self.cluster_name)
        self._data['path'] = self.name

        self._data['clusterName'] = self.cluster_name


class Operator(DataSet):
    type_name = "airflow_operator"

    # todo we can derive this from the spec
    attributes = ["dag_id", "task_id", "command", "conn_id", "name", "execution_date",
                  "start_date", "end_date", "inputs", "outputs"]
