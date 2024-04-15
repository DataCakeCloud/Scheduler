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
#
from airflow.configuration import conf
from airflow.lineage import datasets
from airflow.lineage.backend import LineageBackend
from airflow.lineage.backend.atlas.typedefs import operator_typedef
from airflow.utils.timezone import convert_to_utc
from airflow.utils.log.logging_mixin import LoggingMixin

from atlasclient.client import Atlas
from atlasclient.exceptions import HttpError

SERIALIZED_DATE_FORMAT_STR = "%Y-%m-%dT%H:%M:%S.%fZ"

_username = conf.get("atlas", "username")
_password = conf.get("atlas", "password")
_port = conf.get("atlas", "port")
_host = conf.get("atlas", "host")


log = LoggingMixin().log


class AtlasBackend(LineageBackend):
    def get_guid(self, bucket_res):
        log.debug("atlas res: %s", str(bucket_res))
        bucket_id = ""
        if "mutatedEntities" in bucket_res:
            if "CREATE" in bucket_res["mutatedEntities"]:
                bucket_id = bucket_res["mutatedEntities"]["CREATE"][0]["guid"]
            elif "UPDATE" in bucket_res["mutatedEntities"]:
                bucket_id = bucket_res["mutatedEntities"]["UPDATE"][0]["guid"]
        return bucket_id

    def send_atlas(self, client, entity):
        data_dict = entity.as_dict()

        if entity.type_name == "aws_s3_object":
            s3_name = entity.qualified_name
            if s3_name.startswith("s3://"):
                s3_name = s3_name[5:]
            s3_bucket = s3_name.split("/")[0]
            s3_prefix = s3_name.split("/", 1)[-1]

            prefix_id = ""

            data = {"entity": {"typeName": "aws_s3_bucket", "attributes":
                {"qualifiedName": s3_bucket, "name": s3_bucket}}}
            try:
                # create aws_s3_bucket and get aws_s3_bucket object id
                bucket_res = client.entity_post.create(data=data)
                bucket_id = self.get_guid(bucket_res)

                # create aws_s3_pseudo_dir and get aws_s3_pseudo_dir object id
                data = {"entity": {"typeName": "aws_s3_pseudo_dir", "attributes":
                    {"qualifiedName": s3_prefix, "bucket": {"typeName": "aws_s3_bucket",
                        "guid": bucket_id}, "name": s3_prefix, "objectPrefix": s3_prefix}}}
                bucket_res = client.entity_post.create(data=data)
                prefix_id = self.get_guid(bucket_res)
            except Exception as e:
                log.debug("atlas aws_s3_object except: %s", e)

            data_dict["attributes"]["pseudoDirectory"] = \
                {"typeName": "aws_s3_pseudo_dir", "guid": prefix_id}

        client.entity_post.create(data={"entity": data_dict})

    def send_lineage(self, operator, inlets, outlets, context):
        client = Atlas(_host, port=_port, username=_username, password=_password)
        try:
            client.typedefs.create(data=operator_typedef)
        except HttpError:
            client.typedefs.update(data=operator_typedef)

        _execution_date = convert_to_utc(context['ti'].execution_date)
        _start_date = convert_to_utc(context['ti'].start_date)
        _end_date = convert_to_utc(context['ti'].end_date)

        inlet_list = []
        if inlets:
            for entity in inlets:
                if entity is None:
                    continue

                entity.set_context(context)
                # client.entity_post.create(data={"entity": entity.as_dict()})
                self.send_atlas(client, entity)
                inlet_list.append({"typeName": entity.type_name,
                                   "uniqueAttributes": {
                                       "qualifiedName": entity.qualified_name
                                   }})

        outlet_list = []
        if outlets:
            for entity in outlets:
                if not entity:
                    continue

                entity.set_context(context)
                # client.entity_post.create(data={"entity": entity.as_dict()})
                self.send_atlas(client, entity)
                outlet_list.append({"typeName": entity.type_name,
                                    "uniqueAttributes": {
                                        "qualifiedName": entity.qualified_name
                                    }})

        operator_name = operator.__class__.__name__
        name = "{} {} ({})".format(operator.dag_id, operator.task_id, operator_name)
        qualified_name = "{}_{}_{}@{}".format(operator.dag_id,
                                              operator.task_id,
                                              _execution_date,
                                              operator_name)

        data = {
            "dag_id": operator.dag_id,
            "task_id": operator.task_id,
            "execution_date": _execution_date.strftime(SERIALIZED_DATE_FORMAT_STR),
            "name": name,
            "inputs": inlet_list,
            "outputs": outlet_list,
            "command": operator.lineage_data,
        }

        if _start_date:
            data["start_date"] = _start_date.strftime(SERIALIZED_DATE_FORMAT_STR)
        if _end_date:
            data["end_date"] = _end_date.strftime(SERIALIZED_DATE_FORMAT_STR)

        process = datasets.Operator(qualified_name=qualified_name, data=data)
        client.entity_post.create(data={"entity": process.as_dict()})
