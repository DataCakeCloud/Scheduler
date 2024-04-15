# -*- coding: utf-8 -*-

import json
import time
import random
import logging
import pendulum
from croniter import croniter_range

from airflow import macros
from airflow.shareit.grpc.TaskServiceApi import TaskServiceApi_pb2, TaskServiceApi_pb2_grpc
from airflow.shareit.models.alert import Alert
from airflow.shareit.models.task_desc import TaskDesc
from airflow.shareit.service.task_service import TaskService
from airflow.shareit.service.dataset_service import DatasetService
from airflow.shareit.service.diagnosis_service import DiagnosisService
from airflow.shareit.utils.airflow_date_param_transform import AirlowDateParamTransform
from airflow.shareit.utils.date_util import DateUtil
from airflow.shareit.utils.granularity import Granularity
from airflow.utils import timezone
from airflow.utils.timezone import beijing


class TaskServiceTempGrpcService(TaskServiceApi_pb2_grpc.TaskRpcServiceServicer):
    def fastBackfill(self, request, context, user_info=None):
        return