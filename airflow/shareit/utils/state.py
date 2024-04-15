# -*- coding: utf-8 -*-
"""
Author: zhangtao
CreateDate: 2021/4/22
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
#
from __future__ import unicode_literals

from builtins import object


class State(object):
    """
    Static class with task and dataset states constants  to
    avoid hardcoding.
    """
    # workflow
    NO_STATUS = "no_status"
    WORKFLOW_WAITING = "waiting"
    WORKFLOW_RUNNING = "running"
    WORKFLOW_FAILED = "failed"
    WORKFLOW_SUCCESS = "success"

    # taskInstance
    TASK_WAIT_UPSTREAM = "waiting"
    TASK_STOP_SCHEDULER = "termination"
    TASK_RUNNING = "running"
    TASK_FAILED = "failed"
    TASK_UP_FOR_RETRY = "up_for_retry"
    TASK_SUCCESS = "success"
    TASK_WAIT_QUEUE = "waiting_queue"
    # airflow task state
    QUEUED = "queued"
    SCHEDULED = "scheduled"


    task_state = [
        TASK_WAIT_UPSTREAM,
        TASK_STOP_SCHEDULER,
        TASK_RUNNING,
        TASK_FAILED,
        TASK_UP_FOR_RETRY,
        TASK_SUCCESS,
        TASK_WAIT_QUEUE
    ]
    task_state_map = {TASK_WAIT_UPSTREAM: u"检查上游",
                      TASK_STOP_SCHEDULER: u"终止检查上游",
                      TASK_RUNNING: u"任务运行中",
                      TASK_FAILED: u"任务失败",
                      TASK_UP_FOR_RETRY: u"任务等待重试",
                      TASK_SUCCESS: u"任务成功",
                      TASK_WAIT_QUEUE: u"等待调度队列资源"
                      }

    KILLED = "killed"

    # trigger
    WAITING = "waiting"
    UNKNOWN = "unknown"
    TRI_RUNNING = "running"
    BF_WAIT_DEP = "backfill_wait_dep"

    # dataset
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    BACKFILLING = "backfilling"  # 回算
    BACKFILLDP = "backfilldp"  # 回算依赖
    SUPPLEMENTING = "supplementing"  # 补数

    # sensor
    CHECKING = "checking"

    #sensor_type
    CHECK_PATH = "check_path"
    READY_TIME = "ready_time"
    trigger_states = (
        BF_WAIT_DEP,
        WAITING,
        RUNNING,
        SUCCESS,
        FAILED,
        TASK_STOP_SCHEDULER,
        UNKNOWN
    )

    dataset_states = (
        WAITING,
        RUNNING,
        BACKFILLING,
        BACKFILLDP,
        SUPPLEMENTING,
        SUCCESS,
        FAILED
    )

    flink_submit_success_states = ("INITIALIZING", "CREATED", "RUNNING", "FAILING",
                                   "CANCELLING", "RESTARTING", "RECONCILING")

    @classmethod
    def taskinstance_state_mapping(cls, state):
        if state == None or state == "scheduled":
            return cls.TASK_WAIT_QUEUE

        if state == "queued":
            return cls.TASK_RUNNING
        return state

    @classmethod
    def finished(cls):
        """
        A list of states indicating that a task started and completed a
        run attempt. Note that the attempt could have resulted in failure or
        have been interrupted; in any case, it is no longer running.
        """
        return [
            cls.SUCCESS,
            cls.FAILED,
            cls.KILLED,
            cls.TASK_STOP_SCHEDULER
        ]

    @classmethod
    def unfinished(cls):
        """
        A list of states indicating that a task either has not completed
        a run or has not even started.
        """
        return [
            cls.WAITING,
            cls.RUNNING,
            cls.BACKFILLING,
            cls.SUPPLEMENTING,
            cls.UNKNOWN,
            cls.CHECKING
        ]

    @classmethod
    def all(cls):
        return []

    @classmethod
    # 将taskinstance状态映射为真实的状态
    def taskinstance_state_transform(cls, state):
        transform_dict = {cls.TASK_WAIT_QUEUE: [None,"scheduled"],
                          cls.TASK_RUNNING: ["queued", cls.TASK_RUNNING]}
        if state in transform_dict.keys():
            return transform_dict[state]
        elif state is None:
            return state
        else:
            return [state]

    @classmethod
    def task_ins_state_map(cls, ins_state):
        if ins_state in cls.task_state:
            return cls.task_state_map[ins_state]
        else:
            return ins_state
