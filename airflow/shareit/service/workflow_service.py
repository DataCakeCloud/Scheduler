# -*- coding: utf-8 -*-
"""
Author: Tao Zhang
Date: 2022/6/16
If you are doing your best,you will not have to worry about failure.
"""
import copy
import json
import time
import traceback

import six
import pendulum
from datetime import datetime, timedelta

from croniter import croniter, croniter_range

from airflow import LoggingMixin
from airflow.shareit.models.dag_dataset_relation import DagDatasetRelation
from airflow.shareit.models.event_depends_info import EventDependsInfo
from airflow.shareit.models.trigger import Trigger
from airflow.shareit.models.workflow_task_relation import WorkflowTaskRelation
from airflow.shareit.utils.date_calculation_util import DateCalculationUtil
from airflow.shareit.utils.date_util import DateUtil
from airflow.shareit.utils.granularity import Granularity
from airflow.shareit.grpc.client.greeter_client import GreeterClient
from airflow.shareit.utils.normal_util import NormalUtil
from airflow.shareit.utils.state import State
from airflow.shareit.utils.task_cron_util import TaskCronUtil
from airflow.shareit.workflow.workflow_state_sync import WorkflowStateSync
from airflow.utils import timezone
from airflow.utils.db import provide_session
from airflow.models import DagModel, TaskInstance
from airflow.models.dagrun import DagRun
from airflow.utils.timezone import beijing, utcnow

from airflow.shareit.utils.task_manager import get_dag
from airflow.shareit.service.task_service import TaskService
from airflow.shareit.models.workflow import Workflow
from airflow.shareit.models.task_desc import TaskDesc
from airflow.shareit.models.workflow_instance import WorkflowInstance

log = LoggingMixin().logger


class WorkflowService(object):

    def __init__(self):
        pass

    @staticmethod
    def get_workflow_task_execution_date(workflow_execution_date, gra, task_name):
        gra = Granularity.getIntGra(gra)
        if gra == 6:
            start_date = workflow_execution_date
            end_date = workflow_execution_date + timedelta(hours=1)
        elif gra == 5:
            start_date = workflow_execution_date
            end_date = workflow_execution_date + timedelta(days=1)
        elif gra == 4:
            start_date = workflow_execution_date
            end_date = workflow_execution_date + timedelta(days=7)
        elif gra == 3:
            start_date = workflow_execution_date
            end_date = DateCalculationUtil.datetime_month_caculation(workflow_execution_date, 1)
        else:
            return workflow_execution_date

        return DateCalculationUtil.get_execution_date_in_date_range(start_date=start_date, end_date=end_date,
                                                                    task_name=task_name)

    @staticmethod
    def list_instance_date(workflow_id, size, num,tenant_id=1):
        count = WorkflowInstance.get_instance_count(workflow_id,tenant_id=tenant_id,end_date=utcnow())
        instance_list = WorkflowInstance.list_instance(workflow_id,tenant_id=tenant_id, size=size, page=num,end_date=utcnow())
        instance_timestamps = []
        for instance in instance_list:
            instance_timestamps.append(DateUtil.get_timestamp(instance.execution_date))
        data = {
            'instanceTimestamps': instance_timestamps,
            'count': count
        }
        return data

    @staticmethod
    def list_instance(workflow_id, size, num,tenant_id=1,start_date=None,end_date=None):
        instance_list = WorkflowInstance.list_instance(workflow_id, size, num,tenant_id=tenant_id,start_date=start_date,end_date=end_date)
        cnt = WorkflowInstance.get_instance_count(workflow_id,tenant_id=tenant_id,start_date=start_date,end_date=end_date)
        instance_result_list = []
        for instance in instance_list:
            body = {
                'instanceID': instance.id,
                'instanceState': instance.state,
                'executionDate': DateUtil.get_timestamp(instance.execution_date),
                'startDate': DateUtil.get_timestamp(instance.start_date) if instance.start_date else None,
                'endDate': DateUtil.get_timestamp(instance.end_date) if instance.end_date else None,
                'duration': instance.duration
            }
            instance_result_list.append(body)
        data = {
            'instanceList': instance_result_list,
            'total' : cnt,
            'pageNum' : num,
            'pageSize' : len(instance_result_list)
        }
        return data

    @staticmethod
    @provide_session
    def batch_clear(workflow_id, execution_dates, tasks,tenant_id=1,session=None):
        try:
            transaction=True
            for execution_date in execution_dates:
                WorkflowStateSync.open_wi(workflow_id,execution_date,tenant_id=tenant_id,transaction=transaction,session=session)

            for task in tasks:
                task_name = task['taskName']
                TaskService.clear_instance_transaction(task_name, execution_dates=execution_dates,
                                                       transaction=transaction, session=session)
            session.commit()
        except Exception as e:
            log.error(traceback.format_exc())
            log.error('[DataStudio Workflow] batch clear failed !!!')
            session.rollback()
            raise e


    @staticmethod
    @provide_session
    def batch_backfill(workflow_id, start_date, end_date, is_send_notify,tenant_id=1,session=None):
        transaction=True
        try:
            workflow = Workflow.get_workflow(workflow_id,tenant_id=tenant_id,session=session)
            tasks = workflow.get_tasks(session=session)
            crontab = workflow.crontab
            all_cron_dates = croniter_range(start_date, end_date, crontab)
            execution_date_list = []
            # 为这些日期注册trigger
            for cron_date in all_cron_dates:
                WorkflowInstance.add_and_init_wi(workflow_id=workflow_id,execution_date=cron_date,version=workflow.version,
                                                   transaction=transaction,session=session)
                execution_date_list.append(cron_date.replace(tzinfo=beijing))

            task_names = []
            for task in tasks:
                task_names.append(task.task_name)
            TaskService.backfill_transaction(task_name_list=task_names, execution_date_list=execution_date_list,
                                 is_send_notify=is_send_notify,transaction=transaction,session=session)
        except Exception:
            session.rollback()
            log.error("[DataStudio Workflow] batch backfill failed")
            log.error(traceback.format_exc())
        return

    @staticmethod
    @provide_session
    def get_taskinstance_baseinfo(workflow_id, execution_date, task_name, ds_task_id,ds_task_name,tenant_id=1,session=None):
        from airflow.shareit.utils.state import State as new_state
        gc = GreeterClient()
        ds_task_info_list = gc.getTaskInfoByID([{'taskId':ds_task_id}],tenant_id).info
        ds_task_info = None
        if len(ds_task_info_list) > 0:
            ds_task_info=ds_task_info_list[0]
        task_desc = TaskDesc.get_task(task_name=task_name,session=session)
        tris = Trigger.find(dag_id=task_name, execution_date=execution_date,session=session)
        tis = TaskInstance.find_by_execution_date(dag_id=task_name, execution_date=execution_date,session=session)

        owner = ds_task_info.owner if ds_task_info else None
        update_time = ds_task_info.updateTime if ds_task_info else None
        runtime_config = ds_task_info.runtimeConfig if ds_task_info else None
        runtime_config_dict = json.loads(runtime_config) if ds_task_info else None
        cluster_type = runtime_config_dict.get('acrossCloud',None) if ds_task_info else None
        cluster_resources = runtime_config_dict.get('resourceLevel',None) if ds_task_info else None
        alert_type = runtime_config_dict.get('alertType',None) if ds_task_info else None
        alert_method = runtime_config_dict.get('alertMethod',None) if ds_task_info else None
        crontab = task_desc.crontab

        state = None
        try_number = 0
        start_date = None
        end_date = None
        duration = 0
        genie_job_id = ''
        genie_job_url = ''

        if tris and tris[0] == new_state.WAITING:
            state = new_state.WAITING

        if tis:
            try_number = tis[0].try_number - 1 if tis[0].try_number > 0 else 0

            duration = tis[0].get_duration()
            genie_job_id, genie_job_url = tis[0].get_genie_info()

            # 这代表不是waiting状态的
            if not state:
                end_date = DateUtil.get_timestamp(tis[0].end_date) if tis[0].end_date else None
                start_date = DateUtil.get_timestamp(tis[0].start_date) if tis[0].start_date else None
                state = State.taskinstance_state_mapping(TaskService.state_merge(tis))

        data = {
            'taskName': ds_task_name,
            'taskID': ds_task_id,
            'owner': owner,
            'updateTime': update_time,
            'taskinstanceID' : genie_job_id,
            'state': state,
            'tryNumber': try_number,
            'executionDate': DateUtil.get_timestamp(execution_date),
            'startDate': start_date,
            'endDate': end_date,
            'duration': duration,
            'crontab': crontab,
            'clusterType': cluster_type,
            'clusterResources': cluster_resources,
            'alertType': alert_type,
            'alertMethod': alert_method,
            'genieJobUrl': genie_job_url
        }
        return data

    @staticmethod
    def list_workflow_taskinstance_latest(workflow_id_list, num,tenant_id=1):
        data = {}
        for workflow_id in workflow_id_list:
            workflow_instances = WorkflowInstance.get_latest_instance(workflow_id, num, tenant_id=tenant_id)
            arr = []
            workflow_instances = sorted(workflow_instances,key=lambda x:x.execution_date)
            for instance in workflow_instances:
                arr.append({
                    'instanceID': instance.id,
                    'state': instance.state,
                    'executionDate': DateUtil.get_timestamp(instance.execution_date),
                    'startDate':  DateUtil.get_timestamp(instance.start_date) if instance.start_date else None,
                    'endDate': DateUtil.get_timestamp(instance.end_date) if instance.end_date else None,
                    'duration': instance.duration
                })
            data[workflow_id] = arr
        return data


    @staticmethod
    def list_workflow_next_execution_date(workflow_id_list,tenant_id=1):
        data = {}
        for workflow_id in workflow_id_list:
            try:
                workflow = Workflow.get_workflow(workflow_id,tenant_id=tenant_id)
                if workflow:
                    crontab = workflow.crontab
                    now_date = timezone.utcnow().replace(tzinfo=beijing)
                    cron = croniter(crontab, now_date)
                    cron.tzinfo = beijing
                    next_execution_date = cron.get_next(datetime)
                    data[workflow_id] = DateUtil.get_timestamp(next_execution_date)
            except Exception as e:
                log.error(traceback.format_exc())
        return data

    @staticmethod
    def list_workflow_down_task(workflow_id):
        resul_tasks=[]
        workflow = Workflow.get_workflow(workflow_id)
        if workflow:
            tasks = workflow.get_tasks()
            task_name_set = set()
            for task in tasks:
                task_name_set.add(task.task_name)

            for task in tasks:
                dowen_depend_list = EventDependsInfo.get_downstream_event_depends(task.task_name)
                for depend in dowen_depend_list:
                    if depend.dag_id in task_name_set:
                        continue
                    owners = TaskDesc.get_owner(depend.dag_id)
                    owner = str(owners).split(',')[0]
                    resul_tasks.append({
                        'curTaskName': task.task_name,
                        'downTaskName': depend.dag_id,
                        'downTaskOwner': owner
                    })

        data = {
            'tasks': resul_tasks
        }
        return data



