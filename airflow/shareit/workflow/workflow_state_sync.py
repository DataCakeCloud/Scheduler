# -*- coding: utf-8 -*-
from airflow.utils import timezone

from airflow import LoggingMixin
from airflow.models import TaskInstance
from airflow.shareit.models.trigger import Trigger
from airflow.shareit.models.workflow import Workflow
from airflow.shareit.models.workflow_instance import WorkflowInstance
from airflow.shareit.service.task_service import TaskService
from airflow.shareit.utils.state import State
from airflow.utils.db import provide_session
from airflow.utils.timezone import make_naive


class WorkflowStateSync(LoggingMixin):

    @staticmethod
    @provide_session
    def open_wi(workflow_id,execution_date,tenant_id=1,transaction=False,session=None):
        '''
            Args:
                workflow_id
                execution_date
        '''
        if session:
            instance = WorkflowInstance.find_workflow_instance(workflow_id=int(workflow_id),
                                                               execution_date=execution_date, tenant_id=tenant_id,
                                                               session=session)
        else:
            instance = WorkflowInstance.find_workflow_instance(workflow_id=int(workflow_id),
                                                               execution_date=execution_date, tenant_id=tenant_id)
        if instance:
            if instance.state == State.WORKFLOW_RUNNING:
                return
            instance.state = State.WORKFLOW_RUNNING
            if not instance.start_date:
                instance.start_date = make_naive(timezone.utcnow())
            session.merge(instance)
            if not transaction:
                session.commit()

    @staticmethod
    @provide_session
    def ti_push_state_2_wi(workflow_id,cur_ti,tenant_id=1,transaction=False,session=None):
        execution_date = cur_ti.execution_date.replace(tzinfo=timezone.beijing)
        instance = WorkflowInstance.find_workflow_instance(workflow_id=workflow_id, execution_date=execution_date,tenant_id=tenant_id)
        has_waiting = False
        has_running = False
        has_failed = False
        has_success = False
        total_duration = 0
        if instance:
            cur_ti_duration = cur_ti.get_duration()
            ti_info_list = WorkflowStateSync.get_workflow_all_ti_info(workflow_id=workflow_id,
                                                                      execution_date=execution_date,
                                                                      filter_task=cur_ti.dag_id,
                                                                      tenant_id=tenant_id,
                                                                      session=session)
            ti_info_list.append({'state': cur_ti.state, 'duration': cur_ti_duration})

            for ti_info in ti_info_list:
                if ti_info['state'] == State.WAITING:
                    has_waiting = True
                if ti_info['state'] in [State.FAILED,State.TASK_STOP_SCHEDULER]:
                    has_failed = True
                if ti_info['state'] in [State.RUNNING,State.TASK_UP_FOR_RETRY,State.QUEUED,State.SCHEDULED]:
                    has_running = True
                if ti_info['state'] in [State.SUCCESS]:
                    has_success = True

                total_duration = total_duration + ti_info['duration']

            if has_running:
                # 有running的 ,工作流也是running
                instance.state = State.WORKFLOW_RUNNING
            elif has_waiting and (has_success or has_failed):
                # 有waiting状态, 但同时存在失败或者成功的,也认为是running
                instance.state = State.RUNNING
            elif has_waiting :
                # 有waiting状态,也没有成功失败运行的,则是检查状态
                instance.state = State.WORKFLOW_WAITING
            elif has_failed:
                instance.state = State.WORKFLOW_FAILED
                instance.end_date = make_naive(timezone.utcnow())
            else:
                instance.state = State.WORKFLOW_SUCCESS
                instance.end_date = make_naive(timezone.utcnow())

            instance.duration = total_duration
            session.merge(instance)
            if not transaction:
                session.commit()

    @staticmethod
    @provide_session
    def get_workflow_all_ti_info(workflow_id,execution_date,tenant_id=1,filter_task=None,session=None):
        workflow = Workflow.get_workflow(workflow_id=workflow_id,tenant_id=tenant_id,session=session)
        tasks = workflow.get_tasks()
        result = []
        for task in tasks:
            if filter_task and task.task_name == filter_task:
                continue
            tri = Trigger.find_by_execution_date(task.task_name, execution_date,session=session)
            if tri.state in [State.WAITING,State.TASK_STOP_SCHEDULER]:
                result.append({'state':tri.state,'duration':0})
                continue
            tis = TaskInstance.find_by_execution_date(dag_id=task.task_name,execution_date=execution_date,session=session)
            if tis:
                if len(tis) > 1: # fk sharestore
                    state = TaskService.state_merge(tis)
                    duration = tis[0].get_duration() + tis[1].get_duration()
                else :
                    state = tis[0].state
                    duration = tis[0].get_duration()
                result.append({'state':state,'duration':duration})
        return result