# -*- coding: utf-8 -*-
from datetime import timedelta

from airflow.models import TaskInstance, DagModel
from airflow.shareit.models.task_desc import TaskDesc
from airflow.shareit.models.trigger import Trigger
from airflow.shareit.utils.date_util import DateUtil
from airflow.shareit.utils.state import State
from airflow.utils.timezone import beijing


class CheckNode(object):
    def __init__(self,
                 task_name=None,
                 ds_task_id=None,
                 ds_task_name=None,
                 is_task=True,
                 metadata_id=None,
                 execution_date=None,
                 gra=None,
                 owner=None,
                 tri=None,
                 tis=[],
                 ready_time=0,
                 check_path=None,
                 downstream_task_name=None,
                 downstream_task_execution_date=None,
                 upstream_task_name=None,
                 upstream_task_execution_date=None,
                 is_active=True,
                 is_success=False,
                 ):
        self.task_name = task_name
        self.ds_task_id = ds_task_id
        self.ds_task_name = ds_task_name
        self.tri = tri
        self.tis = tis
        self.ready_time = ready_time
        self.metadata_id = metadata_id
        self.execution_date = execution_date
        self.is_task = is_task
        self.check_path = check_path
        self.downstream_task_name = downstream_task_name
        self.downstream_task_execution_date = downstream_task_execution_date
        self.upstream_task_name = upstream_task_name
        self.upstream_task_execution_date = upstream_task_execution_date
        self.gra = gra
        self.is_active = is_active
        self.owner = owner
        self.is_success = is_success

    def get_task_name(self):
        if self.is_task:
            return self.task_name
        else:
            return self.metadata_id

    def get_ds_task_name(self):
        if self.is_task:
            return self.ds_task_name
        else:
            return self.metadata_id

    def get_ds_task_id(self):
        if self.is_task:
            return self.ds_task_id
        else:
            return 0

    def get_node_id(self):
        if self.is_task:
            return '{},{}'.format(self.task_name, self.execution_date.strftime('%Y-%m-%d %H:%M:%S'))
        else:
            return '{},{}'.format(self.metadata_id, self.execution_date.strftime('%Y-%m-%d %H:%M:%S'))

    def get_down_node_id(self):
        if not self.downstream_task_name:
            return ''
        return '{},{}'.format(self.downstream_task_name,
                              self.downstream_task_execution_date.strftime('%Y-%m-%d %H:%M:%S'))

    def get_up_node_id(self):
        if not self.upstream_task_name:
            return ''
        return '{},{}'.format(self.upstream_task_name,
                              self.upstream_task_execution_date.strftime('%Y-%m-%d %H:%M:%S'))

    def get_ti_detail(self):
        detail = {}
        if self.tri and self.tri.state in ['waiting', 'termination']:
            detail['state'] = self.tri.state
            detail['version'] = -1
            detail['end_date'] = ''
            detail['start_date'] = ''
            detail['genie_job_id'] = ''
            detail['genie_job_url'] = ''
            return detail

        if self.tis:
            detail['state'] = State.taskinstance_state_mapping(TaskInstance.state_merge(self.tis))
            for ti in self.tis:
                if ti.dag_id != ti.task_id:
                    continue
                detail['start_date'] = ti.start_date.strftime('%Y-%m-%d %H:%M:%S') if ti.start_date else ''
                detail['end_date'] = ti.end_date.strftime('%Y-%m-%d %H:%M:%S') if ti.end_date else ''
                genie_info = ti.get_genie_info()
                detail['genie_job_url'] = genie_info[1]
                detail['genie_job_id'] = genie_info[0]
                detail['version'] = ti.get_version()
            return detail
        detail['state'] = State.TASK_WAIT_UPSTREAM if not self.is_success else State.SUCCESS
        detail['version'] = -1
        detail['end_date'] = ''
        detail['start_date'] = ''
        detail['genie_job_id'] = ''
        detail['genie_job_url'] = ''
        return detail

    def set_recursion_cnt(self, num):
        self.recursion_cnt = num


    def node_init_if_need(self,include_ti_info=True):
        if not self.is_task:
            return
        td = TaskDesc.get_task(task_name=self.task_name)
        self.owner = td.owner
        self.ds_task_id = td.ds_task_id
        self.ds_task_name = td.ds_task_name
        if not DagModel.is_active_and_on(dag_id=self.task_name):
            self.is_active = False
        self.execution_date = self.execution_date.replace(tzinfo=beijing)
        if include_ti_info:
            state = ''
            tri = Trigger.find(dag_id=self.task_name, execution_date=self.execution_date)
            if tri and (tri[0].state in [State.TASK_WAIT_UPSTREAM,State.TASK_STOP_SCHEDULER]):
                self.tri = tri[0]
                state = tri[0].state

            tis = TaskInstance.find_by_execution_date(dag_id=self.task_name, execution_date=self.execution_date)
            if tis:
                self.tis = tis
                for ti in tis:
                    if ti.dag_id != ti.task_id:
                        continue
                    if not state and ti.state == State.SUCCESS:
                        self.is_success = True
                        self.ready_time = DateUtil.get_timestamp(ti.end_date if ti.end_date else ti.update_date + timedelta(hours=8))



    def __repr__(self):
        return str(self.__dict__)

    def __str__(self):
        return str(self.__dict__)
