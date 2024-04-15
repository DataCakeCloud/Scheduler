# -*- coding: utf-8 -*-
import copy
import json
import time
import traceback

import six
import pendulum
from datetime import datetime, timedelta

from croniter import croniter

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


class WorkflowChartService(object):
    @staticmethod
    def get_lineagechart(workflow_id,execution_date,tenant_id=1):
        '''
            获取workflow的血缘图
        '''
        workflow_instance = WorkflowInstance.find_workflow_instance(workflow_id=workflow_id, execution_date=execution_date,tenant_id=tenant_id)
        if not workflow_instance:
            return {}
        version = workflow_instance.version
        workflow_task_relation = WorkflowTaskRelation.get_workflow_task_relation_info(workflow_id, version,tenant_id=tenant_id)
        if not workflow_task_relation:
            return {}
        gc = GreeterClient()
        relations = workflow_task_relation.get_relation()
        task_list = workflow_task_relation.get_tasks()
        workflow_inside_task_set = set()
        workflow_outside_task_set = set()
        is_delete_task_set = set()

        for relation in relations:
            workflow_inside_task_set.add(int(relation['target']))
            if relation['sourceIsOutside'] == True:
                workflow_outside_task_set.add(int(relation['source']))

        for task in task_list:
            workflow_inside_task_set.add(int(task.ds_task_id))

        node_list = []
        inside_ds_task_list = map(lambda x:{'taskId':x},list(workflow_inside_task_set))
        if len(inside_ds_task_list) > 0:
            inside_ds_task_info = gc.getTaskInfoByID(inside_ds_task_list,tenant_id).info
            for info in inside_ds_task_info:
                if info.isDelete :
                    is_delete_task_set.add(info.id)
                    continue
                node_info = {
                    'nodeId': str(info.id),
                    'taskId': info.id,
                    'taskName': info.name,
                    'isOutside': False,
                    'isDelete': False,
                    'templateCode': info.templateCode
                }
                if execution_date:
                    state = TaskService.get_task_state(info.name,execution_date)
                    node_info['state'] = State.FAILED if state in [State.TASK_STOP_SCHEDULER,State.TASK_UP_FOR_RETRY] else state
                node_list.append(node_info)

        outside_ds_task_list = map(lambda x:{'taskId':x},list(workflow_outside_task_set))
        if len(outside_ds_task_list) > 0:
            outside_ds_task_info = gc.getTaskInfoByID(outside_ds_task_list,tenant_id).info
            for info in outside_ds_task_info:
                if info.isDelete:
                    is_delete_task_set.add(info.id)
                    continue
                # workflow_outside_task_set.remove(info.id)
                node_info = {
                    'nodeId': str(info.id),
                    'taskId': info.id,
                    'taskName': info.name,
                    'isOutside': True,
                    'isDelete': False,
                    'templateCode': info.templateCode
                }
                node_list.append(node_info)
        # 过滤掉已经删除的任务
        relations = filter(lambda x: int(x['source']) not in is_delete_task_set, relations)
        # '''
        #     如果已经删除的任务是无法通过grpc_client.getTaskInfoByID拿到的,但前端仍然要渲染,所以补充一下这部分的
        # '''
        # for delete_ds_task_id in workflow_outside_task_set:
        #     node_list.append(
        #         {
        #             'nodeId': str(delete_ds_task_id),
        #             'taskId': delete_ds_task_id,
        #             'isOutside': True,
        #             'isDelete': True
        #         }
        #     )
        data = {
            'nodeList': node_list,
            'relation': relations
        }
        return data

    @staticmethod
    def get_linechart(workflow_id, version, start_date, end_date,tenant_id=1):
        '''
            获取workflow的折线图
        '''
        workflow_relation_info = WorkflowTaskRelation.get_workflow_task_relation_info(workflow_id=workflow_id,
                                                                                      version=version,
                                                                                      tenant_id=tenant_id)
        if not workflow_relation_info:
            return {'no_data': True}
        task_list = workflow_relation_info.get_tasks()
        if not start_date and not end_date:
            workflow_instances = WorkflowInstance.find_workflow_instances_by_date_range(workflow_id=workflow_id,
                                                                                        limit=15, tenant_id=tenant_id)
        elif not start_date or not end_date:
            workflow_instances = WorkflowInstance.find_workflow_instances_by_date_range(workflow_id=workflow_id,
                                                                                        start_date=start_date,
                                                                                        end_date=end_date,
                                                                                        limit=15, tenant_id=tenant_id)
        else:
            workflow_instances = WorkflowInstance.find_workflow_instances_by_date_range(workflow_id=workflow_id,
                                                                                        start_date=start_date,
                                                                                        end_date=end_date,
                                                                                        tenant_id=tenant_id)
        if len(workflow_instances) == 0:
            return {
                'no_data': True
            }
        task_name_list = []
        execution_dates = []
        series = []
        durations_sum = 0

        date_duration_dict = {}
        for w_instance in workflow_instances:
            execution_dates.append(w_instance.execution_date.replace(tzinfo=timezone.beijing))
            date_duration_dict[DateUtil.get_timestamp(w_instance.execution_date)] = 0

        if len(execution_dates) > 0:
            for task in task_list:
                task_name_list.append(task.ds_task_name)
                ti_list = TaskInstance.find_by_execution_dates(dag_id=task.task_name, task_id=task.task_name,
                                                               execution_dates=execution_dates)
                task_date_duration_dict = copy.copy(date_duration_dict)

                for ti in ti_list:
                    if ti.start_date is None:
                        duration = 0
                    else:
                        ti_end_date = ti.end_date if ti.end_date is not None else utcnow()
                        duration = (ti_end_date.replace(tzinfo=beijing) - ti.start_date.replace(
                            tzinfo=beijing)).total_seconds()
                        durations_sum = duration + durations_sum
                        duration = int(float(duration)/60.0 * 10) / 10.0
                    task_date_duration_dict[DateUtil.get_timestamp(ti.execution_date)] = duration

                # 按照execution_date排序一下,最后再format只取value值
                ti_duration_list = map(lambda x: x[1], sorted(task_date_duration_dict.items(), key=lambda x: x[0]))
                series.append({
                    'taskName': task.ds_task_name,
                    'durations': ti_duration_list
                })
        data = {
            'series': series,
            'executionDates': sorted(map(lambda x:DateUtil.get_timestamp(x),execution_dates)),
            'taskNameList': task_name_list,
            'no_data': True if durations_sum == 0 else False,
        }
        return data

    @staticmethod
    def draw(ds_task_list,tenant_id=1):
        '''
            绘制一批task的血缘图
        '''
        grpc_client = GreeterClient()
        relation = []
        node_list = []
        inside_ds_task_id_set = set()
        outside_ds_task_id_set = set()
        log.info('')
        inside_ds_task_info = grpc_client.getTaskInfoByID(ds_task_list,tenant_id).info
        for ds_task in ds_task_list:
            inside_ds_task_id_set.add(int(ds_task['taskId']))

        for info in inside_ds_task_info:
            # 已经被删除的任务不展示
            if info.isDelete == True:
                continue
            event_depends = info.eventDepends
            node_list.append(
                {
                    'nodeId': str(info.id),
                    'taskId': info.id,
                    'taskName': info.name,
                    'templateCode': info.templateCode,
                    'isOutside': False,
                    'isDelete': False
                }
            )
            for depend in event_depends:
                if int(depend.taskId) in inside_ds_task_id_set:
                    relation.append({'source': str(depend.taskId), 'target': str(info.id),'sourceIsOutside':False})
                else:
                    outside_ds_task_id_set.add(int(depend.taskId))
                    relation.append({'source': str(depend.taskId), 'target': str(info.id), 'sourceIsOutside': True})

        outside_ds_task_list = map(lambda x:{'taskId':x},list(outside_ds_task_id_set))
        if len(outside_ds_task_list) > 0:
            outside_ds_task_info = grpc_client.getTaskInfoByID(outside_ds_task_list,tenant_id).info
            for info in outside_ds_task_info:
                # 已经被删除的任务不展示
                if info.isDelete == True:
                    continue
                # outside_ds_task_id_set.remove(info.id)
                node_list.append(
                    {
                        'nodeId': str(info.id),
                        'taskId': info.id,
                        'taskName': info.name,
                        'templateCode': info.templateCode,
                        'isOutside': True,
                        'isDelete': False
                    }
                )

        # '''
        #     如果已经删除的任务是无法通过grpc_client.getTaskInfoByID拿到的,但前端仍然要渲染,所以补充一下这部分的
        # '''
        # for delete_ds_task_id in outside_ds_task_id_set:
        #     node_list.append(
        #         {
        #             'nodeId': str(delete_ds_task_id),
        #             'taskId': delete_ds_task_id,
        #             'isOutside': True,
        #             'isDelete': True
        #         }
        #     )

        data = {
            'nodeList': node_list,
            'relation': relation
        }
        return data

    @staticmethod
    def get_gridchart(workflow_id, version, start_date, end_date,tenant_id=1):
        '''
            获取workflow的网格图
            本年度第二复杂函数,holyshit!!! 堪比依赖日期计算模块(可参观DateCalculationUtil)
            Returns:
                返回的结果有 5个模块histogramStart,tree,timeColumn,nodeList,timeMedian
                histogramStart:每个网格分块的开始日期
                tree: 工作流内任务的树形依赖结构(有序的)
                nodeList: 一个二维数组, 和网格图的形状一样! 顺序和tree的顺序保持一致,非常重要
                timeColumn: 运行时长的柱状图
                timeMedian: 运行时长的中位数

        '''
        workflow_task_relation = WorkflowTaskRelation.get_workflow_task_relation_info(workflow_id, version,tenant_id=tenant_id)
        if not workflow_task_relation:
            return {}
        wf = Workflow.get_workflow(workflow_id=workflow_id,tenant_id=tenant_id)
        granularity = wf.granularity
        task_list = workflow_task_relation.get_tasks()
        task_dict = {}  # k: ds_task_id,v: task_desc
        for task in task_list:
            task_dict[str(task.ds_task_id)] = task
        relations = workflow_task_relation.get_relation()  # 工作流内任务的依赖关系
        # root_task_ids = workflow_task_relation.root_task_ids  # 所有root节点的task_id
        node_cache_set = set()  # 用来记录哪些node已经被遍历过了,防止重复遍历
        '''
        先确定tree,也就是node的所有位置
        思路是先拿到第一个root节点,遍历它的得下游,每次遍历完一个就记录在dict中,如果dict中已存在则对当前遍历的节点做序号追加,并且不再往下追溯
        其他节点道理相同
        '''
        source_target_dict = {}  # k:是source, v是target,用来记录每个task的下游
        node_id_list = []  # 生成的所有的node_id,这里用list主要是保证它是有序的
        # 遍历relations, 存入一个dict中,其中key是source,value是target(arr)
        for relation in relations:
            # source如果是外部数据集则跳过
            if relation['sourceIsOutside']:
                continue
            source = relation['source']
            target = relation['target']
            if source in source_target_dict.keys():
                source_target_dict[source].append(target)
            else:
                source_target_dict[source] = [target]

        root_ds_task_id_list = workflow_task_relation.get_root_task_ids()

        tree = WorkflowChartService.format_tree_child_node(node_cache_set=node_cache_set, id_list=root_ds_task_id_list,
                                                           father_id=None, source_target_dict=source_target_dict,
                                                           is_root=True, node_id_list=node_id_list, task_dict=task_dict)

        '''
        取实例的数量,默认取15个,也就是没有传前后截止日期的情况下
        '''
        if not start_date and not end_date:
            workflow_instance_list = WorkflowInstance.find_workflow_instances_by_date_range(workflow_id=workflow_id,
                                                                                            limit=15,
                                                                                            tenant_id=tenant_id)
        elif not start_date or not end_date:
            workflow_instance_list = WorkflowInstance.find_workflow_instances_by_date_range(workflow_id=workflow_id,
                                                                                            start_date=start_date,
                                                                                            end_date=end_date,
                                                                                            limit=15,
                                                                                            tenant_id=tenant_id)
        else:
            workflow_instance_list = WorkflowInstance.find_workflow_instances_by_date_range(workflow_id=workflow_id,
                                                                                            start_date=start_date,
                                                                                            end_date=end_date,
                                                                                            tenant_id=tenant_id)
        if len(workflow_instance_list) == 0:
            return {}
        node_list = []  # 最终的node_list
        ds_task_id_node_info_dict = {}  # k: ds_task_id v:node_info2
        execution_dates = []  # 准备存放所有的实例日期
        date_node_dict = {}  # 准备存放每个日期对应的实例信息,为什么这么做? workflow的实例存在不代表任务的实例存在,如果不存在要去填充空信息
        time_column = []  # 工作流实例运行时长
        '遍历获取到所有的实例日期'
        for w_instance in workflow_instance_list:
            execution_dates.append(w_instance.execution_date.replace(tzinfo=timezone.beijing))
            date_node_dict[DateUtil.get_timestamp(w_instance.execution_date)] = None
            # 注意这里是按照时间倒序插进去的,因为查出来的实例是倒序的
            time_column.append(w_instance.duration)
        # 反转一下
        time_column.reverse()
        '''
        这里,主要是取所有ds_task_id对应的node_info信息!方便最后装载入最终的结构
        '''
        WorkflowChartService.set_grid_gantt_X_axis_data(task_dict=task_dict, date_node_dict=date_node_dict,
                                                        execution_dates=execution_dates,
                                                        ds_task_id_node_info_dict=ds_task_id_node_info_dict,
                                                        granularity=granularity)

        '''
        到这里,我们已经生成了tree,拿到了有序的node_id_list. 以及生成了每个node对应的node_info信息! 现在只要把这些内容全部装载入最终的node_list结构就好了
        '''
        for node_id in node_id_list:
            node_ds_task_id = str(node_id).split('_')[0]
            cur_node_info_list = ds_task_id_node_info_dict[node_ds_task_id]
            # 按5个一格切分成块
            block_list = NormalUtil.get_split_block_list(cur_node_info_list, 5)
            node_list.append(block_list)

        '''
        生成histogram_start, 比较简单
        '''
        execution_dates = sorted(execution_dates)
        histogram_start = []
        i = 0
        for execution_date in execution_dates:
            if i % 5 == 0:
                histogram_start.append(DateUtil.get_timestamp(execution_date))
            i += 1

        data = {
            'histogramStart': histogram_start,
            'tree': tree,
            'timeColumn': NormalUtil.get_split_block_list(time_column, 5),
            'nodeList': node_list,
            'timeMedian': NormalUtil.get_list_mid(time_column)
        }
        return data

    @staticmethod
    def get_ganttchart(workflow_id, execution_date,tenant_id=1):
        '''
            获取workflow的甘特图,和网格图(grid chart)结构相似 ,所以代码逻辑与网格图的基本一致

        '''
        workflow_instance = WorkflowInstance.find_workflow_instance(workflow_id=workflow_id,execution_date=execution_date,tenant_id=tenant_id)
        version = workflow_instance.version
        workflow_task_relation = WorkflowTaskRelation.get_workflow_task_relation_info(workflow_id, version,tenant_id=tenant_id)
        if not workflow_task_relation:
            return {}
        if not workflow_instance:
            return {}
        wf = Workflow.get_workflow(workflow_id=workflow_id,tenant_id=tenant_id)
        granularity = wf.granularity
        task_list = workflow_task_relation.get_tasks()
        task_dict = {}  # k: ds_task_id,v: task_desc
        for task in task_list:
            task_dict[str(task.ds_task_id)] = task
        relations = workflow_task_relation.get_relation()  # 工作流内任务的依赖关系
        # root_task_ids = workflow_task_relation.root_task_ids  # 所有root节点的task_id
        node_cache_set = set()  # 用来记录哪些node已经被遍历过了,防止重复遍历
        '''
        先确定tree,也就是node的所有位置
        思路是先拿到第一个root节点,遍历它的得下游,每次遍历完一个就记录在dict中,如果dict中已存在则对当前遍历的节点做序号追加,并且不再往下追溯
        其他节点道理相同
        '''
        source_target_dict = {}  # k:是source, v是target,用来记录每个task的下游
        node_id_list = []  # 生成的所有的node_id,这里用list主要是保证它是有序的
        # 遍历relations, 存入一个dict中,其中key是source,value是target(arr)
        for relation in relations:
            source = relation['source']
            target = relation['target']
            if source in source_target_dict.keys():
                source_target_dict[source].append(target)
            else:
                source_target_dict[source] = [target]

        root_ds_task_id_list = workflow_task_relation.get_root_task_ids()

        tree = WorkflowChartService.format_tree_child_node(node_cache_set=node_cache_set, id_list=root_ds_task_id_list,
                                                           father_id=None, source_target_dict=source_target_dict,
                                                           is_root=True, node_id_list=node_id_list, task_dict=task_dict)

        '''
           从所有的任务实例中拿到开始日期,结束日期
        '''
        # workflow_instance = WorkflowInstance.find_workflow_instance(workflow_id=workflow_id,
        #                                                             execution_date=execution_date)
        node_list = []  # 最终的node_list
        ds_task_id_node_info_dict = {}  # k: ds_task_id v:node_info2
        date_node_dict = {DateUtil.get_timestamp(
            execution_date): None}  # 准备存放每个日期对应的实例信息,为什么这么做? workflow的实例存在不代表任务的实例存在,如果不存在要去填充空信息

        '''
        这里,主要是取所有ds_task_id对应的node_info信息!方便最后装载入最终的结构
        '''
        WorkflowChartService.set_grid_gantt_X_axis_data(task_dict=task_dict, date_node_dict=date_node_dict,
                                                        execution_dates=[execution_date],
                                                        ds_task_id_node_info_dict=ds_task_id_node_info_dict,
                                                        granularity=granularity)
        '''
        到这里,我们已经生成了tree,拿到了有序的node_id_list. 以及生成了每个node对应的node_info信息! 现在只要把这些内容全部装载入最终的node_list结构就好了
        '''
        for node_id in node_id_list:
            node_ds_task_id = str(node_id).split('_')[0]
            cur_node_info_list = ds_task_id_node_info_dict[node_ds_task_id]
            node_list.append(cur_node_info_list)

        '''
        计算开始结束日期的差值
        '''
        duration = DateUtil.get_cost_time_by_date(workflow_instance.start_date,workflow_instance.end_date)
        internal = DateUtil.get_gantt_internal(duration)

        data = {
            'nodeList': node_list,
            'internal': internal,
            'startDate': DateUtil.get_timestamp(workflow_instance.start_date) if workflow_instance.start_date else DateUtil.get_timestamp(utcnow()),
            'endDate': DateUtil.get_timestamp(workflow_instance.end_date) if workflow_instance.end_date else DateUtil.get_timestamp(utcnow()),
            'tree': tree,
            'duration': duration
        }
        return data

    @staticmethod
    def set_grid_gantt_X_axis_data(task_dict, date_node_dict, execution_dates, ds_task_id_node_info_dict,granularity):
        '''
            获取网格图的X轴数据
            Args:
                task_dict: key是ds_task_id, value是task_desc
                date_node_dict: key是所有的execution_date(时间戳), value是空
                execution_dates: 所有的execution_date(date)
                ds_task_id_node_info_dict: # key: ds_task_id value:node_info. 我们取到所有的node_info后会存入这个dict中
        '''

        def format_node_info(task_desc, execution_date, duration=0, start_date=None, end_date=None, genie_job_id=None,
                             genie_job_url=None, state=None, try_number=0):
            '''
            用来为那些没有实例的任务生成node_info结构
            '''
            return {
                'taskName': task_desc.ds_task_name, 'duration': duration, 'startDate': start_date, 'endDate': end_date,
                'genieJobId': genie_job_id, 'genieJobUrl': genie_job_url,
                'owner': str(task_desc.owner).split(',')[0], 'taskID': task_desc.ds_task_id, 'state': state,
                'version': task_desc.task_version,
                'executionDate': execution_date,
                'tryNumber': try_number,
                'granularity': granularity
            }

        for ds_task_id, task_desc in task_dict.items():
            tri_lsit = Trigger.find_by_execution_dates(dag_id=task_desc.task_name, execution_dates=execution_dates)
            ti_list = TaskInstance.find_by_execution_dates(dag_id=task_desc.task_name, execution_dates=execution_dates)
            date_ti_dict = {}  # key是execution_date(时间戳),value是这个日期对应的taskinstance(list),由于sharestore有两个子任务,所以这里是数组 fuck it,fuck sharestore
            cur_date_node_dict = copy.copy(date_node_dict)  # 这里主要是为了能够复用这个结构, 所以copy一下, 不改变原对象
            # 填充key date_ti_dict:
            for e_date in execution_dates:
                date_ti_dict[DateUtil.get_timestamp(e_date)] = []

            for ti in ti_list:
                exe_timestamp = DateUtil.get_timestamp(ti.execution_date)
                # sharestore任务有两个子任务,fuck it
                date_ti_dict[exe_timestamp].append(ti)

            for tri in tri_lsit:
                exe_timestamp = DateUtil.get_timestamp(tri.execution_date)
                if tri.state in [State.TASK_STOP_SCHEDULER, State.TASK_WAIT_UPSTREAM]:
                    cur_date_node_dict[DateUtil.get_timestamp(tri.execution_date)] = \
                        format_node_info(task_desc=task_desc, execution_date=exe_timestamp,
                                         state=State.FAILED if tri.state == State.TASK_STOP_SCHEDULER else tri.state)
                else:
                    cur_tis = date_ti_dict[exe_timestamp]
                    # 状态合并, sharestore有两个子任务, fuck it!!!!!!!!!!!!!!!!!!!!!
                    state = State.taskinstance_state_mapping(TaskService.state_merge(cur_tis))
                    duration = 0
                    genie_job_url = None
                    genie_job_id = None
                    start_date = None
                    end_date = None
                    try_number = 0
                    for ti in cur_tis:
                        if ti.dag_id != ti.task_id:
                            continue
                        duration = ti.get_duration()
                        genie_info = ti.get_genie_info()
                        genie_job_url = genie_info[1]
                        genie_job_id = genie_info[0]
                        start_date = DateUtil.get_timestamp(ti.start_date) if ti.start_date else None
                        end_date = DateUtil.get_timestamp(ti.end_date) if ti.end_date else None
                        try_number = ti.try_number - 1 if ti.try_number > 0 else 0
                    cur_date_node_dict[DateUtil.get_timestamp(tri.execution_date)] = \
                        format_node_info(task_desc=task_desc, execution_date=exe_timestamp, state=state,
                                         duration=duration, genie_job_id=genie_job_id,
                                         try_number=try_number, start_date=start_date, end_date=end_date,
                                         genie_job_url=genie_job_url)

            '''
                说下这里是在干啥,首先我们拿到了当前task,每个execution_date对应的node_info (数据结构cur_date_node_dict)
                说说我们的目标是需要一个什么样的结构?我们需要一个按照execution_date 正序排序的node_info数组
                所以第一步,拿到此结构的items(是一个key value组成的元组), 然后对key(execution_date)做排序, 拿到一个有序的元组数组
                接着只要重新格式化一下取value(node_info)就可以了, 但是这里有一个问题! node_info可能是空的,原因是workflow实例存在,不代表task的实例一定存在
                所以在格式化时需要为空的node_info给一个默认格式,这样处理之后就拿到我们想要的结果了
            '''
            cur_node_info_list = map(
                lambda x: format_node_info(task_desc=task_desc, execution_date=x[0]) if x[1] is None else x[1],
                sorted(cur_date_node_dict.items(), key=lambda x: x[0]))

            ds_task_id_node_info_dict[str(ds_task_id)] = cur_node_info_list

    @staticmethod
    def format_tree_child_node(node_cache_set, id_list, father_id, source_target_dict, is_root, node_id_list,
                               task_dict):
        '''
            当一个节点只被遍历过一次的时候,他的nodeID = id(ds_task_id), 当一个节点被遍历过多次的时候,除去第一次,后续的nodeID = id + fatherid(ds_task_id)
        '''
        treeNode = []
        if len(id_list) == 0:
            return treeNode
        for id in id_list:
            if id in node_cache_set:
                if is_root:
                    # 这个基本不肯能,除非数据错了
                    continue
                else:
                    node_id = '{}_{}'.format(str(id), str(father_id))
                    node_id_list.append(node_id)
                    treeNode.append({
                        'nodeID': node_id,
                        'taskName': task_dict[id].ds_task_name,
                        'children': []
                    })
            else:
                node_cache_set.add(id)
                node_id_list.append(str(id))
                if id in source_target_dict.keys():
                    child_id_list = source_target_dict[id]
                else:
                    child_id_list = []
                children = WorkflowChartService.format_tree_child_node(node_cache_set=node_cache_set,
                                                                       id_list=sorted(child_id_list),
                                                                       father_id=id,
                                                                       source_target_dict=source_target_dict,
                                                                       is_root=False, node_id_list=node_id_list,
                                                                       task_dict=task_dict)
                treeNode.append({
                    'nodeID': str(id),
                    'taskName': task_dict[id].ds_task_name,
                    'children': children
                })

        return treeNode
