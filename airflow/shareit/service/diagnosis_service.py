# -*- coding: utf-8 -*-
"""
Desc: 任务调度情况诊断
CreateDate: 2021/10/15
"""
import json
import pendulum

from datetime import datetime, timedelta

from airflow.shareit.check.check_dependency_helper import CheckDependecyHelper
from airflow.shareit.models.dataset_partation_status import DatasetPartitionStatus
from airflow.shareit.models.event_depends_info import EventDependsInfo
from airflow.shareit.models.sensor_info import SensorInfo
from airflow.shareit.utils import task_manager
from airflow.shareit.utils.date_util import DateUtil
from airflow.shareit.grpc.client.greeter_client import GreeterClient
from airflow.shareit.utils.constant import Constant
from airflow.shareit.utils.config_util import get_genie_domain
from airflow.shareit.utils.normal_util import NormalUtil
from airflow.shareit.utils.task_cron_util import TaskCronUtil
from airflow.shareit.utils.task_util import TaskUtil
from airflow.utils import timezone
from airflow.utils.timezone import beijing, utcnow
from airflow.utils.log.logging_mixin import LoggingMixin

from airflow.shareit.utils.granularity import Granularity
from airflow.shareit.utils.state import State
from airflow.shareit.models.dag_dataset_relation import DagDatasetRelation
from airflow.shareit.models.trigger import Trigger
from airflow.shareit.models.task_desc import TaskDesc
from airflow.shareit.service.task_service import TaskService
from airflow.models.dag import DagModel
from airflow.models.taskinstance import TaskInstance
from airflow.shareit.check.check_node import CheckNode


class DiagnosisService(LoggingMixin):
    def __init__(self):
        super(DiagnosisService, self).__init__()
        self.depth = 0
        self.lineage = 0
        self.relations = list()
        self.root_link = ''
        self.genie_domain = get_genie_domain()

    def diagnose_success(self,ready_upstreams,result):
        ready_upstreams = self._sorted_node_list(ready_upstreams)
        for upstream in ready_upstreams:
            self.add_node_instance(result,upstream)
            result['relation'].append({'source': upstream.get_node_id(), 'target': upstream.get_down_node_id()})


    @staticmethod
    def is_same_relation(relation, dag_id):
        for item in relation:
            if item.get('source') == dag_id:
                return True
        return False

    @staticmethod
    def is_same_instance(instance, node):
        for item in instance:
            if item.get('dag_id') == node.dag_id and item.get('down_dag_id') == node.down_dag_id:
                return True
        return False

    @staticmethod
    def get_date_state_genie(dag_id, execution_date):
        # 获取节点的开始、结束时间、状态、version、genie_job_id、genie_job_url
        info = dict()
        state = ''
        trigger = Trigger.find(dag_id=dag_id, execution_date=execution_date)
        if trigger and (trigger[0].state == 'waiting' or trigger[0].state == 'termination'):
            state = trigger[0].state

        task_instance_list = TaskInstance.find_by_execution_dates(dag_id=dag_id, task_id=dag_id, execution_dates=[execution_date.replace(tzinfo=timezone.beijing)])
        if task_instance_list:
            if not state:
                state = State.taskinstance_state_mapping(TaskService.state_merge(task_instance_list))
            for ti in task_instance_list:
                if ti.dag_id != ti.task_id:
                    continue
                info['start_date'] = ti.start_date.strftime('%Y-%m-%d %H:%M:%S') if ti.start_date else ''
                info['end_date'] = ti.end_date.strftime('%Y-%m-%d %H:%M:%S') if ti.end_date else ''
                genie_info = ti.get_genie_info()
                info['genie_job_url'] = genie_info[1]
                info['genie_job_id'] = genie_info[0]
                info['state'] = state
                info['version'] = ti.get_version()

        else:
            info['version'] = -1
            info['end_date'] = ''
            info['start_date'] = ''
            info['genie_job_id'] = ''
            info['genie_job_url'] = ''
            info['state'] = State.TASK_WAIT_UPSTREAM if not state else state
        return info

    def package_node_instance(self,node,include_ti_info=True):
        node_instance = {
            'ready': node.is_success,
            'dagId': node.get_ds_task_name(),
            'owner': node.owner,
            'checkPath': node.check_path,
            'isExternal': not node.is_task,
            'metadataId': node.metadata_id,
            'executionDate': node.execution_date.strftime('%Y-%m-%d %H:%M:%S'),
            'nodeId': node.get_node_id(),
            'online': node.is_active,
            'taskName': node.get_task_name(),
            'downTaskName': node.downstream_task_name,
            'downNodeId': node.get_down_node_id(),
            'successDate':  DateUtil.timestamp_str(node.ready_time),
            'upTaskName': node.upstream_task_name,
            'upNodeId': node.get_up_node_id(),
            'dsTaskId':node.get_ds_task_id()
        }
        info = node.get_ti_detail()
        if node.is_task:
            if include_ti_info:
                node_instance['start_date'] = info['start_date']
                node_instance['end_date'] = info['end_date']
                node_instance['state'] = info['state']
                node_instance['version'] = info['version']
                node_instance['genie_job_id'] = info['genie_job_id']
                node_instance['genie_job_url'] = info['genie_job_url']
                node_instance['duration'] = DateUtil.get_cost_time(info['start_date'], info['end_date'])
            else :
                node_instance['start_date'] = 0
                node_instance['end_date'] = 0
                node_instance['state'] = State.WAITING
                node_instance['version'] = ''
                node_instance['genie_job_id'] = ''
                node_instance['genie_job_url'] = ''
                node_instance['duration'] = 0
        else:
            node_instance['state'] = State.TASK_WAIT_UPSTREAM if not node.is_success else State.SUCCESS
        return node_instance

    def add_node_instance(self,result, node):
        result['instance'].append(self.package_node_instance(node))
        return result

    def diagnose_new(self, task_id, execute_date, state, ds_task_name=None):
        result = {'relation': [], 'instance': []}
        core_node_id = '{},{}'.format(task_id, execute_date.strftime('%Y-%m-%d %H:%M:%S'))
        result['coreTaskId'] = core_node_id
        ready = state == State.SUCCESS
        ready_upstreams, unready_upstreams = self._get_upstream_list(task_id, execute_date)
        self.diagnose_success(ready_upstreams,result)
        # cache记录已经存在的未就绪的上游，目的是防止出现环
        cache = set()
        cache.add(task_id)
        cache.update(upstream.task_name for upstream in unready_upstreams)
        for upstream in unready_upstreams:
            ready = False
            self.add_node_instance(result,upstream)

            if not upstream.is_task:
                result['relation'].append({'source': upstream.get_node_id(), 'target': core_node_id})
                continue

            if upstream.task_name == task_id:
                # 防止两个nodeid是一样的，也就是说，规避的是用户配置错了，完全自己依赖自己的死循环这种情况
                if not DateUtil.date_equals(upstream.execution_date,execute_date):
                    result['relation'].append({'source': upstream.get_node_id(), 'target': core_node_id})
                continue

            indirect_ready_ups, indirect_unready_ups = self._get_upstream_list(upstream.task_name, upstream.execution_date)
            indirect_unready_ups = [ups for ups in indirect_unready_ups if ups.task_name not in cache]
            if not indirect_unready_ups:
                result['relation'].append({'source': upstream.get_node_id(), 'target': core_node_id})
            else:
                fault_nodes = self.recursion_fault_node(upstream, indirect_unready_ups,cache=cache)
                # if not fault_nodes:
                #     for item in indirect_unready_ups:
                #         result['relation'].append({'source': item.node_id, 'target': upstream.node_id})
                #         self.add_node_instance(result, item)
                #         continue

                for node, cur_upstream_node in fault_nodes:
                    recursion_cnt = node.recursion_cnt
                    if not node.is_task:
                        if recursion_cnt >= 2:
                            if recursion_cnt >= 3:
                                if DiagnosisService.is_same_relation(result['relation'], node.task_name):
                                    continue
                                result['relation'].append(
                                    {'source': node.get_down_node_id(), 'target': upstream.get_node_id(), 'type': False})
                            else:
                                result['relation'].append(
                                    {'source': node.get_down_node_id(), 'target': upstream.get_node_id()})
                            result['relation'].append({'source': node.get_node_id(), 'target': node.get_down_node_id()})
                            result['relation'].append({'source': upstream.get_node_id(), 'target': core_node_id})
                            self.add_node_instance(result, cur_upstream_node)
                        elif recursion_cnt == 1:
                            result['relation'].append({'source': node.get_node_id(), 'target': node.get_down_node_id()})
                            result['relation'].append({'source': upstream.get_node_id(), 'target': core_node_id})
                    else:
                        if recursion_cnt >= 2:
                            if DiagnosisService.is_same_relation(result['relation'], node.task_name):
                                continue
                            result['relation'].append({'source': node.get_node_id(), 'target': upstream.get_node_id(), 'type': False})
                            result['relation'].append({'source': upstream.get_node_id(), 'target': core_node_id})
                        else:
                            result['relation'].append({'source': node.get_node_id(), 'target': upstream.get_node_id(), 'type': False})
                            result['relation'].append({'source': upstream.get_node_id(), 'target': core_node_id})

                    self.add_node_instance(result, node)

        info = DiagnosisService.get_date_state_genie(task_id, execute_date)
        result['instance'].append({
            'ready': ready,
            'dagId': ds_task_name,
            'owner': DagModel.get_task_owner(task_id),
            'executionDate': execute_date.strftime('%Y-%m-%d %H:%M:%S'),
            'nodeId': core_node_id,
            'start_date': info['start_date'],
            'end_date': info['end_date'],
            'state': info['state'],
            'online': DagModel.is_active_and_on(dag_id=task_id)
        })
        result['relation'] = DiagnosisService.set_list(result['relation'])
        return result


    def recursion_fault_node(self, cur_node, direct_upstreams, num=0,cache=None):
        # type: (CheckNode, List[CheckNode], int) -> List[Tuple[CheckNode, CheckNode]]
        """
        递归获取故障节点
        :param cur_node: 当前节点
        :param direct_upstreams: 当前节点直接上游
        :param num: 递归次数
        :return: 故障节点列表
        """
        cache = set() if not cache else cache
        fault_nodes = list()
        num += 1
        if num > 20:  # 是否可以删除
            return fault_nodes

        cache.update(node.task_name for node in direct_upstreams)
        for node in direct_upstreams:
            # 外部数据不需要继续向上追溯
            if not node.is_task:
                node.set_recursion_cnt(num)
                fault_nodes.append((node, cur_node))
                continue

            # 自依赖不再向上追溯（理论上不会出现了，cache避免了这种情况）
            if node.task_name == cur_node.task_name:
                node.set_recursion_cnt(num)
                fault_nodes.append((node, cur_node))
                continue

            # 获取当前节点未就绪上游
            ready_upstreams,unready_upstreams = self._get_upstream_list(node.task_name, node.execution_date)
            # 如果上游存在于cache中，直接过滤掉
            unready_upstreams = [ups for ups in unready_upstreams if ups.task_name not in cache]
            # 当前节点上游都就绪，代表故障节点即当前节点，停止向上追溯
            if len(unready_upstreams) == 0:
                node.set_recursion_cnt(num)
                fault_nodes.append((node, cur_node))
            else:
                # 递归，继续向上追溯
                fault_nodes.extend(self.recursion_fault_node(node, unready_upstreams, num=num,cache=cache))
        # 递归结束，返回故障节点列表,列表的内容是元组，元组的内容是故障节点和故障节点的上游节点
        return fault_nodes


    @staticmethod
    def dagid_count(task_id):
        # return TaskInstance.count_with_date_range(dag_id=task_id) # 检查上游与停止检查上游，没有生成实例
        # trigger表中获取不到最近一次的状态是检查上游的实例，待补充
        return Trigger.count_with_date_range(dag_id=task_id)

    @staticmethod
    def task_dates(task_id, execution_date):
        result = Trigger.get_instance_dates(task_id, execution_date)
        dates = list()
        for item in result:
            dates.append(item.execution_date.strftime('%Y-%m-%d %H:%M:%S'))
        # if not dates:  # 可以不必须给前端传递一个值
        #     dates.append(str(execution_date.strftime('%Y-%m-%d %H:%M:%S')))
        task_desc = TaskDesc.get_task(task_name=task_id)
        now = TaskCronUtil.ger_prev_execution_date_by_task(task_name=task_id, cur_date=execution_date, task_desc=task_desc)
        if now:
            now = now.strftime('%Y-%m-%d %H:%M:%S')
            if now not in dates:
                dates.insert(0, now)
                if len(dates) > 20:
                    dates.pop()

        return dates

    def data_lineage(self, task_id, execution_date, depth, stream,tenant_id=1,ds_task_id=None):
        """
        depth: 除当前节点外，数据血缘深度默认为3，取值范围为1-10，由前端设置
        return：获取当前节点的数据血缘图
        """
        if stream == 1:  # 上游
            return self.upstream_data_lineage(task_id, execution_date, depth,tenant_id=tenant_id)
        else:  # 2 下游
            return self.downstream_data_lineage(task_id, execution_date, depth,tenant_id=tenant_id,ds_task_id=ds_task_id)

    def upstream_data_lineage(self, task_id, execution_date, depth,tenant_id=1):
        self.depth = depth
        result = dict()
        result['relation'] = list()
        result['instance'] = list()
        core_node_id = '{},{}'.format(task_id, execution_date.strftime('%Y-%m-%d %H:%M:%S'))
        result['coreTaskId'] = core_node_id
        self.lineage = 1
        metadata_id = DagDatasetRelation.get_metadata_id(task_id)
        cur_node = CheckNode(task_name=task_id,
                             execution_date=execution_date,
                             metadata_id=metadata_id,
                             is_task=True,
                             )
        cur_node.node_init_if_need()
        # 根节点不获取下游节点
        node_instance = self.splice_node(cur_node)
        result['instance'].append(node_instance)
        upstreams = self.recursion_upstream(task_id, execution_date)
        for upstream in upstreams:
            result['instance'].append(upstream)
            result['relation'].append({'source': upstream.get('nodeId'), 'target': upstream.get('downNodeId')})

        return result

    @staticmethod
    def is_same_lineage(relation, relations):
        for item in relations:
            if item == relation:
                return True
        return False

    @staticmethod
    def set_list(info):
        # 删除重复的节点与连接关系
        content = [dict(t) for t in set([tuple(d.items()) for d in info])]
        content.sort(key=info.index)
        return content

    def recursion_upstream(self, task_id, execution_date, num=0, link_dates_dict=None, filter_ds_task=None,
                           has_appeared=None):
        '''
        这里有两个地方在调用, 一个是血缘图,一个是链路分析图
        lineage等于1,代表是数据血缘图，等于2代表是链路分析图
        血缘图与链路分析图在展示依赖关系时有所不同:
        1. 血缘图中希望尽可能的展示所有的依赖关系,所以即便是在两个分支上同时依赖一个上游,我们也希望他们都能存在连接关系
           血缘图,当遇到已经出现过节点时,我们保留此节点,但不再向后递归,因为此节点之后的节点,我们已经在之前的递归中获取到了,我们保留此节点的目的,是为了此节点与下游节点的连接关系
        2. 链路分析图主要看的是实例的运行情况,所以在两个分支上同时依赖一个上游,同时展示两个连接关系,没有意义,可读性很差.
           链路分析图,一共只展示四层关系树,root-第一级-第二级-第三极-第四级,在第一级节点,允许展示重复依赖,但是在第二级节点及之后的节点,我们需要对重复的节点去重,使用的是filter_ds_task

        :param task_id:
        :param execution_date:
        :param num:
        :param link_dates_dict: 链路分析图使用, 用于记录每个节点的实例日期
        :param filter_ds_task: 链路分析图使用, 用于记录第二级节点及之后的重复依赖
        :param has_appeared: 血缘图使用, 用于记录已经出现过的节点
        :return:

        '''
        result = list()
        num += 1
        if num > self.depth:
            return result

        if not filter_ds_task:
            filter_ds_task = set()
        if not has_appeared:
            has_appeared = set()

        if self.lineage == 1:
            # 在获取上游的过程中直接拿到实例信息
            ready_upstreams, unready_upstreams = self._get_upstream_list(task_id, execution_date)
            upstreams = ready_upstreams + unready_upstreams
        else:
            # 在获取上游的过程中不包含实例信息,目的是减少数据库的查询
            upstreams = CheckDependecyHelper.get_dependency(task_id, execution_date)

        if self.lineage != 1:
            upstreams = [x for x in upstreams if x.get_ds_task_name() not in filter_ds_task]
            # 在filter之后加入到filter_ds_task中，防止下游有对节点的依赖
            filter_ds_task.update(upstream.get_ds_task_name() for upstream in upstreams)
        upstreams = self._sorted_node_list(upstreams)

        repeat = dict()
        for upstream in upstreams:
            node_instance = self.create_node_instance(upstream, repeat,link_dates_dict)
            if not upstream.is_task:
                # 如果是数据集,直接添加到结果中
                result.append(node_instance)
                continue

            if upstream.task_name == task_id:
                # 如果是自依赖,直接跳过
                continue

            result.append(node_instance)
            if self.lineage == 1 and upstream.get_ds_task_name() + upstream.execution_date.strftime(
                    '%Y-%m-%d %H:%M:%S') in has_appeared:
                has_appeared.add(upstream.get_ds_task_name() + upstream.execution_date.strftime('%Y-%m-%d %H:%M:%S'))
                continue
            has_appeared.add(upstream.get_ds_task_name() + upstream.execution_date.strftime('%Y-%m-%d %H:%M:%S'))

            result.extend(self.recursion_upstream(upstream.task_name,
                                                  upstream.execution_date.replace(tzinfo=beijing),
                                                  num=num, link_dates_dict=link_dates_dict,
                                                  filter_ds_task=filter_ds_task, has_appeared=has_appeared))
        repeat.clear()
        return result

    def recursion_downstream(self, task_id, execution_date, num=0, ds_task_id=None,tenant_id=1 ,filter_ds_task=None,
                           has_appeared=None):
        '''
        只有血缘图需要获取下游
        '''
        result = list()
        num += 1
        if num > self.depth:
            return result
        if not filter_ds_task:
            filter_ds_task = set()
        if not has_appeared:
            has_appeared = set()

        # 在获取上游的过程中直接拿到实例信息
        ready, unready = self._get_downstream_list(task_id, execution_date,ds_task_id,tenant_id)
        downstreams = ready + unready


        downstreams = [x for x in downstreams if x.get_ds_task_name() not in filter_ds_task]
        # 在filter之后加入到filter_ds_task中，防止下游有对节点的依赖
        filter_ds_task.update(downstream.get_ds_task_name() for downstream in downstreams)
        downstreams = self._sorted_node_list(downstreams)

        for downstream in downstreams:
            node_instance = self.create_node_instance(downstream)
            if downstream.task_name == task_id:
                # 如果是自依赖,直接跳过
                continue

            result.append(node_instance)
            if self.lineage == 1 and downstream.get_ds_task_name() + downstream.execution_date.strftime(
                    '%Y-%m-%d %H:%M:%S') in has_appeared:
                # has_appeared.add(upstream.get_ds_task_name() + upstream.execution_date.strftime('%Y-%m-%d %H:%M:%S'))
                continue
            has_appeared.add(downstream.get_ds_task_name() + downstream.execution_date.strftime('%Y-%m-%d %H:%M:%S'))

            result.extend(self.recursion_downstream(downstream.task_name,
                                                  downstream.execution_date.replace(tzinfo=beijing),
                                                  num=num,
                                                  filter_ds_task=filter_ds_task, has_appeared=has_appeared,tenant_id=tenant_id,ds_task_id=node_instance['dsTaskId']))
        return result


    def create_node_instance(self, upstream, repeat=None,link_dates_dict=None):
        # 这里include_ti_info=False，是因为链路分析图需要的ti很多,在之后范围查询db获取,不需要在这里获取
        include_ti_info = self.lineage == 1
        node_instance = self.splice_node(upstream, include_ti_info=include_ti_info)

        if self.lineage != 1:
            if upstream.get_ds_task_name() == self.root_link:  # 去除根节点出现重复
                node_instance['link'] = self.root_link
            else:
                repeat[upstream.get_ds_task_name()] = repeat.get(upstream.get_ds_task_name(), 0) + 1
                node_instance['link'] = upstream.get_ds_task_name() + '_' + str(repeat[upstream.get_ds_task_name()])

            if node_instance['link'] in link_dates_dict.keys():
                link_dates_dict[node_instance['link']].append(
                    {upstream.execution_date.strftime('%Y-%m-%d %H:%M:%S'): node_instance})
            else:
                link_dates_dict[node_instance['link']] = [{upstream.execution_date.strftime('%Y-%m-%d %H:%M:%S'): node_instance}]
        return node_instance


    def splice_node(self,node,include_ti_info=True):
        node_instance = self.package_node_instance(node,include_ti_info=include_ti_info)
        if node.is_task:
            node_instance['task_id'] = node.ds_task_id

        return node_instance

    def downstream_data_lineage(self, task_name, execution_date, depth,tenant_id=1,ds_task_id=None):
        self.lineage = 1
        self.depth = depth
        result = {}
        result['relation'] = list()
        result['instance'] = list()
        core_node_id = '{},{}'.format(task_name, execution_date.strftime('%Y-%m-%d %H:%M:%S'))
        result['coreTaskId'] = core_node_id
        metadata_id = DagDatasetRelation.get_metadata_id(task_name)
        cur_node = CheckNode(task_name=task_name,
                             execution_date=execution_date,
                             metadata_id=metadata_id,
                             is_task=True,
                             )
        cur_node.node_init_if_need()
        # 根节点不获取下游节点
        node_instance = self.splice_node(cur_node)
        result['instance'].append(node_instance)
        downstreams = self.recursion_downstream(task_name,execution_date,ds_task_id=ds_task_id,tenant_id=tenant_id)
        for downstream in downstreams:
            result['instance'].append(downstream)
            result['relation'].append({'source': downstream.get('upNodeId'), 'target': downstream.get('nodeId')})

        return result

        # self._get_downstream_list(task_name,execution_date,ds_task_id=ds_task_id,tenant_id=tenant_id)

        # res = TaskService.instance_relation_shell(core_task=task_name, execution_date=execution_date, level=depth)
        # result = dict()
        # result['relation'] = list()
        # result['instance'] = list()
        # result['coreTaskId'] = DiagnosisService.get_instance_id(res.get('core_task_id'))

        # for relation in res.get('relation'):
        #     result['relation'].append({'source': DiagnosisService.get_instance_id(relation.get('source')),
        #                                'target': DiagnosisService.get_instance_id(relation.get('target'))})
        # for instance in res.get('instance_list'):
        #     td = TaskDesc.get_task(task_name=instance.get('dag_id'))
        #     ds_task_name = td.ds_task_name
        #     owner = td.owner.split(',')[0]
        #     id_spark = DiagnosisService.get_id_spark(ds_task_name,tenant_id=tenant_id)  # 下游没有外部数据
        #     node = {
        #         'dagId': ds_task_name,
        #         'start_date': DateUtil.timestamp_str(instance.get('start_date')),
        #         'end_date': DateUtil.timestamp_str(instance.get('end_date')),
        #         'successDate': DateUtil.timestamp_str(instance.get('end_date')),
        #         'executionDate': DateUtil.timestamp_str(instance.get('execution_date')),
        #         'isExternal': False,
        #         'state': instance.get('state') if instance.get('state') else 'waiting',
        #         'duration': instance.get('duration'),
        #         'nodeId': DiagnosisService.get_instance_id(instance.get('id')),
        #         'owner': owner,
        #         'ready': True if instance.get('state') == State.SUCCESS else False,
        #         'task_id': id_spark[0],
        #         'is_spark_task': id_spark[1],
        #         'template_code': id_spark[2],
        #         'metadataId': DagDatasetRelation.get_metadata_id(instance.get('dag_id'))
        #     }
        #     DiagnosisService.add_genie(instance.get('dag_id'), node, self.genie_domain)
        #     result['instance'].append(node)
        # return result

    @staticmethod
    def get_instance_id(task_id):
        info = task_id.split('_')
        if len(info) == 2:
            task_name = info[0]
        else:
            task_name = '_'.join(info[:len(info)-1])

        date = info[-1].split('T')
        format_date = date[0][: 4] + '-' + date[0][4: 6] + '-' + date[0][6:] + ' ' + \
                      date[1][: 2] + ':' + date[1][2: 4] + ':' + date[1][4:]

        return task_name + ',' + format_date

    @staticmethod
    def format_instance_start_end(result):
        for item in result['list']:
            for content in item:
                for node in content:
                    if node:
                        node['start_date'] = DateUtil.to_timestamp(node['start_date']) if node.get('start_date') else 0
                        node['end_date'] = DateUtil.to_timestamp(node['end_date']) if node.get('end_date') else 0

    @staticmethod
    def get_instance(link, result):
        # 获取柱状图的二维数组节点信息
        node = None
        for i in range(len(result['list'])):
            if result['list'][i].keys()[0] == link:
                node = result['list'][i][link]
                del result['list'][i]
                break
        return node

    @staticmethod
    def add_id_spark(is_external, nodes, task_id, is_spark_task, template_code):
        # 给非外部数据节点，添加task_id、is_spark_task
        if not is_external:
            for item in nodes:
                for column in item:
                    column['task_id'] = task_id
                    column['is_spark_task'] = is_spark_task
                    column['template_code'] = template_code

    @staticmethod
    def handle_node(item, node, instances):
        DiagnosisService.add_id_spark(item.get('isExternal'), node, item.get('task_id'), item.get('is_spark_task'), item.get('template_code'))
        instances.append(node)
        # 删除tree中的task_id, is_spark_task
        del item['task_id']
        del item['is_spark_task']
        del item['template_code']

    @staticmethod
    def format_instances(result):
        # 去掉实例的key，即将map保留value这个list值
        # 删除tree中的task_id, is_spark_task，将其填充到list的对应节点中
        # 不同下游，可能依赖同一个上游，其中一个下游处理过其上游之后，其它下游就不用再处理该上游(删除list中的二维节点信息)
        instances = list()
        node = DiagnosisService.get_instance(result.get('tree').get('link'), result)
        if node is not None:
            DiagnosisService.add_id_spark(result.get('tree').get('isExternal'), node, result.get('tree').get('task_id'),
                                          result.get('tree').get('is_spark_task'), result.get('tree').get('template_code'))
            instances.append(node)
            del result.get('tree')['task_id']
            del result.get('tree')['is_spark_task']
            del result.get('tree')['template_code']
        for item in result.get('tree').get('children'):
            node = DiagnosisService.get_instance(item['link'], result)
            if node is not None:
                DiagnosisService.handle_node(item, node, instances)
            if len(item['children']):
                for content in item['children']:
                    node = DiagnosisService.get_instance(content['link'], result)
                    if node is not None:
                        DiagnosisService.handle_node(content, node, instances)
                    if len(content['children']):
                        for info in content['children']:
                            node = DiagnosisService.get_instance(info['link'], result)
                            if node is not None:
                                DiagnosisService.handle_node(info, node, instances)
        return instances

    @staticmethod
    def add_instance_cost(columns, column):
        length = len(columns)
        if length == 0:
            columns.append([int(column//1000)])
            return

        tail = len(columns[length-1])
        if tail == 5:
            columns.append([int(column//1000)])
        else:
            columns[-1].append(int(column//1000))

    @staticmethod
    def get_histogram_median(result, num):
        # 柱状图获取每一个实例任务（一列）的执行时长
        columns = list()
        new_list = list()
        for item in result['list']:
            info = list()
            for i in range(len(item)):
                for j in range(len(item[i])):
                    info.append(item[i][j])
            new_list.append(info)

        if new_list:  # 排除没有实例的异常
            length = len(new_list[0])
            if num > length:
                num = length
            for i in range(num):
                column = 0
                for j in range(len(new_list)):
                    end_date = new_list[j][i].get('end_date', 0)
                    start_date = new_list[j][i].get('start_date', 0)
                    if not end_date and not start_date:
                        continue
                    if end_date < start_date:
                        end_date = start_date
                    column += end_date - start_date
                DiagnosisService.add_instance_cost(columns, column)
        result['time_column'] = columns
        result['time_median'] = DateUtil.second_str(int(DateUtil.get_time_median(columns))) if num > 0 else 0

    def data_link_analysis(self, task_id, execution_date, num, gantt=0,tenant_id=1,ds_task_name=None):
        result = self._data_link_analysis(task_id, execution_date, num, gantt,ds_task_name=ds_task_name,tenant_id=tenant_id)
        print('origin-result==', result)
        if gantt == 1:  # 链路分析--甘特图
            now_timestamp = DateUtil.get_timestamp(utcnow())

            # 获取所有开始和结束日期
            start_dates = [item[0][0].get('start_date') for item in result['list']]
            end_dates = [item[0][0].get('end_date') for item in result['list']]

            # 获取最早的开始日期
            valid_start_dates = [date for date in start_dates if date]
            if valid_start_dates:
                start = min(valid_start_dates)
            else:
                start = now_timestamp

            if start > now_timestamp:
                start = now_timestamp

            # 获取最晚的结束日期
            valid_end_dates = [date for date in end_dates if date]
            if valid_end_dates:
                end = max(valid_end_dates)
            else:
                end = now_timestamp
            time_difference = (end-start)/1000
            internal = DateUtil.get_gantt_internal(time_difference)
            result['internal'] = internal
            result['start_date'] = start - internal * 1000 // 5
            result['end_date'] = end + internal * 1000 // 5
            return result
        else:  # 链路分析--柱状图
            print('format_instances-result==', result)
            print('histogram-result==', result)
            DiagnosisService.get_histogram_median(result, num)
            return result

    @staticmethod
    def add_genie(dag_id, node, genie_domain):
        task_instance = TaskInstance.find_by_execution_dates(dag_id=dag_id, task_id=dag_id,
                            execution_dates=[pendulum.parse(node.get('executionDate')).replace(tzinfo=beijing)])
        if task_instance and task_instance[0].external_conf:
            external_conf = json.loads(task_instance[0].external_conf)
            node['version'] = external_conf.get('version', -1)
            node['genie_job_id'] = str(external_conf.get('genie_job_id', ''))
            node['genie_job_url'] = genie_domain + 'file/' + node['genie_job_id'] + '/output/stderr'
        else:
            node['version'] = -1
            node['genie_job_id'] = ''
            node['genie_job_url'] = ''

    @staticmethod
    def get_histogram_start(dates, gra):
        interval = list()
        length = len(dates)
        end_time = ''

        # test使用
        print('dates-length==', length)
        print('dates==', dates)
        if length == 0:
            return interval, end_time
        if length <= 5:
            interval.append(DateUtil.format_histogram_date(dates[0], gra))
            if length == 5:
                end_time = DateUtil.format_histogram_date(dates[4], gra)
            return interval, end_time

        nums = range(0, length, 5)
        for i in nums:
            interval.append(DateUtil.format_histogram_date(dates[i], gra))
        if len(nums) * 5 == length:
            end_time = DateUtil.format_histogram_date(dates[-1], gra)
        return interval, end_time

    @staticmethod
    def get_id_spark(name,tenant_id=1):
        if name:
            result = GreeterClient().getTaskInfoByName(name,tenant_id=tenant_id)
            if result.get('code') != 0:
                return -1, result.get('message'), ''
            else:
                return result.get('info').get('id'), result.get('info').get('isSparkTask'), str(result.get('info').get('templateCode'))
        else:
            return -1, 'invalid dag_id[{}]'.format(name), ''

    def generate_link_tree(self,upstreams, result,tree_node_sequence):
        # 生成链路分析的树状图
        def add_node(children, item):
            children.append({
                'name': item['dagId'],
                'task_id': item.get('task_id'),
                'isExternal': item['isExternal'],
                'link': item.get('link'),
                'children': [],
                'pipeline_id': item.get('taskName')
            })
            tree_node_sequence.append(item.get('link'))

        def is_contain_name(name, children):
            return children['pipeline_id'] == name

        for item in upstreams:
            down_task_name = item['downTaskName']
            if down_task_name == result['tree']['link']:  # 一级节点
                add_node(result['tree']['children'], item)
            else:
                # 遍历子节点
                for i, child in enumerate(result['tree']['children']):
                    if is_contain_name(down_task_name, child):  # 二级节点
                        add_node(child['children'], item)
                        break
                    else:
                        # 遍历子节点的子节点（三级节点）
                        for sub_child in child['children']:
                            if is_contain_name(down_task_name, sub_child):
                                add_node(sub_child['children'], item)
                                break

    def _data_link_analysis(self, task_id, execution_date, num, gantt,ds_task_name=None,tenant_id=1):
        """
        depth: 深度<=3
        num: 实例个数
        execution_date: 当前实例task_id的执行时间
        return: 获取当前节点的数据链路分析图：树结构+实例柱状图
        """
        gra = ''
        self.depth = 3
        self.lineage = 2
        tree = True  # 遍历一次获取树结构
        result = dict()
        result['tree'] = dict()
        result['list'] = list()

        result['tree']['name'] = ds_task_name
        result['tree']['children'] = list()

        '''
            这个link_dates_dict的结构是这样的, 
            {'link_id': [{'date': ti_info}, {'date': ti_info}]
        '''
        link_dates_dict = {} # 用于存储每个link_id的实例时间

        tree_node_sequence = [] # 用于存储树结构的节点顺序

        metadata_id = DagDatasetRelation.get_metadata_id(task_id)
        root_node = CheckNode(task_name=task_id,execution_date=execution_date,metadata_id=metadata_id)
        root_node.node_init_if_need(include_ti_info=False)
        dates = Trigger.get_data_link_dates(task_id, execution_date, num=num)[::-1]
        '''
              0 1 2
            0 a b c
            1 d e f
            2 g h i
        '''
        self.root_link = root_node.task_name
        for date in dates:
            root_node.execution_date = date.replace(tzinfo=beijing)
            root_node_instance = self.package_node_instance(root_node,include_ti_info=False)
            root_node_instance['link'] = root_node.task_name
            if root_node.task_name in link_dates_dict:
                link_dates_dict[root_node.task_name].append({date.strftime('%Y-%m-%d %H:%M:%S'): root_node_instance})
            else:
                link_dates_dict[root_node.task_name] = [{date.strftime('%Y-%m-%d %H:%M:%S'): root_node_instance}]
            if tree:
                gra = root_node.gra
                result['tree']['link'] = root_node.task_name
                result['tree']['isExternal'] = False
                result['tree']['task_id'] = root_node.ds_task_id
                tree_node_sequence.append(root_node.task_name)

            upstreams = self.recursion_upstream(task_id, date.replace(tzinfo=beijing),link_dates_dict=link_dates_dict)
            if tree:
                self.generate_link_tree(upstreams, result,tree_node_sequence)
                tree = False  # 第一个周级即将树状图绘制好

        link_list_dict = self.get_link_list_dict(link_dates_dict)
        for link in tree_node_sequence:
            result['list'].append(link_list_dict[link])

        if gantt == 0:
            histogram_internal = DiagnosisService.get_histogram_start(dates, gra)
            result['histogram_start'] = histogram_internal[0]
            result['endTime'] = histogram_internal[1]

        return result

    def get_link_list_dict(self, link_dates_dict):
        link_list_dict = {}

        for link_id, date_dict_list in link_dates_dict.items():
            exe_dates = [pendulum.parse(list(d.keys())[0]).replace(tzinfo=beijing) for d in date_dict_list]
            exe_date_node_dict = {k: v for date_dict in date_dict_list for k, v in date_dict.items()}

            date_ti_dict = {e_date.strftime('%Y-%m-%d %H:%M:%S'): [] for e_date in exe_dates}

            is_external, task_name = list(date_dict_list[0].values())[0]['isExternal'], list(date_dict_list[0].values())[0]['taskName']

            if is_external:
                link_list = [{} for d in date_dict_list]
                block_list = NormalUtil.get_split_block_list(link_list, 5)
                link_list_dict[link_id] = block_list
                continue

            tri_list = Trigger.find_by_execution_dates(dag_id=task_name, execution_dates=exe_dates)
            ti_list = TaskInstance.find_by_execution_dates(dag_id=task_name, execution_dates=exe_dates)

            for ti in ti_list:
                date_ti_dict[ti.execution_date.strftime('%Y-%m-%d %H:%M:%S')].append(ti)

            for tri in tri_list:
                exe_date = tri.execution_date.strftime('%Y-%m-%d %H:%M:%S')
                cur_tis = date_ti_dict[exe_date]
                state = tri.state if tri.state in [State.TASK_STOP_SCHEDULER, State.TASK_WAIT_UPSTREAM] else ''
                if state:
                    exe_date_node_dict[exe_date].update({'state': state,})
                    continue
                for ti in cur_tis:
                    if ti.dag_id != ti.task_id:
                        continue

                    duration = ti.get_duration()
                    genie_job_id, genie_job_url = ti.get_genie_info()
                    start_date = DateUtil.get_timestamp(ti.start_date) if ti.start_date else None
                    end_date = DateUtil.get_timestamp(ti.end_date) if ti.end_date else None
                    try_number = ti.try_number - 1 if ti.try_number > 0 else 0
                    state = State.taskinstance_state_mapping(TaskService.state_merge(cur_tis))
                    exe_date_node_dict[exe_date].update({
                        'state': state,
                        'duration': duration,
                        'genie_job_url': genie_job_url,
                        'genie_job_id': genie_job_id,
                        'start_date': start_date,
                        'end_date': end_date,
                        'try_number': try_number
                    })

            link_list = [list(d.values())[0] for d in date_dict_list]
            block_list = NormalUtil.get_split_block_list(link_list, 5)
            link_list_dict[link_id] = block_list

        return link_list_dict

    # def get_link_list_dict(self,link_dates_dict):
    #     link_list_dict = {}
    #     for link_id,date_dict_list in link_dates_dict.items():
    #         exe_dates = map(lambda x: pendulum.parse(list(x.keys())[0]).replace(tzinfo=beijing), date_dict_list)
    #         exe_date_node_dict = {}
    #         for date_dict in date_dict_list:
    #             for k,v in date_dict:
    #                 exe_date_node_dict[k] = v
    #
    #         date_ti_dict = {}
    #         for e_date in exe_dates:
    #             date_ti_dict[e_date.strftime('%Y-%m-%d %H:%M:%S')] = []
    #
    #         is_external = False
    #         task_name = ''
    #         for k,v in date_dict_list[0].items():
    #             is_external = v.get('isExternal')
    #             task_name = v.get('taskName')
    #         if is_external :
    #             link_list = map(lambda x: list[x.values()][0],date_dict_list)
    #             block_list = NormalUtil.get_split_block_list(link_list, 5)
    #             link_list_dict[link_id] = block_list
    #             continue
    #
    #
    #         tri_list = Trigger.find_by_execution_dates(dag_id=task_name, execution_dates=exe_dates)
    #         ti_list = TaskInstance.find_by_execution_dates(dag_id=task_name, execution_dates=exe_dates)
    #
    #
    #         for ti in ti_list:
    #             # sharestore任务有两个子任务,fuck it
    #             date_ti_dict[ti.execution_date.strftime('%Y-%m-%d %H:%M:%S')].append(ti)
    #
    #         for tri in tri_list:
    #             duration = 0
    #             genie_job_url = ''
    #             genie_job_id = ''
    #             start_date = ''
    #             end_date = ''
    #             try_number = 0
    #             exe_date = tri.execution_date.strftime('%Y-%m-%d %H:%M:%S')
    #             if tri.state in [State.TASK_STOP_SCHEDULER, State.TASK_WAIT_UPSTREAM]:
    #                 state = tri.state
    #             else:
    #                 cur_tis = date_ti_dict[exe_date]
    #                 # 状态合并, sharestore有两个子任务, fuck it!!!!!!!!!!!!!!!!!!!!!
    #                 state = State.taskinstance_state_mapping(TaskService.state_merge(cur_tis))
    #                 for ti in cur_tis:
    #                     if ti.dag_id != ti.task_id:
    #                         continue
    #                     duration = ti.get_duration()
    #                     genie_info = ti.get_genie_info()
    #                     genie_job_url = genie_info[1]
    #                     genie_job_id = genie_info[0]
    #                     start_date = DateUtil.get_timestamp(ti.start_date) if ti.start_date else None
    #                     end_date = DateUtil.get_timestamp(ti.end_date) if ti.end_date else None
    #                     try_number = ti.try_number - 1 if ti.try_number > 0 else 0
    #             exe_date_node_dict[exe_date]['state'] = state
    #             exe_date_node_dict[exe_date]['duration'] = duration
    #             exe_date_node_dict[exe_date]['genie_job_url'] = genie_job_url
    #             exe_date_node_dict[exe_date]['genie_job_id'] = genie_job_id
    #             exe_date_node_dict[exe_date]['start_date'] = start_date
    #             exe_date_node_dict[exe_date]['end_date'] = end_date
    #             exe_date_node_dict[exe_date]['try_number'] = try_number
    #
    #         link_list = map(lambda x: list[x.values()][0], date_dict_list)
    #         block_list = NormalUtil.get_split_block_list(link_list, 5)
    #         link_list_dict[link_id] = block_list

    def _get_upstream_list(self, task_id, execution_date):
        ## 获取上游结果
        result_tuple = CheckDependecyHelper.list_dependency_result_tuple(dag_id=task_id,
                                                                         execution_date=execution_date)
        # 判断返回的是成功的上游还是失败的上游
        return result_tuple[0],result_tuple[1]

    def _get_downstream_list(self, task_id, execution_date,ds_task_id,tenant_id):
        ## 获取下游结果
        result_tuple = CheckDependecyHelper.list_downstream_event_dependency_result_tuple(dag_id=task_id,
                                                                         execution_date=execution_date,ds_task_id=ds_task_id,tenant_id=tenant_id)
        # 判断返回的是成功的下游还是失败的下游
        return result_tuple[0],result_tuple[1]


    def _sorted_node_list(self, node_list):
        '''
        排序节点list，根据metadata_id，execution_date ，顺序排序
        '''
        if not node_list:
            return node_list

        return sorted(node_list,
                      key=lambda x: '{}_{}'.format(x.metadata_id, x.execution_date.strftime('%Y-%m-%d%H:%M:%S')))

    def _disitnct_fault_node_list(self, fault_node_list):
        '''
        去重故障节点list
        根据metadata_id与execution_date进行去重
        '''
        fault_node_dict = {}

        if len(fault_node_list) == 0:
            return fault_node_list

        def filter_rule(node):
            key = (node.metadata_id, node.execution_date.strftime('%Y-%m-%d %H:%M:%S'))
            if fault_node_dict.has_key(key):
                return False
            else:
                fault_node_dict[key] = True
                return True

        filter_list = filter(lambda x: filter_rule(x), fault_node_list)
        return filter_list

    def _generate_resp_data(self, msg, fault_node_list=None, success_node_list=None, fault_check_path_list=None):
        resp = {
            "reason": msg
        }
        if fault_node_list != None:
            resp["faultNodeList"] = fault_node_list

        if success_node_list != None:
            resp["successNodeList"] = success_node_list

        if fault_check_path_list != None:
            resp["faultCheckPathList"] = fault_check_path_list

        return resp


'''
每个数据集的分区可以看做是调度的一个节点，此类用来存储数据集有关的一些信息
'''


class DatasetNode(LoggingMixin):
    def __init__(self,
                 metadata_id,
                 data_partition_date=None,
                 down_task_id=None,
                 task_id=None,
                 link=0
                 ):
        self.metadata_id = metadata_id
        self.data_partition_date = data_partition_date
        self.dag_id = task_id
        self.down_dag_id = None
        self.check_path = None
        self.execution_date = None
        self.owner = None
        self.success_time = None
        self.recursion_cnt = 0
        self.down_node_id = None
        self.task_desc = None

        if task_id :
            self._set_dataset_base_info_by_dag_id()
        # elif down_task_id is None or link == 1 or link == 2:
        elif down_task_id is None:
            self._set_dataset_base_info()
        else:
            self._set_dataset_base_info_by_down(down_task_id)
        self._set_execution_date()
        self.node_id = '{},{}'.format(self.metadata_id if self.is_external else self.dag_id,
                                      self.execution_date.strftime('%Y-%m-%d %H:%M:%S'))

    def _set_dataset_base_info(self):
        '''
        获取数据集的基本信息，gra、offset、is_external
        '''
        datasets = DagDatasetRelation.get_active_upstream_dags(metadata_id=self.metadata_id)
        if datasets and DagModel.is_active_and_on(dag_id=datasets[0].dag_id):
            self.gra = datasets[0].granularity
            self.offset = datasets[0].offset
            self.dag_id = datasets[0].dag_id
            self.is_external = False
            self.task_desc = TaskDesc.get_task(task_name=self.dag_id)
        else:
            self.gra = ''
            self.dag_id = None
            self.is_external = True

    def _set_dataset_base_info_by_dag_id(self):
        '''
        获取数据集的基本信息，gra、offset、is_external
        '''
        self.task_desc = TaskDesc.get_task(task_name=self.dag_id)
        self.is_external = False
        datasets = DagDatasetRelation.get_active_upstream_dags(metadata_id=self.metadata_id)
        if datasets :
            self.gra = datasets[0].granularity
            self.offset = datasets[0].offset
        else:
            # 如果不存在,这是不合理的,但给一个默认的值
            self.gra = Granularity.gra_priority_map['daily']
            self.offset = -1

    @staticmethod
    def get_gra(metadata_id):
        datasets = DagDatasetRelation.get_active_upstream_dags(metadata_id=metadata_id)
        if datasets and DagModel.is_active_and_on(dag_id=datasets[0].dag_id):
            gra = datasets[0].granularity
        else:
            gra = ''
        return gra

    def _set_dataset_base_info_by_down(self, down_task_id):
        '''
        获取数据集的基本信息，gra、offset、is_external
        '''
        is_external = True
        datasets = DagDatasetRelation.get_active_upstream_dags(metadata_id=self.metadata_id)
        # 当数据集被某个任务产出，并且这个任务被当前下游任务配置了任务依赖时，才认为是一个任务
        if datasets:
            dependences = EventDependsInfo.get_dag_event_depends(down_task_id, Constant.TASK_SUCCESS_EVENT)
            for dep in dependences:
                if dep.get_depend_task_id() == datasets[0].dag_id:
                    is_external = False
                    break
        if is_external:
            self.dag_id = None
            self.is_external = is_external
        else:
            self.gra = datasets[0].granularity
            self.offset = datasets[0].offset
            self.dag_id = datasets[0].dag_id
            self.is_external = is_external
            self.task_desc = TaskDesc.get_task(task_name=self.dag_id)


    def _set_execution_date(self):
        '''
        找到当前数据日期的数据集，对应的调度实例日期是哪一天
        '''
        # todo 这里待优化，把数据日期的概念去掉
        # 去掉了生成数据集偏移量之后 ，所有的调度日期都等于数据日期
        self.execution_date = self.data_partition_date

        # 外部数据没有实例，所以使用数据日期作为其实例日期
        # if self.is_external:
        #     self.execution_date = self.data_partition_date
        #     return
        # # todo
        # if self.data_partition_date != None:
        #     tmp_exe = Granularity.getNextTime(self.data_partition_date, gra=self.gra, offset=self.offset)
        #     execution_dates = TaskCronUtil.get_all_date_in_one_cycle(self.dag_id, tmp_exe)
        #     self.execution_date = execution_dates[0]
        #     return
        # else:
        #     self.execution_date = None

    def set_task_base_info(self):
        '''
        获取一下taks的基本信息
        '''
        dag_id = ''
        if self.is_external:
            if self.down_dag_id is None:
                self.owner = ''
                return
            else:
                dag_id = self.down_dag_id
        else:
            dag_id = self.dag_id
        # 如果是外部数据，这里其实取的是直接下游的owner
        task = TaskDesc.get_task(task_name=dag_id)
        if task is None:
            self.owner = ''
        else:
            self.owner = task.owner

    def set_external_check_path(self, down_execution_date):
        '''
        获取外部数据的检查路径
        '''
        if self.is_external:
            # todo 理论上应该获取真正判断成功那一次checkpath记录，but现在做不到呀，怎么搞呢
            si = SensorInfo.find(dag_id=self.down_dag_id, metadata_id=self.metadata_id,
                                 detailed_execution_date=self.data_partition_date, execution_date=down_execution_date)

            if len(si) > 0:
                self.check_path = si[0].check_path
                return

        self.check_path = ""
        return

    def set_dataset_success_time(self):
        from airflow.shareit.models.sensor_info import SensorInfo
        si = SensorInfo()
        sensors = si.find(dag_id=self.dag_id, metadata_id=self.metadata_id,
                          detailed_execution_date=self.data_partition_date)
        if self.is_external and len(sensors) > 0:
            si = SensorInfo.find(dag_id=self.down_dag_id, metadata_id=self.metadata_id,
                                 detailed_execution_date=self.data_partition_date, state=State.SUCCESS)
            if len(si) > 0:
                self.success_time = si[0].last_check
        else:
            dataset = DatasetPartitionStatus.find(self.metadata_id, dataset_partition_sign=self.data_partition_date)
            if len(dataset) > 0:
                self.success_time = dataset[0].update_date + timedelta(hours=8)
            else:
                self.success_time = None

    def set_down_dag_id(self, down_dag_id, execution_date):
        self.down_dag_id = down_dag_id
        self.down_node_id = '{},{}'.format(down_dag_id, execution_date.strftime('%Y-%m-%d %H:%M:%S'))

    def set_recursion_cnt(self, num):
        self.recursion_cnt = num

    def set_owner(self, dag_id):
        dag = DagModel().get_dagmodel(dag_id)
        self.owner = dag.owners if dag else ''

    @staticmethod
    def get_data_partition_date(task_id, metadata_id, execution_date):
        gra = 0
        offset = 0
        flag = False
        datasets = DagDatasetRelation.get_active_upstream_dags(metadata_id=metadata_id)
        if datasets:
            dependences = EventDependsInfo.get_dag_event_depends(task_id, Constant.TASK_SUCCESS_EVENT)
            for dep in dependences:
                if dep.depend_id == datasets[0].dag_id:
                    flag = True
                    break
        if datasets and DagModel.is_active_and_on(dag_id=datasets[0].dag_id):
            gra = datasets[0].granularity
            offset = datasets[0].offset
        else:
            if datasets and flag:
                gra = datasets[0].granularity
                offset = datasets[0].offset
        return Granularity.getNextTime(execution_date, gra=gra, offset=-offset)
