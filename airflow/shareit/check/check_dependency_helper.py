# -*- coding: utf-8 -*-
import datetime
import os
import signal

import jinja2
import requests
import six

from airflow.shareit.check.check_node import CheckNode
from airflow.shareit.models.dataset_partation_status import DatasetPartitionStatus
from airflow.shareit.models.event_depends_info import EventDependsInfo
from airflow.shareit.models.task_desc import TaskDesc
from airflow.shareit.models.trigger import Trigger
from airflow.shareit.utils.constant import Constant
from airflow.shareit.utils.date_calculation_util import DateCalculationUtil
from airflow.shareit.utils.date_util import DateUtil
from airflow.shareit.utils.granularity import Granularity
from airflow.shareit.utils.task_cron_util import TaskCronUtil
from airflow.utils import timezone
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.shareit.utils.state import State
from datetime import timedelta
from airflow.models.dag import DagModel
from airflow.shareit.service.dataset_service import DatasetService
from airflow.shareit.models.dag_dataset_relation import DagDatasetRelation
from airflow.utils.timezone import my_make_naive
from airflow.models import TaskInstance
log = LoggingMixin()


# check模块的功能: 1.check上游有没有完成 2.获取未就绪的上游列表
class CheckDependecyHelper(LoggingMixin):
    def __init__(self):
        return

    @staticmethod
    def check_dependency(dag_id, execution_date, is_backfill=False):
        '''
        检查上游是否全部就绪
            Returns:
                bool,date(时间戳) ,上游是否ready,ready时间
        '''
        is_event_depend_ready, event_ready_time = CheckDependecyHelper.check_event_dependency(dag_id, execution_date,
                                                                            is_backfill=is_backfill)
        is_data_depend_ready, data_ready_time = CheckDependecyHelper.check_data_dependency(dag_id, execution_date,
                                                                          is_backfill=is_backfill)
        return is_event_depend_ready and is_data_depend_ready,max(event_ready_time,data_ready_time)

    @staticmethod
    def list_dependency_result_tuple(dag_id, execution_date, is_backfill=False) :
        # type: (str, datetime.datetime, bool) -> (list[CheckNode], list[CheckNode])
        success_list = []
        failed_list = []

        event_result_tuple = CheckDependecyHelper.list_event_dependency_result_tuple(dag_id, execution_date,
                                                                              is_backfill=is_backfill)

        data_result_tuple = CheckDependecyHelper.list_data_dependency_result_tuple(dag_id, execution_date,
                                                                                    is_backfill=is_backfill)

        success_list.extend(event_result_tuple[0])
        success_list.extend(data_result_tuple[0])

        failed_list.extend(event_result_tuple[1])
        failed_list.extend(data_result_tuple[1])
        return (success_list,failed_list)

    @staticmethod
    def list_data_dependency(dag_id, execution_date):
        '''
        获取所有数据依赖
        '''
        result_list = []
        # 获取该任务的所有上游数据
        input_datasets = DagDatasetRelation.get_inputs(dag_id=dag_id)
        input_datasets = DateCalculationUtil.generate_ddr_date_calculation_param(input_datasets)
        # # 用来判断是不是外部数据
        # outer_datasets = [(out.dag_id, out.metadata_id) for out in DagDatasetRelation.get_outer_dataset(dag_id=dag_id)]
        for dataset in input_datasets:
            # 获取所有数据日期
            data_dates = DateCalculationUtil.get_upstream_all_data_dates(execution_date=execution_date,
                                                                         calculation_param=dataset.date_calculation_param,
                                                                         gra=dataset.granularity)
            for data_date in data_dates:
                result_list.append((data_date,dataset.metadata_id))
        return result_list

    @staticmethod
    def get_dependency(dag_id, execution_date):
        """
        :return: 获取所有上游结果，不管是否就绪
        """
        event_dep = CheckDependecyHelper.get_event_dependency(dag_id, execution_date)
        data_dep = CheckDependecyHelper.get_data_dependency(dag_id, execution_date)
        event_dep.extend(data_dep)
        return event_dep


    @staticmethod
    def check_event_dependency(dag_id, execution_date, is_backfill=False):
        '''
        检查事件上游是否全部就绪
            Return:
                bool,int(时间戳)  是否ready,ready时间
        '''

        # 目前只有一种事件,先这样
        event_depends = EventDependsInfo.get_dag_event_depends(dag_id, Constant.TASK_SUCCESS_EVENT)
        if len(event_depends) == 0:
            return True,0
        all_success_time = []
        for e_depend in event_depends:
            if not e_depend.depend_ds_task_id :
                return False,0
            td = TaskDesc.get_task_with_tenant(ds_task_id=e_depend.depend_ds_task_id,tenant_id=e_depend.tenant_id)
            if not td:
                return False,0
            upstream_dag_id = td.task_name
            # 上游下线判定失败
            if not DagModel.is_active_and_on(dag_id=upstream_dag_id):
                return False,0
            # 获取要检查的任务的所有实例日期
            upstream_exe_dates = DateCalculationUtil.get_upstream_all_execution_dates(execution_date,
                                                                                     e_depend.date_calculation_param,e_depend.depend_id,
                                                                                      upstream_gra=td.granularity,upstream_crontab=td.crontab)
            for up_exe_date in upstream_exe_dates:
                check_node = CheckNode()
                is_success = CheckDependecyHelper.check_ti_is_success(upstream_dag_id, up_exe_date,
                                                                      check_node, gra=td.granularity,
                                                                      is_backfill=is_backfill,
                                                                      create_date=td.create_date,
                                                                      )
                all_success_time.append(check_node.ready_time)
                if not is_success:
                    return False, 0

        return True,max(all_success_time) if len(all_success_time) > 0 else 0

    @staticmethod
    def get_event_dependency(dag_id, execution_date):
        """
        :return: 获取所有事件(任务成功)上游的结果， 包括已成功的上游数据和未成功的上游
        """
        events = list()
        event_depends = EventDependsInfo.get_dag_event_depends(dag_id, Constant.TASK_SUCCESS_EVENT)
        for e_depend in event_depends:
            if not e_depend.depend_ds_task_id:
                continue
            td = TaskDesc.get_task_with_tenant(ds_task_id=e_depend.depend_ds_task_id,tenant_id=e_depend.tenant_id)
            if not td :
                continue
            upstream_dag_id = td.task_name
            is_active = True
            # 上游下线判定失败
            if not DagModel.is_active_and_on(dag_id=upstream_dag_id):
                is_active = False
            # 获取要检查的任务的所有实例日期
            upstream_exe_dates = DateCalculationUtil.get_upstream_all_execution_dates(execution_date,
                                                                                      e_depend.date_calculation_param,
                                                                                      e_depend.depend_id,upstream_gra=td.granularity,upstream_crontab=td.crontab)
            outputs = DagDatasetRelation.get_outputs(dag_id=upstream_dag_id)
            for up_exe_date in upstream_exe_dates:
                check_node = CheckNode(task_name=td.task_name,
                                       ds_task_name=td.ds_task_name,
                                       ds_task_id=td.ds_task_id,
                                       execution_date=up_exe_date,
                                       metadata_id=outputs[0].metadata_id if outputs else None,
                                       is_active=is_active,
                                       downstream_task_name=dag_id,
                                       downstream_task_execution_date=execution_date,
                                       owner=td.owner,
                                       )

                events.append(check_node)
        return events

    @staticmethod
    def list_event_dependency_result_tuple(dag_id, execution_date, is_backfill=False):
        # type: (str, datetime, bool) -> (list[CheckNode], list[CheckNode])
        from airflow.models.dag import DagModel
        '''
        获取所有事件(任务成功)上游的结果

        return: 返回已成功的上游数据和未成功的上游 (success_list[(date_date,metadata_id)],failed_list)
        '''
        success_list = []
        failed_list = []

        # 目前只有一种事件,先这样
        event_depends = EventDependsInfo.get_dag_event_depends(dag_id, Constant.TASK_SUCCESS_EVENT)

        for e_depend in event_depends:
            if not e_depend.depend_ds_task_id:
                continue
            td = TaskDesc.get_task_with_tenant(ds_task_id=e_depend.depend_ds_task_id,tenant_id=e_depend.tenant_id)
            if not td:
                check_node = CheckNode(task_name=str(e_depend.depend_ds_task_id),
                                       ds_task_name=str(e_depend.depend_ds_task_id),
                                       ds_task_id=e_depend.depend_ds_task_id,
                                       execution_date=execution_date,
                                       metadata_id=None,
                                       is_active=False,
                                       downstream_task_name=dag_id,
                                       downstream_task_execution_date=execution_date,
                                       owner='',
                                       )
                failed_list.append(check_node)
                continue
            upstream_dag_id = td.task_name
            is_active = True
            # 上游下线判定失败
            if not DagModel.is_active_and_on(dag_id=upstream_dag_id):
                is_active = False
            # 获取要检查的任务的所有实例日期
            upstream_exe_dates = DateCalculationUtil.get_upstream_all_execution_dates(execution_date,
                                                                                      e_depend.date_calculation_param,
                                                                                      e_depend.depend_id,upstream_gra=td.granularity,upstream_crontab=td.crontab)

            outputs = DagDatasetRelation.get_outputs(dag_id=upstream_dag_id)
            for up_exe_date in upstream_exe_dates:
                check_node = CheckNode(task_name=td.task_name,
                                       ds_task_name=td.ds_task_name,
                                       ds_task_id=td.ds_task_id,
                                       execution_date=up_exe_date,
                                       metadata_id=outputs[0].metadata_id if outputs else None,
                                       is_active=is_active,
                                       downstream_task_name=dag_id,
                                       downstream_task_execution_date=execution_date,
                                       owner=td.owner,
                                       )
                is_success = CheckDependecyHelper.check_ti_is_success(upstream_dag_id, up_exe_date,check_node,gra=td.granularity,
                                                                                    is_backfill=is_backfill)
                if not is_success or not is_active:
                    # tri = CheckDependecyHelper.get_lastest_trigger(task_name=upstream_dag_id,execution_date=up_exe_date)
                    check_node.execution_date = check_node.tri.execution_date if check_node.tri else up_exe_date
                    failed_list.append(check_node)
                else:
                    # tri = CheckDependecyHelper.get_lastest_trigger(task_name=upstream_dag_id,execution_date=up_exe_date)
                    check_node.execution_date = check_node.tri.execution_date if check_node.tri else up_exe_date
                    success_list.append(check_node)

        return (success_list, failed_list)

    @staticmethod
    def check_data_dependency(dag_id, execution_date, is_backfill=False):
        '''
        检查数据上游是否全部就绪
            Return:
                  bool,int(时间戳)  是否ready,ready时间
        '''
        # if is_backfill:
        #     pass_state = [State.SUCCESS, State.BACKFILLDP]
        # else:
        #     pass_state = [State.SUCCESS]
        all_success_time = []
        # 获取该任务的所有上游数据
        input_datasets = DagDatasetRelation.get_inputs(dag_id=dag_id)
        input_datasets = DateCalculationUtil.generate_ddr_date_calculation_param(input_datasets)
        # 用来判断是不是外部数据
        # outer_datasets = [(out.dag_id, out.metadata_id) for out in DagDatasetRelation.get_outer_dataset(dag_id=dag_id)]
        for dataset in input_datasets:
            # 获取所有数据日期
            data_dates = DateCalculationUtil.get_upstream_all_data_dates(execution_date=execution_date,
                                                                         calculation_param=dataset.date_calculation_param,
                                                                         gra=dataset.granularity)


            from airflow.shareit.models.sensor_info import SensorInfo
            si = SensorInfo()
            si_dict = si.get_detailed_sensors_dict(dag_id=dag_id, metadata_id=dataset.metadata_id,
                                                   execution_date=execution_date)
            # 如果是外部数据判断有没有sensor,
            # if (dataset.dag_id, dataset.metadata_id) in outer_datasets:
            if len(si_dict) == 0:
                return False, 0
            for data_date in data_dates:
                if data_date.strftime('%Y%m%d %H%M%S') in si_dict.keys():
                    if si_dict[data_date.strftime('%Y%m%d %H%M%S')].state != State.SUCCESS:
                        return False, 0
                    all_success_time.append(DateUtil.get_timestamp(si_dict[data_date.strftime('%Y%m%d %H%M%S')].last_check))
                else:
                    return False, 0

        return True, max(all_success_time) if len(all_success_time) > 0 else 0

    @staticmethod
    def get_data_dependency(dag_id, execution_date):
        """
        :return: 检查数据上游
        """
        datas = list()
        # 获取该任务的所有上游数据
        input_datasets = DagDatasetRelation.get_inputs(dag_id=dag_id)
        input_datasets = DateCalculationUtil.generate_ddr_date_calculation_param(input_datasets)
        for dataset in input_datasets:
            # 获取所有数据日期
            data_dates = DateCalculationUtil.get_upstream_all_data_dates(execution_date=execution_date,
                                                                         calculation_param=dataset.date_calculation_param,
                                                                         gra=dataset.granularity)
            for data_date in data_dates:
                check_node = CheckNode(metadata_id=dataset.metadata_id, execution_date=data_date,
                                       is_task=False,
                                       downstream_task_name=dag_id,
                                       downstream_task_execution_date=execution_date,
                                       )
                datas.append(check_node)
        return datas

    @staticmethod
    def list_data_dependency_result_tuple(dag_id, execution_date, is_backfill=False):
        # type: (str, datetime, bool) -> List[CheckNode]
        '''
        检查数据上游是否全部就绪
        '''
        success_list = []
        failed_list = []

        # 获取该任务的所有上游数据
        input_datasets = DagDatasetRelation.get_inputs(dag_id=dag_id)
        input_datasets = DateCalculationUtil.generate_ddr_date_calculation_param(input_datasets)
        for dataset in input_datasets:
            # 获取所有数据日期
            data_dates = DateCalculationUtil.get_upstream_all_data_dates(execution_date=execution_date,
                                                                         calculation_param=dataset.date_calculation_param,
                                                                         gra=dataset.granularity)

            from airflow.shareit.models.sensor_info import SensorInfo
            si = SensorInfo()
            si_dict = si.get_detailed_sensors_dict(dag_id=dag_id, metadata_id=dataset.metadata_id, execution_date=execution_date)

            # # todo 存在一种情况，外部数据集之前由别的任务依赖，存在数据状态但无sensor，那么首次这里获取时，状态存在的就在成功列表中。补数时，就会缺少sensor
            # # 如果是外部数据判断有没有sensor,
            # if (dataset.dag_id, dataset.metadata_id) in outer_datasets:  # todo：如果是外部数据，没有sensor怎么处理？
            flag = False
            if len(si_dict) == 0:
                flag = True
            for data_date in data_dates:
                check_node = CheckNode(metadata_id=dataset.metadata_id, execution_date=data_date,
                                       is_task=False,
                                       downstream_task_name=dag_id,
                                       downstream_task_execution_date=execution_date,
                                       )
                if is_backfill or flag:
                    failed_list.append(check_node)  # todo: 为什么这是失败的list？

                else:
                    if data_date.strftime('%Y%m%d %H%M%S') in si_dict.keys():
                        cur_si = si_dict[data_date.strftime('%Y%m%d %H%M%S')]
                        if cur_si.check_path:
                            check_node.check_path = cur_si.check_path

                        if cur_si.state == State.SUCCESS:
                            check_node.is_success = True
                            check_node.ready_time = DateUtil.get_timestamp(cur_si.last_check)
                            success_list.append(check_node)
                        else:
                            failed_list.append(check_node)
                    else:
                        failed_list.append(check_node)
        return (success_list,failed_list)


    @staticmethod
    def get_lastest_trigger(task_name, execution_date,gra=None):
        execution_date = execution_date.replace(tzinfo=timezone.beijing)
        start_date, end_date = TaskCronUtil.get_all_date_in_one_cycle_range(task_name=task_name,execution_date=execution_date,
                                                                            gra=int(gra))
        tri = Trigger.find_lastest_trigger(dag_id=task_name, start_date=start_date, end_date=end_date)
        return tri

    @staticmethod
    def check_ti_is_success(task_name, execution_date,check_node,gra=None,is_backfill=False,create_date=None):
        '''
            Return: taskInstanceNode
                 bool,taskInstanceNode(时间戳)  是否ready,ready时间
        '''

        tri = CheckDependecyHelper.get_lastest_trigger(task_name=task_name, execution_date=execution_date,gra=gra)
        if tri:
            tis = TaskInstance.find_by_execution_date(dag_id=task_name, task_id=task_name, execution_date=tri.execution_date.replace(tzinfo=timezone.beijing))
            check_node.tri = tri
            check_node.tis = tis if tis else []
        else:
            tis = None
        # tri = Trigger.find(dag_id=task_name, execution_date=execution_date)
        ti = tis[0] if tis and len(tis)> 0 else None
        ti_state = tis[0].state if ti else None
        tri_state = tri.state if tri else None
        '''
        这里的逻辑描述一下：
            如果是非补数、重算任务，判断成功的条件是：
                1、trigger状态非‘waiting’状态
                2、任务实例状态为success
            如果是补数、重算任务，判断成功的条件是:
                1、trigger状态非‘waiting’状态
                2、任务实例状态为success 或 任务实例不存在，但是实例日期>任务创建日期-2天
        '''
        if is_backfill:

            if tri_state:
                # trigger存在，优先判断trigger是否为waiting
                if tri_state in [State.WAITING,State.BF_WAIT_DEP,State.TASK_STOP_SCHEDULER]:
                    return False
                # ti存在，判断ti状态是否为success
                if ti_state:
                    if ti_state == State.SUCCESS:
                        check_node.is_success = True
                        check_node.ready_time = DateUtil.get_timestamp(ti.end_date if ti.end_date else ti.update_date + timedelta(hours=8))
                        return True
                return False
            else:
                # trigger不存在，理论上ti也是不存在的,直接判断任务创建日期
                # flag_date = create_date - timedelta(days=2)
                # if DateUtil.date_compare(flag_date,execution_date):
                #     # 如果在任务的创建日期一天之前，那我们在补数的时候认为此实例已经成功
                #     check_node.is_success = True
                #     return True
                return False
        else:

            if tri_state:
                # trigger存在，优先判断trigger是否为waiting
                if tri_state in [State.WAITING,State.BF_WAIT_DEP,State.TASK_STOP_SCHEDULER]:
                    return False
                # ti存在，判断ti状态是否为success
                if ti_state:
                    if ti_state == State.SUCCESS:
                        check_node.ready_time = DateUtil.get_timestamp(ti.end_date if ti.end_date else ti.update_date + timedelta(hours=8))
                        check_node.is_success = True
                        return True
                return False
            else:
                # trigger不存在，ti也不会存在
                return False

    @staticmethod
    def list_downstream_event_dependency_result_tuple(dag_id, execution_date,ds_task_id,tenant_id=1):
        from airflow.models.dag import DagModel
        '''
        获取所有任务依赖下游的结果

        return: 返回已成功的下游和未成功的下游 (success_list[(date_date,metadata_id)],failed_list)
        '''
        success_list = []
        failed_list = []
        event_depends = EventDependsInfo.get_downstream_event_depends(ds_task_id=ds_task_id,tenant_id=tenant_id)

        for e_depend in event_depends:
            if not e_depend.depend_ds_task_id:
                continue
            td = TaskDesc.get_task(task_name=e_depend.dag_id)
            if not td :
                continue
            downstream_dag_id = td.task_name
            is_active = True
            # 上游下线判定失败
            if not DagModel.is_active_and_on(dag_id=downstream_dag_id):
                is_active = False
            # 获取要检查的任务的所有实例日期
            downstrem_exe_dates = DateCalculationUtil.get_downstream_all_execution_dates(execution_date,
                                                                                      e_depend.date_calculation_param,
                                                                                      downstream_dag_id,td.granularity,td.crontab)

            outputs = DagDatasetRelation.get_outputs(dag_id=downstream_dag_id)
            for down_exe_date in downstrem_exe_dates:
                check_node = CheckNode(task_name=td.task_name,
                                       ds_task_name=td.ds_task_name,
                                       ds_task_id=td.ds_task_id,
                                       execution_date=down_exe_date,
                                       metadata_id=outputs[0].metadata_id if outputs else None,
                                       is_active=is_active,
                                       upstream_task_name=dag_id,
                                       upstream_task_execution_date=execution_date,
                                       owner=td.owner,
                                       )
                is_success = CheckDependecyHelper.check_ti_is_success(downstream_dag_id, down_exe_date,check_node,gra=td.granularity,
                                                                                    is_backfill=False)
                if not is_success or not is_active:
                    check_node.execution_date = check_node.tri.execution_date if check_node.tri else down_exe_date
                    failed_list.append(check_node)
                else:
                    check_node.execution_date = check_node.tri.execution_date if check_node.tri else down_exe_date
                    success_list.append(check_node)

        return (success_list, failed_list)