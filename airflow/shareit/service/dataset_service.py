# -*- coding: utf-8 -*-
"""
Author: zhangtao
CreateDate: 2021/4/25
"""
import re

import six

from airflow.shareit.utils.date_calculation_util import DateCalculationUtil
from airflow.utils.timezone import my_make_naive
from airflow.shareit.models.dag_dataset_relation import DagDatasetRelation
from airflow.shareit.models.dataset_partation_status import DatasetPartitionStatus
from airflow.shareit.models.trigger import Trigger
from airflow.shareit.utils.granularity import Granularity
from airflow.shareit.utils.state import State
from airflow.shareit.models.task_desc import TaskDesc


# from airflow.shareit.models.dataset import Dataset


class DatasetService:
    def __init__(self):
        pass

    @classmethod
    def _format_detailed_dependency(cls, detailed_dependency):
        formatted = []
        tmp = []
        if isinstance(detailed_dependency, six.string_types):
            if not detailed_dependency.strip():
                return None
            tmp = detailed_dependency.split(',')
        if isinstance(detailed_dependency, list):
            tmp = detailed_dependency

        def is_int(num_str):
            try:
                x = int(num_str)
            except Exception as e:
                raise ValueError("详细设置中详细粒度设置错误")
            return x

        def l_pattern(s):
            tmp_num = s.lower().split('l')[1]
            x = is_int(tmp_num)
            return x

        for item in tmp:
            if isinstance(item, six.string_types) and "all" == item.lower():
                formatted.append("ALL")
            elif "~" in item:
                ss = item.split("~")
                if len(ss) == 2:
                    l_num = l_pattern(ss[0])
                    r_num = l_pattern(ss[1])
                    if r_num < l_num:
                        raise ValueError("详细设置中详细粒度设置错误")
                    for i in range(l_num, r_num + 1, 1):
                        if i < 0:
                            formatted.append("L" + str(i))
                        else:
                            formatted.append("L+" + str(i))
            elif "L" in item or "l" in item:
                l_pattern(item)
                formatted.append(item)
            else:
                is_int(num_str=item)
                formatted.append(item)

        return ",".join(formatted)

    @staticmethod
    def update_task_dataset_relation(dag_id=None, metadata_id=None, dataset_type=None, granularity=None, offset=None,
                                     ready_time=None, check_path=None, detailed_dependency=None, detailed_gra=None,
                                     date_calculation_param='',transaction=False,
                                     session=None):
        ddr = DagDatasetRelation.get_relation(dag_id=dag_id, metadata_id=metadata_id, dataset_type=dataset_type,session=session)
        if detailed_dependency:
            detailed_dependency = DatasetService._format_detailed_dependency(detailed_dependency)
        if not ddr:
            ddr = DagDatasetRelation()
        if session is None:
            ddr.sync_ddr_to_db(dag_id=dag_id,
                               metadata_id=metadata_id,
                               dataset_type=dataset_type,
                               granularity=granularity,
                               offset=offset,
                               ready_time=ready_time,
                               check_path=check_path,
                               detailed_dependency=detailed_dependency,
                               detailed_gra=detailed_gra,
                               date_calculation_param=date_calculation_param,
                               session=session,
                               transaction=transaction)
        else:
            ddr.sync_ddr_to_db(dag_id=dag_id,
                               metadata_id=metadata_id,
                               dataset_type=dataset_type,
                               granularity=granularity,
                               offset=offset,
                               ready_time=ready_time,
                               check_path=check_path,
                               detailed_dependency=detailed_dependency,
                               detailed_gra=detailed_gra,
                               date_calculation_param=date_calculation_param,
                               transaction=transaction,
                               session=session)



    @staticmethod
    def add_dataset_state(dag_id, execution_date, run_id, state, transaction=False, session=None):
        # 获取任务的所有output
        # taskdesc = TaskDesc.get_task(task_name=dag_id)
        ddrs = DagDatasetRelation.get_outputs(dag_id=dag_id)
        for ddr in ddrs:
            dataset_sign_date = Granularity.getPreTime(baseDate=execution_date, gra=ddr.granularity, offset=ddr.offset)
            DatasetPartitionStatus.sync_state(metadata_id=ddr.metadata_id, dataset_partition_sign=dataset_sign_date,
                                              run_id=run_id, state=state, transaction=transaction, session=session)
        # for dataset in taskdesc.get_output_datasets():
        #     DatasetPartitionStatus.sync_state(dataset['id'], execution_date, run_id, state)
        # 给每个output均写入状态

    @staticmethod
    def get_all_input_datasets(dag_id=None,session=None):
        res = []
        if session:
            for ddr in DagDatasetRelation.get_inputs(dag_id=dag_id,session=session):
                res.append(ddr.metadata_id)
        else :
            for ddr in DagDatasetRelation.get_inputs(dag_id=dag_id):
                res.append(ddr.metadata_id)
        return res

    @staticmethod
    def get_all_output_datasets(dag_id=None,session=None):
        res = []
        if session:
            for ddr in DagDatasetRelation.get_outputs(dag_id=dag_id,session=session):
                res.append(ddr.metadata_id)
        else :
            for ddr in DagDatasetRelation.get_outputs(dag_id=dag_id):
                res.append(ddr.metadata_id)
        return res

    @staticmethod
    def delete_ddr(dag_id=None, metadata_ids=None, dataset_type=None,transaction=False,session=None):
        if session:
            DagDatasetRelation.delete(dag_id=dag_id, metadata_ids=metadata_ids, dataset_type=dataset_type,
                                      transaction=transaction, session=session)
        else:
            DagDatasetRelation.delete(dag_id=dag_id, metadata_ids=metadata_ids, dataset_type=dataset_type)

    @staticmethod
    def add_metadata_sign(metadata_id, dataset_partition_sign, state, run_id=None):
        DatasetPartitionStatus.sync_dps_to_db(metadata_id, dataset_partition_sign, state, run_id)

    @staticmethod
    def trigger_downstream_task_by_dataset_info(metadata_id, execution_date, granularity=None, state=State.WAITING,dag_id=None):
        from airflow.shareit.trigger.trigger_helper import TriggerHelper
        if dag_id:
            ddrs = DagDatasetRelation.get_active_downstream_dags(metadata_id=metadata_id, dag_id=dag_id)
        else :
            ddrs = DagDatasetRelation.get_active_downstream_dags(metadata_id=metadata_id)

        # 生成ddr的date_calculation_param信息
        downstream_ddrs = DateCalculationUtil.generate_ddr_date_calculation_param(ddrs)
        for ddr in downstream_ddrs:
            downstream_dag_id = ddr.dag_id
            # 判断是否可以被数据触发
            if not TriggerHelper.can_be_triggered_by_data(ddr.dag_id):
                continue

            downstream_exec_dates = DateCalculationUtil.get_downstream_all_execution_dates(
                execution_date=execution_date,
                calculation_param=ddr.date_calculation_param, downstream_task=downstream_dag_id)

            if downstream_exec_dates is None or len(downstream_exec_dates) == 0:
                continue
            else:
                for exec_date in downstream_exec_dates:
                    print '外部数据的数据触发: {},日期{},外部数据:{},外部数据日期:{}'.format(downstream_dag_id,exec_date.strftime("%Y%m%d %H"),metadata_id,execution_date.strftime("%Y%m%d %H"))

                    # # 只生成当前任务本次上线之后的实例，历史实例跳过(本次上线实际时间取上一粒度的开始时间，防止当天任务无法启动)
                    # td = TaskDesc.get_task(task_name=ddr.dag_id)
                    # if td:
                    #     tmp_cmp_date = Granularity.get_execution_date(td.last_update, ddr.granularity)
                    #     cmp_date = Granularity.getPreTime(tmp_cmp_date, ddr.granularity, -1)
                    #
                    #     if my_make_naive(cmp_date) >= my_make_naive(exec_date):
                    #         continue
                    #         # 生成trigger
                    Trigger.sync_trigger_to_db(dag_id=downstream_dag_id, execution_date=exec_date, state=state,priority=5,
                                                   trigger_type="pipeline", editor="DataPipeline")

    @staticmethod
    def old_trigger_downstream_task_by_dataset_info(metadata_id, execution_date, granularity=None, state=State.WAITING):
        from airflow.shareit.trigger.trigger_helper import TriggerHelper
        # dataset = Dataset.get_dataset(metadata_id)
        base_gra = DagDatasetRelation.get_dataset_granularity(metadata_id)
        ddrs = DagDatasetRelation.get_active_downstream_dags(metadata_id=metadata_id)
        from airflow.models.dag import DagModel
        for ddr in ddrs:
            # 判断是否可以被数据触发
            if not TriggerHelper.can_be_triggered_by_data(ddr.dag_id):
                continue
            if granularity is not None and Granularity.formatGranularity(granularity) != Granularity.formatGranularity(
                    ddr.granularity):
                continue
            if base_gra != -1:
                if ddr.granularity == base_gra:
                    exe_time = Granularity.getNextTime(execution_date, ddr.granularity, ddr.offset)
                elif ddr.granularity < base_gra:
                    target_time = Granularity.getNextTime(execution_date, ddr.granularity, ddr.offset)
                    exe_time = Granularity.get_execution_date(target_time, ddr.granularity)
                    if ddr.detailed_dependency is not None and ddr.detailed_gra is not None:
                        dependency_sign = Granularity.getPreTime(exe_time, ddr.granularity, ddr.offset)
                        last_dependency_time = ddr.get_last_detailed_dependency(gra=base_gra,
                                                                                data_partation_sign=dependency_sign)
                        if last_dependency_time is False or last_dependency_time != execution_date:
                            continue
                    else:
                        t2 = Granularity.getPreTime(exe_time, base_gra, -1)
                        if t2.strftime('%Y-%m-%d %H:%M:%S') != execution_date.strftime('%Y-%m-%d %H:%M:%S'):
                            continue
                else:
                    continue
            else:
                exe_time = Granularity.getNextTime(execution_date, ddr.granularity, ddr.offset)
            # 只生成当前任务本次上线之后的实例，历史实例跳过(本次上线实际时间取上一粒度的开始时间，防止当天任务无法启动)
            td = TaskDesc.get_task(task_name=ddr.dag_id)
            if td:
                tmp_cmp_date = Granularity.get_execution_date(td.last_update, ddr.granularity)
                cmp_date = Granularity.getPreTime(tmp_cmp_date, ddr.granularity, -1)

                if my_make_naive(cmp_date) >= my_make_naive(exe_time):
                    continue
                Trigger.sync_trigger_to_db(dag_id=ddr.dag_id, execution_date=exe_time, state=state,priority=5,
                                           trigger_type="pipeline", editor="DataPipeline")

    @staticmethod
    def get_all_dependency_dataset_status(dag_id, execution_date):
        """
        dag_id : 任务唯一标识
        execution_date：标识某一粒度的时间标识
        return: 上游依赖的列表[{metadata_id,granularity,sign}]
        """
        dep_list = []
        input_ddrs = DagDatasetRelation.get_inputs(dag_id=dag_id)
        for ddr in input_ddrs:
            depend_gra = ddr.granularity  # 天
            offset = ddr.offset
            metadata_id = ddr.metadata_id
            dataset_gra = DagDatasetRelation.get_dataset_granularity(metadata_id=metadata_id)  # 小时
            if dataset_gra == -1 or depend_gra == dataset_gra:
                tmp_dataset_time = Granularity.getPreTime(execution_date, depend_gra, offset)
                dep_list.append({"metadata_id": metadata_id, "granularity": depend_gra, "sign": tmp_dataset_time})
            elif dataset_gra > depend_gra:
                tmp_st_time = Granularity.getPreTime(execution_date, depend_gra, offset)
                tmp_ed_time = Granularity.getPreTime(tmp_st_time, depend_gra, 1)
                time_ranges = Granularity.splitTimeByGranularity(tmp_st_time, tmp_ed_time, dataset_gra)
                for exec_time in time_ranges:
                    dep_list.append({"metadata_id": metadata_id, "granularity": dataset_gra, "sign": exec_time})
            else:
                pass
        return dep_list

    @staticmethod
    def get_dataset_info(name):
        granularity = -1
        output = DagDatasetRelation.get_active_upstream_dags(metadata_id=name)
        if output:
            is_external = 0
            granularity = max(granularity, Granularity.getIntGra(output[0].granularity))
        else:
            is_external = 1
        from airflow.models.dag import DagModel

        check_paths = list()
        pattern = r"strftime\(\s*'([^']+)'\s*\)"
        execution_date_pattern = re.compile(r'{\s*{[^}]+}\s*}')
        ddrs = DagDatasetRelation.get_active_downstream_dags(metadata_id=name)
        for ddr in ddrs:
            if ddr.check_path:
                path = ddr.check_path
                execution_all = execution_date_pattern.findall(ddr.check_path)
                for item in execution_all:
                    match = re.search(pattern, item)
                    if match:
                        path = path.replace(item, '[[%s]]' % match.group(1))
                check_paths.append(path)
            if not DagModel.is_active_and_on(dag_id=ddr.dag_id):
                continue

        if granularity != -1:
            granularity = Granularity.formatGranularity(granularity)
        return {"is_external": is_external, "granularity": granularity, 'check_path': list(set(check_paths))}

    @staticmethod
    def is_output_exist(dag_id, metadata_id):
        res = DagDatasetRelation.check_output(dag_id=dag_id, metadata_id=metadata_id)
        if res:
            return True
        else:
            return False
