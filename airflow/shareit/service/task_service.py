# -*- coding: utf-8 -*-
"""
Author: zhangtao
CreateDate: 2021/4/25
"""
import copy
import datetime
import json
import logging
import multiprocessing
import traceback

import pendulum
import requests
import six
from concurrent.futures import ThreadPoolExecutor, as_completed

from airflow.configuration import conf
from croniter import croniter, croniter_range
from google.protobuf import json_format

from airflow.models.dag import DagModel
from airflow.models.taskinstance import TaskInstance
from airflow.shareit.constant.taskInstance import PIPELINE, CLEAR, BACKFILL
from airflow.shareit.constant.trigger import TRI_FORCE, TRI_CLEAR_BACKFILL, TRI_BACKFILL
from airflow.shareit.models.backfill_job import BackfillJob
from airflow.shareit.models.constants_map import ConstantsMap
from airflow.shareit.models.event_depends_info import EventDependsInfo
from airflow.shareit.models.event_trigger_mapping import EventTriggerMapping
from airflow.shareit.models.sensor_info import SensorInfo
from airflow.shareit.models.trigger import Trigger
from airflow.shareit.trigger.trigger_helper import TriggerHelper
from airflow.shareit.utils.alarm_adapter import build_normal_backfill_notify_content
from airflow.shareit.utils.cloud.cloud_utli import CloudUtil
from airflow.shareit.utils.constant import Constant
from airflow.shareit.utils.date_calculation_util import DateCalculationUtil
from airflow.shareit.utils.normal_util import NormalUtil
from airflow.shareit.utils.task_cron_util import TaskCronUtil
from airflow.shareit.utils.task_manager import from_dict, from_json
from airflow.shareit.utils.task_util import TaskUtil
from airflow.utils import timezone
from airflow.utils.db import provide_session
from airflow.utils.timezone import utcnow, beijing, make_naive
from airflow.utils.log.logging_mixin import LoggingMixin

from airflow.shareit.models.dag_dataset_relation import DagDatasetRelation
from airflow.shareit.models.dataset_partation_status import DatasetPartitionStatus
from airflow.shareit.models.task_desc import TaskDesc
from airflow.shareit.models.user_action import UserAction
from airflow.shareit.service.dataset_service import DatasetService
from airflow.shareit.utils.granularity import Granularity
from airflow.shareit.utils.state import State
from airflow.shareit.utils.task_manager import get_dag, get_dag_by_soure_json
from airflow.shareit.utils.date_util import DateUtil

log = LoggingMixin()


def run_class_fun(TaskService, task_name,ds_task_name):
    return TaskService.get_last_7_instance_status(task_name, ds_task_name)


def populate_trigger(dag_id, gra):
    TriggerHelper.populate_trigger(dag_id, gra)

class TaskService:
    def __init__(self):
        pass

    @staticmethod
    def _get_value_from_dict(dict_obj, key):
        if not isinstance(dict_obj, dict):
            return None
        return dict_obj[key] if key in dict_obj.keys() else None

    @staticmethod
    def proto_task_code_to_json(task_code):
        task_code_json = json_format.MessageToDict(task_code, including_default_value_fields=True)
        if task_code.extraParam != "":
            extra_param_dict = json.loads(task_code.extraParam)
            task_code_json['extra_params'] = extra_param_dict
        if task_code.taskItems != "":
            task_items_dict = json.loads(task_code.taskItems)
            task_code_json['task_items'] = task_items_dict
        print json.dumps(task_code_json)
        return task_code_json

    @staticmethod
    @provide_session
    def update_task(task_id, task_code, ds_task_name=None,ds_task_id=None,tenant_id=1,workflow_name=None, is_online=True, is_commit=True,is_temp=False, session=None):
        try:
            transaction = True
            task_code = TaskService.proto_task_code_to_json(task_code)
            if isinstance(task_code, unicode) or isinstance(task_code, str):
                task_code = json.loads(task_code)
            task_name = TaskService._get_value_from_dict(task_code, 'name')
            if task_name != task_id:
                task_name = task_id
            TaskService._new_task_pre_check(task_code)
            emails = TaskService._get_value_from_dict(task_code, 'emails')
            owner = TaskService._get_value_from_dict(task_code, 'owner')
            retries = TaskService._get_value_from_dict(task_code, 'retries')
            input_datasets = TaskService._get_value_from_dict(task_code, 'input_datasets')
            output_datasets = TaskService._get_value_from_dict(task_code, 'output_datasets')
            deadline = None
            property_weight = None
            extra_params = TaskService._get_value_from_dict(task_code, 'extra_params')
            task_items = TaskService._get_value_from_dict(task_code, 'task_items')
            start_date = TaskService._get_value_from_dict(task_code, 'start_date')
            end_date = TaskService._get_value_from_dict(task_code, 'end_date')
            event_depends = TaskService._get_value_from_dict(task_code, 'event_depend')
            trigger_param = TaskService._get_value_from_dict(task_code, 'trigger_param')
            depend_types = TaskService._get_value_from_dict(task_code, 'depend_types')
            task_version = TaskService._get_value_from_dict(task_code, 'version')
            user_group_info = TaskService._get_value_from_dict(task_code,'user_group_info')
            template_code = TaskService._get_value_from_dict(task_code, 'template_code')
            is_paused = None
            # 是否有数据依赖
            data_depend_flag = False
            # 是否有事件依赖
            event_depend_flag = False
            if 'dataset' in str(depend_types):
                data_depend_flag = True
            if 'event' in str(depend_types):
                event_depend_flag = True
            task_gra = Granularity.gra_priority_map[trigger_param['output_granularity']]
            # 任务的触发类型
            crontab = trigger_param['crontab'] if trigger_param['type'] == 'cron' else ""
            # 不定时调度任务
            is_irregular_sheduler = False
            if trigger_param['is_irregular_sheduler'] == 2:
                is_irregular_sheduler = True

            # 判断任务是否已经存在
            if not is_temp:
                task = TaskDesc.get_task_with_tenant(tenant_id=tenant_id,ds_task_id=ds_task_id,session=session)
            else:
                # 临时任务直接查ID
                task = TaskDesc.get_task(task_name=task_id, session=session)
            # task = TaskDesc.get_task_by_ds_task_id(ds_task_id=ds_task_id, session=session)
            if task:
                # 名字不同,要更新名字
                if task.task_name != task_name:
                    TaskService.update_name_transaction(task.task_name, task_name, transaction=transaction,
                                                        session=session)
                # 刷新一下数据库, 不然事务中后面的查询用新id是查不到东西的
                session.flush()
            else:
                task = TaskDesc()

            is_regular_alert = False
            if extra_params and extra_params.has_key("regularAlert"):
                is_regular_alert = True

            task.sync_task_desc_to_db(task_name=task_name,
                                      owner=owner,
                                      retries=retries,
                                      email=emails,
                                      start_date=start_date,
                                      end_date=end_date,
                                      input_datasets=input_datasets,
                                      output_datasets=output_datasets,
                                      deadline=None,
                                      property_weight=property_weight,
                                      extra_param=extra_params,
                                      tasks=task_items,
                                      source_json=task_code,
                                      crontab= '0 0 0 0 0' if is_temp or is_irregular_sheduler else crontab,
                                      granularity=task_gra,
                                      task_version=task_version,
                                      workflow_name=workflow_name,
                                      is_paused=is_paused,
                                      is_temp=is_temp,
                                      ds_task_id=ds_task_id,
                                      ds_task_name=ds_task_name,
                                      tenant_id=tenant_id,
                                      is_regular_alert=is_regular_alert,
                                      user_group_info=user_group_info,
                                      template_code=template_code,
                                      session=session
                                      )

            in_ids = DatasetService.get_all_input_datasets(dag_id=task.task_name,session=session)
            out_ids = DatasetService.get_all_output_datasets(dag_id=task.task_name,session=session)

            if is_temp or is_irregular_sheduler:
                temp_ts = DateUtil.get_timestamp(timezone.utcnow().replace(tzinfo=beijing))
                EventDependsInfo.delete(dag_id=task_name, transaction=transaction, session=session)
                if in_ids:
                    DatasetService.delete_ddr(dag_id=task_name, metadata_ids=in_ids, dataset_type=True,transaction=transaction,session=session)
                # 临时任务只插入产出数据集的信息
                for output_data in task.get_output_datasets():
                    offs = output_data['offset'] if 'offset' in output_data.keys() else None
                    DatasetService.update_task_dataset_relation(dag_id=task_name,
                                                                metadata_id=output_data['id'] + str(temp_ts),
                                                                dataset_type=False,
                                                                granularity=task_gra,
                                                                offset=offs,
                                                                ready_time=None,
                                                                date_calculation_param='',
                                                                session=session)
            else:
                # update task_dataset_relation to db
                if data_depend_flag:

                    from airflow.shareit.models.trigger import Trigger

                    tris = Trigger.get_untreated_triggers(dag_id=task_name,session=session)
                    execution_dates = [tri.execution_date for tri in tris if
                                       DateUtil.date_compare(timezone.utcnow().replace(tzinfo=beijing),
                                                             tri.execution_date)]

                    for exe_date in execution_dates:
                        SensorInfo.delete_by_execution_date(task_name=task_name, execution_date=exe_date,transaction=transaction,session=session)

                    for input_data in task.get_input_datasets():
                        check_path = input_data.get('check_path',None)
                        ready_time = input_data.get('ready_time',None)
                        if input_data['id'] in in_ids:
                            in_ids.remove(input_data['id'])
                        format_gra = Granularity.formatGranularity(input_data['granularity'])
                        if format_gra:
                            granularity = Granularity.gra_priority_map[format_gra]
                        else:
                            raise ValueError("[DataPipeline] illegal granularity in input dataset " + input_data['id'])
                        detailed_dependency = input_data[
                            "detailed_dependency"] if "detailed_dependency" in input_data else None
                        if detailed_dependency == "":
                            detailed_dependency = None
                        detailed_gra = Granularity.getIntGra(
                            input_data["detailed_gra"]) if "detailed_gra" in input_data else None
                        if detailed_gra is False:
                            detailed_gra = None
                        # 将偏移量也转换为date_calculation_param格式, 统一时间计算
                        if input_data['use_date_calcu_param'] == False:
                            date_calculation_param = DateCalculationUtil.offset_2_data_calculation_param(granularity,
                                                                                                         input_data[
                                                                                                             'unitOffset'])
                        else:
                            date_calculation_param = json.dumps(input_data['date_calculation_param'])
                        DatasetService.update_task_dataset_relation(dag_id=task_name,
                                                                    metadata_id=input_data['id'],
                                                                    dataset_type=True,
                                                                    granularity=granularity,
                                                                    offset=input_data['offset'],
                                                                    ready_time=ready_time,
                                                                    check_path=check_path,
                                                                    detailed_dependency=detailed_dependency,
                                                                    detailed_gra=detailed_gra,
                                                                    date_calculation_param=date_calculation_param,
                                                                    transaction=transaction,
                                                                    session=session
                                                                    )
                        # 将sensor更新为最新状态.如果任务由check_path改为ready_time,check_path还在,但路径为空
                        if check_path and check_path.strip():
                            for exe_date in execution_dates:
                                render_res = TaskUtil.render_datatime(content=check_path,
                                                                      execution_date=exe_date,
                                                                      dag_id=task_name,
                                                                      date_calculation_param=date_calculation_param,
                                                                      gra=granularity)
                                for render_re in render_res:
                                    SensorInfo.register_sensor(dag_id=task_name, metadata_id=input_data['id'],
                                                               execution_date=exe_date,
                                                               detailed_execution_date=render_re["date"],
                                                               granularity=granularity, check_path=render_re["path"],
                                                               cluster_tags=task_items[0]["cluster_tags"],
                                                               transaction=transaction, session=session)
                        else:
                            for exe_date in execution_dates:
                                data_dates = DateCalculationUtil.get_upstream_all_data_dates(execution_date=exe_date,
                                                                                             calculation_param=date_calculation_param,
                                                                                             gra=granularity)

                                completion_time = DateUtil.date_format_ready_time_copy(exe_date,ready_time,granularity)
                                SensorInfo.register_sensor(dag_id=task_name, metadata_id=input_data['id'],
                                                           execution_date=exe_date,
                                                           detailed_execution_date=data_dates[0],
                                                           granularity=granularity,
                                                           cluster_tags=task_items[0]["cluster_tags"],
                                                           completion_time=completion_time, transaction=transaction,
                                                           session=session
                                                           )


                for output_data in task.get_output_datasets():
                    if output_data['id'] in out_ids:
                        out_ids.remove(output_data['id'])
                    # update output dataset to db
                    offs = output_data['offset'] if 'offset' in output_data.keys() else None
                    # Dataset.sync_ds_to_db(metadata_id=output_data['id'], name=output_data['id'],
                    #                       dataset_type=None, granularity=task_gra, offset=offs)
                    DatasetService.update_task_dataset_relation(dag_id=task_name,
                                                                metadata_id=output_data['id'],
                                                                dataset_type=False,
                                                                granularity=task_gra,
                                                                offset=offs,
                                                                ready_time=None,
                                                                date_calculation_param='',
                                                                transaction=transaction,
                                                                session=session)
                if in_ids:
                    DatasetService.delete_ddr(dag_id=task_name, metadata_ids=in_ids, dataset_type=True,transaction=transaction,session=session)
                if out_ids:
                    DatasetService.delete_ddr(dag_id=task_name, metadata_ids=out_ids, dataset_type=False,transaction=transaction,session=session)

                # 事件依赖先删除
                EventDependsInfo.delete(dag_id=task_name, transaction=transaction, session=session)
                if event_depend_flag:
                    # 再添加
                    for e_depend in event_depends:
                        depend_info = EventDependsInfo()
                        granularity = Granularity.gra_priority_map[e_depend['granularity']]
                        # 将偏移量也转换为date_calculation_param格式, 统一时间计算
                        if e_depend['use_date_calcu_param'] == False:
                            date_calculation_param = DateCalculationUtil.offset_2_data_calculation_param(granularity,
                                                                                                         e_depend[
                                                                                                             'unitOffset'])
                        else:
                            date_calculation_param = json.dumps(e_depend['date_calculation_param'])

                        depend_info.sync_event_depend_info_to_db(
                            dag_id=task_name,
                            date_calculation_param=date_calculation_param,
                            depend_id=e_depend['depend_id'],
                            depend_gra=e_depend['granularity'],
                            tenant_id=tenant_id,
                            depend_ds_task_id=e_depend['task_id'],
                            transaction=transaction,
                            session=session
                        )

                if trigger_param['type'] == 'cron':
                    from airflow.shareit.models.trigger import Trigger
                    now_date = timezone.utcnow().replace(tzinfo=beijing)
                    # 存在一种情况，任务之前是数据触发，提前生成了trigger，修改为时间触发后，两个调度时间对不上，所以在这里删除所有比当前时间晚的trigger
                    Trigger.delete_by_execution_date_range(task_name=task_name, start_date=make_naive(now_date),transaction=transaction,session=session)
                    # 如果是时间触发的任务,生成最近一次日期的trigger
                    # 这里的目的是为了防止一个任务下线太久之后重新上线,导致中间所有的日期都被调起
                    TriggerHelper.generate_latest_cron_trigger(dag_id=task_name, crontab=crontab, transaction=transaction,
                                                               session=session)

                else:
                    # 每次上线都默认生成上一次的trigger
                    TriggerHelper.generate_latest_data_trigger(dag_id=task_name, gra=task_gra, transaction=transaction,
                                                               session=session)
                    # 重新打开未来日期终止状态的trigger
                    TriggerHelper.reopen_future_trigger(task_name=task_name, transaction=transaction, session=session)

            session.commit()
            if is_online:
                from airflow.models import DagModel
                dag = get_dag_by_soure_json(task_code,task_name)
                # dag = get_dag(task_name)
                dag.sync_to_db_transaction(is_online=is_online,transaction=transaction, session=session)
                # dm = DagModel.get_dagmodel(task_name, session=session)
                # if dm:
                #     dm.is_paused = False
                #     session.merge(dm)
            session.commit()
        except Exception as e:
            session.rollback()
            s = traceback.format_exc()
            logging.error(s)
            log.log.error(s)
            raise e
        return task

    @staticmethod
    def test_trigger(task_name,task_gra):
        # 填充未来的实例，起单独的子进程执行
        proccess = multiprocessing.Process(
            target=populate_trigger,
            args=(
                task_name,
                task_gra
            ),
            name="populateTrigger-Process"
        )
        proccess.start()

    @classmethod
    def _new_task_pre_check(cls, task_code):
        trigger_param = TaskService._get_value_from_dict(task_code, 'trigger_param')
        depend_types = TaskService._get_value_from_dict(task_code, 'depend_types')
        if trigger_param is None:
            raise ValueError("trigger_param不允许为空!!")

        if depend_types is None:
            raise ValueError("depend_types不允许为空!!")

    @staticmethod
    @provide_session
    def delete_task(task_id,session=None):
        try:
            TaskService.delete_task_transaction(task_name=task_id,transaction=True,session=session)
            session.commit()
        except Exception as e:
            logging.error("[DataStudio pipeline] delete_task error ")
            logging.error(traceback.format_exc())
            session.rollback()
            raise e


    @staticmethod
    def delete_task_transaction(task_name,transaction=False,session=None):
        from airflow.api.common.experimental import delete_dag
        task = TaskDesc.get_task(task_name=task_name,session=session)
        if not task:
            return
            # raise ValueError("Can not get task :{task_id} in db !".format(task_id=task_name))
        task.delete_task(transaction=transaction,session=session)
        DatasetService.delete_ddr(transaction=transaction,dag_id=task_name,session=session)
        TaskService.dagrun_delete(dag_id=task_name,session=session,transaction=transaction)
        delete_dag.delete_dag_new_transaction(task_name,transaction=transaction,session=session)
        EventDependsInfo.delete(task_name,transaction=transaction,session=session)
        Trigger.delete(task_name,transaction=transaction,session=session)


    # backfill
    # 对每一个task 在对应时间段内，检查每一个粒度
    # 每一个task的每一个output 对应的粒度的数据状态设置为backfill
    # 检查每一个粒度中是否有任务，有则clear ，没有就创建任务，生成trigger
    @staticmethod
    @provide_session
    def backfill_transaction(task_name_list,execution_date_list, is_send_notify=False, is_batch_clear=False,operator=None,transaction=False,session=None):
        """
        Args:
            start_date:
            end_date:
            is_send_notify: 发送通知
            execution_date_list: 与start_date/end_date不同, 这个参数代表具体的实例日期集合,不需要再根据时间范围去获取实例日期. 与start_date/end_date互斥.
        """
        from airflow.models.taskinstance import TaskInstance
        try:
            trigger_list = []

            if execution_date_list is None or len(execution_date_list) == 0:
                return

            if is_batch_clear:
                trigger_type = "clear_backCalculation"
            else:
                trigger_type = "backCalculation"

            for task_name in task_name_list:
                # task = TaskDesc.get_task(task_name=task_name, session=session)
                tis = TaskInstance.find_by_execution_dates(dag_id=task_name, execution_dates=execution_date_list,
                                                           state=State.RUNNING, session=session)
                # kill掉正在运行的任务
                TaskInstance.clear_task_instance_list(tis, target_state=State.FAILED, transaction=transaction,
                                                      session=session)
                for execution_date in execution_date_list:
                    if DateUtil.date_compare(execution_date, utcnow()):
                        continue
                    # 生成类型为backfill 的trigger
                    trigger_list.append((task_name, execution_date))
                    # Trigger.sync_trigger_to_db(dag_id=task_id, execution_date=stime, state=State.WAITING,
                    #                            trigger_type="backCalculation", editor="DataPipeline")

            for item in trigger_list:
                Trigger.sync_trigger_to_db(dag_id=item[0], execution_date=item[1], state=State.BF_WAIT_DEP, priority=5,
                                           trigger_type=trigger_type, editor="DataPipeline", transaction=transaction,
                                           session=session)
            notify_type = conf.get('email', 'notify_type')
            if is_send_notify and "dingding" in notify_type:
                """{"task_owner1":{"task_name":["BBB","CCC"],
                                   "upstream_task":["AAA"],
                                   "connector":["zhangSan"]},
                    "task_owner2":{"task_name":["DDDD"],
                                   "upstream_task":[""],
                                   "connector":["zhangSan","liSi","luoXiang"]}
                    }
                """
                res = {}
                from airflow.models.dag import DagModel
                from airflow.shareit.utils.alarm_adapter import build_backfill_notify_content, send_ding_notify
                # 所有直接下游（不包含task_ids里面的）
                proccessed_dags = copy.deepcopy(task_name_list)
                for task_name in task_name_list:
                    td = TaskDesc.get_task(task_name=task_name,session=session)
                    # cdm = DagModel.get_dagmodel(dag_id=task_name, session=session)
                    connector = td.owner.split(",") if td.owner else []
                    downstream_dags = EventDependsInfo.get_downstream_event_depends(ds_task_id=td, tenant_id=td.tenant_id,session=session)
                    for ds_dag in downstream_dags:
                        if ds_dag.dag_id in proccessed_dags:
                            continue
                        else:
                            proccessed_dags.append(ds_dag.dag_id)
                        dm = DagModel.get_dagmodel(dag_id=ds_dag.dag_id, session=session)
                        if dm.is_active == True and dm.is_paused == False:
                            owners = dm.owners.split(",") if dm.owners else []
                            for owner in owners:
                                if owner in res:
                                    if ds_dag.dag_id not in res[owner]["task_name"]:
                                        res[owner]["task_name"].append(ds_dag.dag_id)
                                    if task_name not in res[owner]["upstream_task"]:
                                        res[owner]["upstream_task"].append(task_name)
                                    for c_name in connector:
                                        if c_name not in res[owner]["connector"]:
                                            res[owner]["connector"].append(c_name)
                                else:
                                    res[owner] = {"task_name": [ds_dag.dag_id],
                                                  "upstream_task": [task_name],
                                                  "connector": connector}
                # 所有直接下游任务的owner
                # 发送消息
                for name in res:
                    try:
                        content = build_normal_backfill_notify_content(task_names=res[name]["task_name"],
                                                                       upstream_tasks=res[name]["upstream_task"],
                                                                       execution_date_list=execution_date_list,
                                                                       connectors=res[name]["connector"],
                                                                       operator=operator)
                        send_ding_notify(title="DataCake 提示信息", content=content, receiver=[name])
                    except Exception as e:
                        logging.error(traceback.format_exc())
                        logging.error('[DataStudio TaskService] send ding failed')
                        continue
            if not transaction:
                session.commit()
        except Exception as e:
            logging.error(traceback.format_exc())
            logging.error('[DataStudio TaskService] normal backfill failed')
            raise e
        # warning_msg = ""
        # if len(failed_list) > 0:
        #     message = "回算下列任务失败: "
        #     for failed_m in failed_list:
        #         tmp = failed_m["task_id"] + ": " + failed_m["reason"] + "   ; "
        #         message += tmp
        #     warning_msg += message
        # if len(warning_list) > 0:
        #     state_message = "上游依赖数据状态缺失，请检查: " + "  ".join(warning_list)
        #     warning_msg += state_message
        # if len(skip_list) > 0:
        #     state_message = "下列任务被跳过: " + "  ".join(warning_list)
        #     warning_msg += state_message
        # if warning_msg != "":
        #     raise ValueError(warning_msg)

    @classmethod
    @provide_session
    def backfill(cls,sub_task_name_list=None, core_task_name=None, start_date=None, end_date=None,
                               is_send_notify=False, ignore_check=False, operator=None,label=None, session=None):
        if core_task_name is None or core_task_name == "":
            raise ValueError("missing core_task_name")
        core_task = TaskDesc.get_task(task_name=core_task_name, session=session)
        if not core_task:
            raise ValueError("{} 任务不存在！！".format(core_task_name))

        if not DagModel.is_active_and_on(core_task_name):
            raise ValueError("{} 任务没有上线！！".format(core_task_name))

        if int(core_task.granularity) > 4:
            # 日期范围太大的天、小时、分钟周期的补数要切分成多个job，防止一个补数job长时间无法开启。
            # 目前是按照30天一个窗口切分
            date_range = DateUtil.split_date_range(start_date, end_date)
            for start, end in date_range:
                BackfillJob.add(core_task_name=core_task_name, sub_task_names=','.join(sub_task_name_list),
                                start_date=start, end_date=end,
                                operator=operator, ignore_check=ignore_check,
                                is_notify=is_send_notify,label=label,session=session)
        else:
            BackfillJob.add(core_task_name=core_task_name, sub_task_names=','.join(sub_task_name_list),
                            start_date=start_date, end_date=end_date,
                            operator=operator, ignore_check=ignore_check,
                            is_notify=is_send_notify,label=label,session=session)



    @classmethod
    @provide_session
    def backfill_with_relation(cls, task_ids=None, core_task_name=None, start_date=None, end_date=None,
                               is_send_notify=False, ignore_check=False, operator=None, session=None):
        """
        (已经弃用)
        执行回算任务，根据核心任务计算出对应下游实例进行回算
        """
        from airflow.shareit.workflow.workflow_state_sync import WorkflowStateSync
        if core_task_name is None or core_task_name == "":
            raise ValueError("missing core_task_name")
        instance_list = []
        trigger_list = []
        failed_list = []
        task_desc_dict = {}
        task_set = set(task_ids)
        task_set_copy = task_set.copy()

        trigger_type = TRI_BACKFILL
        # 获取核心任务需要回算的实例
        times = None
        core_task = TaskDesc.get_task(task_name=core_task_name,session=session)
        if core_task:
            if core_task.crontab and core_task.crontab != '0 0 0 0 0':
                task_desc_dict[core_task_name] = core_task
                times = croniter_range(start_date, end_date, core_task.crontab)
            else:
                gra = core_task.granularity
                times = Granularity.splitTimeByGranularity(start_time=start_date, end_time=end_date, gra=gra)
        else:
            raise ValueError("{} 任务不存在！！".format(core_task_name))

        for t in task_set_copy:
            td = TaskDesc.get_task(task_name=t,session=session)
            if td:
                task_desc_dict[t] = td
            else:
                # 任务不存在就移除
                task_set.remove(t)
        task_set.add(core_task_name)

        if not times:
            raise ValueError("{task} {meta}:{et} ".format(task=core_task_name,
                                                          meta="任务启动失败",
                                                          et="输入的时间段应至少包含一个运行周期/或核心任务未上线"))
        for exe_time in times:
            if exe_time.strftime('%Y%m%d %H%M%S') > utcnow().strftime('%Y%m%d %H%M%S'):
                continue
            instance_list.append({"dag_id": core_task_name, "execution_date": exe_time})
            work_queue = [core_task_name]
            processed_set = set()
            processed_set.add(core_task_name)
            while len(work_queue) > 0:
                task = work_queue.pop(0)
                res = cls._get_related_task_instance(core_task=task, execution_date=exe_time)
                for ins in res:
                    # if ins["execution_date"] > utcnow():
                    #     continue
                    if ins["dag_id"] in task_set and ins["dag_id"] not in processed_set:
                        instance_list.append(ins)
                        processed_set.add(ins["dag_id"])
                        work_queue.append(ins["dag_id"])
                if len(processed_set) >= len(task_set):
                    break
        # 去重
        instance_list = [dict(t) for t in set([tuple(sorted(d.items())) for d in instance_list])]
        # 事务的
        try:
            # 处理上游依赖数据、下游生成数据状态
            for ins in instance_list:
                if ins["execution_date"].strftime('%Y%m%d %H%M%S') > utcnow().strftime('%Y%m%d %H%M%S'):
                    continue
                # 将实例都记为失败，也是为了kill掉正在补数的任务
                TaskService._set_task_instance_state(dag_id=ins["dag_id"], execution_date=ins["execution_date"],
                                                     state=State.FAILED, transaction=True, session=session)
                # 生成类型为backfill 的trigger
                trigger_list.append((ins["dag_id"], ins["execution_date"]))
                if task_desc_dict[ins['dag_id']].workflow_name:
                    # 打开任务归属的workflow
                    WorkflowStateSync.open_wi(task_desc_dict[ins['dag_id']].workflow_name, ins["execution_date"],
                                              transaction=True, session=session)

            instance_list = []
            for item in trigger_list:
                # 只有root节点可以忽略检查上游，否则深度补数没有意义
                ignore = False
                if item[0] == core_task_name:
                    ignore = ignore_check
                Trigger.sync_trigger_to_db(dag_id=item[0], execution_date=item[1], state=State.BF_WAIT_DEP, priority=5,
                                           trigger_type=trigger_type, editor="DataPipeline_backfill", is_executed=True,
                                           start_check_time=timezone.utcnow(),ignore_check=ignore,transaction=True,session=session)
                instance_list.append((item[0], item[1].strftime("%Y-%m-%dT%H")))
            session.commit()
        except Exception:
            logging.error(traceback.format_exc())
            session.rollback()
            raise
        # 将相关信息存入user_action表待用
        details = {"owners": core_task.owner, "instance_list": instance_list}
        user_action = UserAction.add_user_action(user_id=operator,
                                                 action_type="backfill_with_relation",
                                                 details=details)
        # notify_type = conf.get('email', 'notify_type')
        # if is_send_notify and "dingding" in notify_type:
        #     """{"task_owner1":{"task_name":["BBB","CCC"],
        #                        "upstream_task":["AAA"],
        #                        "connector":["zhangSan"]},
        #         "task_owner2":{"task_name":["DDDD"],
        #                        "upstream_task":[""],
        #                        "connector":["zhangSan","liSi","luoXiang"]}
        #         }
        #     """
        #     res = {}
        #     from airflow.models.dag import DagModel
        #     from airflow.shareit.utils.alarm_adapter import build_backfill_notify_content, send_ding_notify
        #     # 所有直接下游（不包含task_ids里面的）
        #     proccessed_dags = copy.deepcopy(task_ids)
        #     for task_id in task_ids:
        #         owner = task_desc_dict[task_id].owner
        #         connector = owner.split(",") if owner else []
        #         downstream_dags = DagDatasetRelation.get_downstream_dags_by_dag_id(dag_id=task_id)
        #         for ds_dag in downstream_dags:
        #             if ds_dag.dag_id in proccessed_dags:
        #                 continue
        #             else:
        #                 proccessed_dags.append(ds_dag.dag_id)
        #             dm = DagModel.get_dagmodel(dag_id=ds_dag.dag_id)
        #             owners = dm.owners.split(",") if dm.owners else []
        #             for owner in owners:
        #                 if owner in res:
        #                     if ds_dag.dag_id not in res[owner]["task_name"]:
        #                         res[owner]["task_name"].append(ds_dag.dag_id)
        #                     if task_id not in res[owner]["upstream_task"]:
        #                         res[owner]["upstream_task"].append(task_id)
        #                     for c_name in connector:
        #                         if c_name not in res[owner]["connector"]:
        #                             res[owner]["connector"].append(c_name)
        #                 else:
        #                     res[owner] = {"task_name": [ds_dag.dag_id],
        #                                   "upstream_task": [task_id],
        #                                   "connector": connector}
        #     # 所有直接下游任务的owner
        #     # 发送消息
        #     for name in res:
        #         try:
        #             content = build_backfill_notify_content(task_names=res[name]["task_name"],
        #                                                     upstream_tasks=res[name]["upstream_task"],
        #                                                     start_date=start_date,
        #                                                     end_date=end_date,
        #                                                     connectors=res[name]["connector"],operator=operator)
        #             send_ding_notify(title="DataCake 提示信息", content=content, receiver=[name])
        #         except Exception as e:
        #             logging.error(traceback.format_exc())
        #             continue
        #
        # # 发送进度信息
        # try:
        #     cls.send_backfill_progress_reminder_handle(operator=operator,
        #                                                core_task=core_task_name,
        #                                                start_date=start_date,
        #                                                end_date=end_date,
        #                                                receiver=core_task.owner,
        #                                                user_action_id=user_action.id,
        #                                                email_receiver=core_task.email)
        # except Exception as e:
        #     logging.error(traceback.format_exc())
        # warning_msg = ""
        # if len(failed_list) > 0:
        #     message = "回算下列任务失败: "
        #     for failed_m in failed_list:
        #         tmp = failed_m["task_id"] + ": " + failed_m["reason"] + "   ; "
        #         message += tmp
        # if warning_msg != "":
        #     raise ValueError(warning_msg)


    @staticmethod
    @provide_session
    def _set_dag_run_state(dag_id, execution_date, state, session=None):
        from airflow.models.dagrun import DagRun
        from airflow.utils import timezone
        dag_run = session.query(DagRun).filter(
            DagRun.dag_id == dag_id,
            DagRun.execution_date == execution_date
        ).one()
        dag_run.state = state
        if state == State.RUNNING:
            dag_run.start_date = timezone.utcnow()
            dag_run.end_date = None
        elif dag_run.end_date is None:
            dag_run.end_date = timezone.utcnow()

        # 执行成功后将dagrun补数标记的run_id替换为正常run_id
        if dag_run.run_id.startswith("backCalculation"):
            dag_run.run_id = dag_run.run_id.replace("backCalculation", "pipeline")

        session.merge(dag_run)
        return dag_run

    @staticmethod
    @provide_session
    def _set_task_instance_state(dag_id, execution_date, state, transaction=False, session=None):
        from airflow.models.taskinstance import TaskInstance
        from airflow.utils import timezone
        tis = session.query(TaskInstance).filter(
            TaskInstance.dag_id == dag_id,
            TaskInstance.execution_date == execution_date
        ).with_for_update().all()
        for ti in tis:
            ti.state = state
            if state == State.RUNNING:
                ti.start_date = timezone.utcnow()
                ti.end_date = None
            elif ti.end_date is None:
                ti.end_date = timezone.utcnow()
        if not transaction:
            session.commit()

    @staticmethod
    def generate_success_file(task_name, execution_date):
        from airflow.shareit.models.task_desc import TaskDesc
        import re
        from airflow import macros
        context = {}
        context["execution_date"] = execution_date
        context["macros"] = macros
        td = TaskDesc()
        ts = TaskService()
        file_name = td.get_success_file(task_name=task_name)
        if file_name:
            file_name = ts.render_api(file_name, context, task_name=task_name)
            print file_name
            search = re.search(r'(obs|s3|gs)://([^\/]*)/(.*)/(.*)', file_name, re.M | re.I)
            try:
                tenant_id, cluster_region, provider = TaskDesc.get_cluster_manager_info(task_name=task_name)
                cloud = CloudUtil(provider=provider, region=cluster_region, tenant_id=tenant_id)
                # cloud = CloudUtil()
                cloud.upload_empty_file_to_cloud(file_name, search.group(4))
            except :
                raise ValueError("上传成功文件失败")
            # if search.group(1) == "obs":
            #     from airflow.shareit.utils.obs_util import Obs
            #     obs = Obs()
            #     obs.upload_file_obs(search.group(2), search.group(3) + "/" + search.group(4), search.group(4))
            # elif search.group(1) == "s3":
            #     from airflow.shareit.utils.s3_util import S3
            #     s3 = S3()
            #     s3.upload_file_s3(search.group(2), search.group(3) + "/" + search.group(4), search.group(4))
            # elif search.group(1) == 'gs':
            #     from airflow.shareit.utils.gs_util import GCS
            #     GCS().upload_file_gs(search.group(2), search.group(4), search.group(3) + "/" + search.group(4))

    @staticmethod
    @provide_session
    def set_success(dag_id, execution_date, session=None):
        from airflow.models.taskinstance import TaskInstance
        from airflow.shareit.utils.task_manager import get_dag
        try:
            task_instance_list = TaskInstance.find_by_execution_dates(dag_id=dag_id,
                                                                      execution_dates=[execution_date])
            now_date = utcnow().replace(tzinfo=timezone.beijing)
            ti = None
            # set trigger
            Trigger.sync_trigger_to_db(dag_id=dag_id, execution_date=execution_date,
                               trigger_type=None, transaction=False, state=State.SUCCESS, session=session)
            if len(task_instance_list) == 0:
                dag = get_dag(dag_id)
                for task in six.itervalues(dag.task_dict):
                    ti = TaskInstance(task, execution_date, state=State.SUCCESS, task_type=PIPELINE,start_date=now_date,end_date=now_date)
                    session.merge(ti)
            else :
                TaskInstance.clear_task_instance_list(tis=task_instance_list, target_state=State.SUCCESS,update_label=False, transaction=False,session=session)
                for t in task_instance_list:
                    if str(t.task_id).endswith('_import_sharestore'):
                        continue
                    ti = t

            TriggerHelper.task_finish_trigger(ti)

            ts = TaskService()
            ts.generate_success_file(task_name=dag_id, execution_date=execution_date)

            session.commit()
        except Exception as e:
            session.rollback()
            logging.error("[DataStudio pipeline] set successed error ")
            logging.error(traceback.format_exc())
            raise e

    @staticmethod
    @provide_session
    def set_failed(dag_id, execution_date, session=None):
        transaction=False
        try:
            TaskService.set_failed_transaction(dag_id,execution_date,transaction=transaction,session=session)
            session.commit()
        except Exception as e:
            # session.rollback()
            logging.error("[DataStudio pipeline] set failed error " + str(e))
            raise e


    @staticmethod
    @provide_session
    def set_failed_transaction(dag_id, execution_date,transaction=False, session=None):
        from airflow.models.taskinstance import TaskInstance
        try:
            now_date = utcnow().replace(tzinfo=timezone.beijing)
            # set trigger
            Trigger.sync_trigger_to_db(dag_id=dag_id, execution_date=execution_date,
                                       trigger_type=None, transaction=transaction, state=State.FAILED,session=session)
            # 检查是否是存在taskInstance, 不存在说明是对waiting状态的trigger进行终止
            task_instance_list = TaskInstance.find_by_execution_dates(dag_id=dag_id,
                                                                      execution_dates=[execution_date],session=session)
            ti = None
            if len(task_instance_list) == 0:
                dag = get_dag(dag_id)
                for task in six.itervalues(dag.task_dict):
                    ti = TaskInstance(task, execution_date, state=State.FAILED, task_type=PIPELINE,start_date=now_date,end_date=now_date)
                    session.merge(ti)
                    session.commit()

            else:
                TaskInstance.clear_task_instance_list(tis=task_instance_list, target_state=State.FAILED,update_label=False,
                                                      transaction=transaction, session=session)
                for t in task_instance_list:
                    if str(t.task_id).endswith('_import_sharestore'):
                        continue
                    ti = t
            TriggerHelper.task_finish_trigger(ti)
        except Exception as e:
            logging.error(traceback.format_exc())
            raise e

    @staticmethod
    @provide_session
    def clear_instance(dag_id,execution_dates,ignore_check=None,session=None):
        transaction = True
        try:
            TaskService.clear_instance_transaction(dag_id,execution_dates,ignore_check=ignore_check,transaction=transaction,session=session)
            session.commit()
        except Exception :
            session.rollback()
            logging.error("[DataStudio pipeline] clear instance error ")


    @staticmethod
    def clear_instance_transaction(dag_id,execution_dates,ignore_check=False,transaction=False,session=None):
        """
        Args:
            execution_dates: 日期集合,list格式
        """
        from airflow.models.taskinstance import TaskInstance


        for execution_date in execution_dates:
            try:
                Trigger.sync_state(dag_id=dag_id, execution_date=execution_date,ignore_check=ignore_check,
                                   trigger_type=TRI_CLEAR_BACKFILL, state=State.BF_WAIT_DEP, priority=5,
                                   start_check_time=timezone.utcnow(), clear_label=True,transaction=transaction, session=session)
                # 检查是否是存在taskInstance, 不存在说明是对终止状态的trigger进行恢复,所以更新trigger的状态
                task_instance_list = TaskInstance.find_by_execution_dates(dag_id=dag_id, task_id=dag_id,
                                                                          execution_dates=[execution_date],session=session)
                if task_instance_list and len(task_instance_list) > 0:
                    TaskInstance.clear_task_instance_list(task_instance_list,task_type=CLEAR,target_state=State.FAILED,transaction=transaction,session=session)

            except Exception as e:
                logging.error(traceback.format_exc())
                logging.error("[DataStudio pipeline] clear failed " + str(e))
                raise e


        # 如果需要检查上游,则转为backfill任务
        # if len(clear_ti_execution_dates) > 0:
        #     TaskService.backfill(task_ids=[dag_id], execution_date_list=clear_ti_execution_dates, is_batch_clear=True)
        #     count = count + len(clear_ti_execution_dates)

    @staticmethod
    @provide_session
    def get_specified_dag_run_info(name=None, page=None, size=None, state=None, start_date=None, end_date=None,
                                   session=None):
        from airflow.models.dagrun import DagRun
        qry = session.query(DagRun).filter(DagRun.dag_id == name)
        if start_date:
            qry = qry.filter(DagRun.execution_date >= start_date)
        if end_date:
            qry = qry.filter(DagRun.execution_date <= end_date)
        if state:
            qry = qry.filter(DagRun.state == state)
        count = qry.count()
        drs = qry.order_by(DagRun.execution_date.desc()).limit(size).offset((page - 1) * size).all()
        task_info = []
        for dr in drs:
            trigger_type = None
            if dr.run_id.startswith("pipeline"):
                trigger_type = "pipeline"
            elif dr.run_id.startswith("manual"):
                trigger_type = "manual"
            elif dr.run_id.startswith("backCalculation"):
                trigger_type = "backCalculation"
            if dr.end_date:
                t_sec = (dr.end_date - dr.start_date).total_seconds()
                sec = int(t_sec % 60)
                t_mi = t_sec // 60
                mi = int(t_mi % 60)
                hou = int(t_mi // 60)
                duration = ""
                if hou > 0:
                    duration += "{} Hours ".format(hou)
                if mi > 0:
                    duration += "{} Min ".format(mi)
                if sec > 0:
                    duration += "{} Sec".format(sec)
            else:
                duration = "None"
            genie_job_id = ""
            tis = dr.get_task_instances()
            for ti in tis:
                if ti.task_id == dr.dag_id:
                    ex_conf = ti.get_external_conf()
                    if ex_conf:
                        genie_job_id = ex_conf["genie_job_id"] if "genie_job_id" in ex_conf.keys() else ""
            end_date = dr.end_date.strftime("%Y-%m-%dT%H:%M:%S.%f") if dr.end_date else ""
            tmp_info = {"name": dr.dag_id,
                        "execution_date": dr.execution_date.strftime("%Y-%m-%dT%H:%M:%S.%f"),
                        "start_date": dr.start_date.strftime("%Y-%m-%dT%H:%M:%S.%f"),
                        "end_date": end_date,
                        "state": dr.state,
                        "type": trigger_type,
                        "duration": duration,
                        "genie_job_id": genie_job_id}
            task_info.append(tmp_info)
        return {"count": count,
                "task_info": task_info}

    @staticmethod
    @provide_session
    def get_last_instance_info(name_tuple_list=None, session=None):
        from airflow.models.dag import DagModel
        res_list = []
        from airflow.models.dagrun import DagRun
        for name_tuple in name_tuple_list:
            dagrun = session.query(DagRun).filter(DagRun.dag_id == name_tuple[0], DagRun.execution_date <= utcnow()) \
                .order_by(DagRun.execution_date.desc()).first()
            if dagrun:
                # 毫秒时间戳
                fState = dagrun.get_state() if DagModel.is_active_and_on(dag_id=name_tuple[0]) else "offline"
                end_date = int((dagrun.end_date
                                - datetime.datetime(1970, 1, 1, tzinfo=pendulum.timezone('UTC')))
                               .total_seconds()) * 1000 if dagrun.end_date else None
                start_date = int((dagrun.start_date
                                  - datetime.datetime(1970, 1, 1, tzinfo=pendulum.timezone('UTC')))
                                 .total_seconds()) * 1000 if dagrun.start_date else None
                exe_date = int((dagrun.execution_date
                                - datetime.datetime(1970, 1, 1, tzinfo=pendulum.timezone('UTC')))
                               .total_seconds()) * 1000 if dagrun.execution_date else None
                # end_date = dagrun.end_date.strftime("%Y-%m-%dT%H:%M:%S.%f") if dagrun.end_date else ""
                # start_date = dagrun.start_date.strftime("%Y-%m-%dT%H:%M:%S.%f") if dagrun.start_date else ""
                # exe_date = dagrun.execution_date.strftime("%Y-%m-%dT%H:%M:%S.%f") if dagrun.execution_date else ""

                data = {"name": name_tuple[1],
                        "execution_date": exe_date,
                        "start_date": start_date,
                        "end_date": end_date,
                        "state": fState}
                res_list.append(data)
        return res_list

    @classmethod
    def get_last7_instances(cls, namelist):
        data = {}
        pool = ThreadPoolExecutor(max_workers=16)
        all_task = [pool.submit(cls.get_last7_instance_status, name) for name in namelist]
        for task in as_completed(all_task):
            result = task.result()
            data.update(result)
        return data

    @classmethod
    def get_last7_instance_status(cls, name=None):
        res = {}
        status_list = []
        try:
            specified_info = cls.get_specified_task_instance_info(name=name, page=1, size=7)
            for info in specified_info["task_info"]:
                tmp_info = {
                    "name": info["name"],
                    "execution_date": info["execution_date"],
                    "start_date": info["start_date"],
                    "end_date": info["end_date"],
                    "state": info["state"],
                    "duration": info["duration"]
                }
                status_list.append(tmp_info)
        except Exception as e:
            logging.error("Get {dag_id} last 7 instance info Failed: {error}".format(dag_id=name, error=str(e)))
        res[name] = status_list
        return res

    @classmethod
    def get_result_last_7_instance(cls, name_tuple_list):
        pool = multiprocessing.Pool(processes=6)
        data_list = []
        for name_tuple in name_tuple_list:
            # if task_name is None or task_name == "":
            #     continue
            # print task_name
            temp_data = pool.apply_async(run_class_fun, args=(cls, name_tuple[0], name_tuple[1]))
            data_list.append(temp_data)
        pool.close()
        pool.join()
        data = {}
        print data_list
        for temp in data_list:
            data.update(temp.get())
        return data

    @classmethod
    def get_last_7_instance_status(cls, task_name=None, ds_task_name=None):
        res = {}
        status_list = []
        try:
            specified_info = cls.get_specified_task_instance_info(name=task_name, page=1, size=7, ds_task_name=ds_task_name, last7=True)
            for info in specified_info["task_info"]:
                tmp_info = {
                    "name": ds_task_name,
                    "execution_date": info["execution_date"],
                    "start_date": info["start_date"],
                    "end_date": info["end_date"],
                    "state": info["state"],
                    "duration": info["duration"]
                }
                status_list.append(tmp_info)
        except Exception as e:
            logging.error("Get {dag_id} last 7 instance info Failed!".format(dag_id=task_name))
        res[ds_task_name] = status_list
        return res

    @classmethod
    @provide_session
    def get_specified_task_instance_info(cls, name=None, page=None, size=None, state=None, start_date=None,
                                         end_date=None, ds_task_name=None, last7=False, instance_id=None,session=None):
        """
        获取任务实例列表,此函数不仅仅是实现从task_instance表中获取某个任务的最近实例,还会获取即将生成实例的调度信息包装成task实例返回.

        由于调度是触发式的调度, 上游没有触发就不会有任务实例生成, 这个对用户造成的最大影响是,在这期间感知不到任务具体发生了什么.
        所以将这部分等待生成实例的信息补充至任务实例列表中,状态为waiting.

        需要被补充的:
        1. 最近一个调度周期的,尚未生成实例
        2. trigger状态为waiting的
        """
        if end_date is None:
            end_date = utcnow()

        task = TaskDesc.get_task(task_name=name, session=session)
        if task is None:
            return {'count': 0, 'task_info': []}

        dag = None
        if task.source_json:
            if isinstance(task.source_json, dict):
                dag = from_dict(task.source_json, name, session=session)
            else:
                dag = from_json(task.source_json, name, session=session)

        have_sub_task = False
        is_crontab_task = False
        if dag and len(dag.task_dict) > 1:
            have_sub_task = True

        is_active = DagModel.is_active_and_on(dag_id=name,session=session)
        if task.crontab is not None and task.crontab != '':
            is_crontab_task = True

        # 获取一些需要的参数
        appurl_dict = ConstantsMap.get_value_to_dict(_type='spark_appurl',session=session)

        instance_count = 0
        all_taskinstance_data = []
        # 状态映射转换
        transform_states = State.taskinstance_state_transform(state)

        if instance_id:
            ti = TaskService.get_ti_by_instance_id(task_name=name, instance_id=instance_id, session=session)
            if ti:
                ti_data = cls.get_format_taskinstance_data(ti, ds_task_name=ds_task_name, appurl_dict=appurl_dict,
                                                           session=session)
                all_taskinstance_data.append(ti_data)
                return {'count': 1, 'task_info': all_taskinstance_data}
            else:
                return {'count': 0, 'task_info': []}

        # 判断筛选条件
        # 如果存在state条件(不包含waiting),那么不需要再去补充trigger的内容,直接查询task_instance表获取
        # 如果是下线任务,也直接查询task_instance表获取
        if (state is not None and state not in [State.TASK_STOP_SCHEDULER, State.TASK_WAIT_UPSTREAM]) \
                or is_active is False:
            task_instance_list = TaskInstance.find_with_date_range(dag_id=name, task_id=name, start_date=start_date,
                                                                   end_date=end_date, states=transform_states,
                                                                   page=page, size=size,
                                                                   offset=(page - 1) * size,session=session)
            if not last7:
                instance_count = TaskInstance.count_with_date_range(dag_id=name, task_id=name, start_date=start_date,
                                                                    states=transform_states, end_date=end_date,session=session)

            for ti in task_instance_list:
                ti_data = cls.get_format_taskinstance_data(ti, ds_task_name=ds_task_name,appurl_dict=appurl_dict,session=session)
                all_taskinstance_data.append(ti_data)

            return {'count': instance_count, 'task_info': all_taskinstance_data}

        else:
            # 获取当前粒度，当前时间周期的execution_date
            nearest_execution_date = TaskCronUtil.ger_prev_execution_date_by_task(task_name=name, cur_date=end_date, task_desc=task)
            # 查询分页的偏移量
            offset = (page - 1) * size
            # 前提条件: 有taskinstance一定存在trigger,但是有trigger不一定存在taskinstance,所以此处先查询trigger确定要展示的实例
            trigger_count = 0
            if not last7:
                trigger_count = Trigger.count_with_date_range(dag_id=name, start_date=start_date, end_date=end_date,
                                                              states=transform_states,session=session)
            first_trigger = Trigger.first_with_date_range(dag_id=name, start_date=start_date, end_date=end_date,session=session)

            if DateUtil.is_date_in_the_range(nearest_execution_date, start_date, end_date) \
                    and (first_trigger is None
                         or (DateUtil.date_equals(first_trigger.execution_date,nearest_execution_date) is False
                             and DateUtil.date_compare(nearest_execution_date, first_trigger.execution_date, eq=False))) \
                    and is_crontab_task is False:

                trigger_count += 1
                if page == 1:
                    exe_date = DateUtil.get_timestamp(nearest_execution_date)
                    ti_data = cls._get_instance_data(name=name, execution_date=exe_date, state=State.TASK_WAIT_UPSTREAM)
                    all_taskinstance_data.append(ti_data)
                    size = size - 1
                    offset = 0
                else:
                    offset = offset - 1

                if first_trigger is None:
                    return {'count': trigger_count, 'task_info': all_taskinstance_data}

            # 筛选出所有triggers
            triggers = Trigger.find_with_date_range(dag_id=name, start_date=start_date, end_date=end_date, page=page,
                                                    states=transform_states,
                                                    size=size, offset=offset,session=session)

            # 存所有trigger的execution_date,作为筛选taskinstance的条件
            all_execution_date = []
            for tri in triggers:
                all_execution_date.append(tri.execution_date.replace(tzinfo=timezone.beijing))

            # waiting和termination状态的只有trigger, 不需要从task_instance表中获取
            if state == State.TASK_WAIT_UPSTREAM:
                task_instance_list = []
            else:
                # 筛选出taskInstance
                task_instance_list = TaskInstance.find_by_execution_dates(dag_id=name, task_id=name,
                                                                          execution_dates=all_execution_date,session=session)

            ti_dict = {}
            for ti in task_instance_list:
                ti_dict[ti.execution_date.strftime('%Y-%m-%d %H:%M:%S.%f')] = ti

            for tri in triggers:
                if tri.state in [State.TASK_WAIT_UPSTREAM, State.TASK_STOP_SCHEDULER, State.BF_WAIT_DEP] \
                        or tri.execution_date.strftime('%Y-%m-%d %H:%M:%S.%f') not in ti_dict.keys():
                    # 如果trigger存在,但是taskInstance不存在,则插入一条假的instance,状态为等待上游就绪
                    # 毫秒时间戳
                    try_number = ''
                    if tri.execution_date.strftime('%Y-%m-%d %H:%M:%S.%f') in ti_dict.keys():
                        try_number = ti_dict[tri.execution_date.strftime('%Y-%m-%d %H:%M:%S.%f')].try_number
                    exe_date = DateUtil.get_timestamp(tri.execution_date) if tri.execution_date else None
                    ti_data = cls._get_instance_data(name=ds_task_name, execution_date=exe_date,
                                                     try_number=try_number,
                                                     state=State.TASK_WAIT_UPSTREAM if tri.state in [State.TASK_WAIT_UPSTREAM, State.BF_WAIT_DEP] else State.TASK_STOP_SCHEDULER)
                    if not last7:
                        try:
                            ti_data["version"] = TaskDesc.get_version(dag_id=tri.dag_id,session=session)
                        except Exception as e:
                            logging.error("[DataStudio pipeline] set version error " + str(e))
                    all_taskinstance_data.append(ti_data)

                    continue

                # TODO 多个task的时候，enddate 应该是最后一个任务的end_date，state 应该是有running为running ，有failed为failed，所有为success为success
                ti = ti_dict[tri.execution_date.strftime('%Y-%m-%d %H:%M:%S.%f')]
                ti_data = cls.get_format_taskinstance_data(ti, has_sub_task=have_sub_task, ds_task_name=ds_task_name,appurl_dict=appurl_dict,session=session)
                all_taskinstance_data.append(ti_data)

            return {'count': trigger_count, 'task_info': all_taskinstance_data}

    @classmethod
    @provide_session
    def get_format_taskinstance_data(cls, taskInstance, has_sub_task=False, ds_task_name=None,appurl_dict=None,session=None):
        # TODO 多个task的时候，enddate 应该是最后一个任务的end_date，state 应该是有running为running ，有failed为failed，所有为success为success
        # 毫秒时间戳
        # dr = DagRun.get_run(dag_id=taskInstance.dag_id, execution_date=taskInstance.execution_date, session=session)
        exe_date = DateUtil.get_timestamp(taskInstance.execution_date) if taskInstance.execution_date else None
        start_date = None
        end_date = None
        duration = ''
        genie_job_id = ''
        genie_job_url = ''
        external_conf = taskInstance.get_external_conf()

        try_number = taskInstance.try_number - 1 if taskInstance.try_number > 0 else 0
        # state = taskInstance.state if dr.get_state() == State.RUNNING else dr.get_state()
        state = taskInstance.state
        state = State.taskinstance_state_mapping(state)
        backfill_label = taskInstance.backfill_label
        if state in [State.RUNNING,State.FAILED,State.SUCCESS,State.TASK_UP_FOR_RETRY]:
            if has_sub_task:
                # 如果有子任务，这里做一个状态合并
                tis = TaskInstance.find_by_execution_dates(dag_id=taskInstance.dag_id, execution_dates=[taskInstance.execution_date], only_state=True,session=session)
                state = State.taskinstance_state_mapping(cls.state_merge(tis))

            end_date = DateUtil.get_timestamp(taskInstance.end_date) if taskInstance.end_date else None
            start_date = DateUtil.get_timestamp(taskInstance.start_date) if taskInstance.start_date else None
            duration_sec = taskInstance.get_duration()
            sec = int(duration_sec % 60)
            t_mi = duration_sec // 60
            mi = int(t_mi % 60)
            hou = int(t_mi // 60)
            duration = ""
            if hou > 0:
                duration += "{}h ".format(hou)
            if mi > 0:
                duration += "{}m ".format(mi)
            if sec > 0:
                duration += "{}s".format(sec)

            genie_info = taskInstance.get_genie_info()
            genie_job_url = genie_info[1]
            genie_job_id = genie_info[0]
        spark_ui = taskInstance.get_spark_webui(appurl_dict)

        return cls._get_instance_data(name=ds_task_name, execution_date=exe_date, start_date=start_date,
                                      end_date=end_date, state=state, type=taskInstance.operator, duration=duration,
                                      genie_job_id=genie_job_id, genie_job_url=genie_job_url,try_number=try_number,
                                      task_type=taskInstance.task_type, is_kyuubi_job=taskInstance.is_kyuubi_job(),
                                      external_conf=external_conf,spark_ui=spark_ui,backfill_label=backfill_label)

    @staticmethod
    def state_merge(tis):
        if not tis or len(tis) == 0:
            return None
        if len(tis) == 1:
            return tis[0].state
        for ti in tis :
            if ti.state in [State.FAILED,State.TASK_UP_FOR_RETRY,State.RUNNING,State.QUEUED,State.SCHEDULED]:
                return ti.state

        return State.SUCCESS

    @staticmethod
    @provide_session
    def dagrun_delete(dag_id=None, execution_date=None,transaction=False, session=None):
        from airflow.models.taskinstance import TaskInstance
        ti_qry = session.query(TaskInstance).filter(TaskInstance.dag_id == dag_id)
        if execution_date:
            ti_qry = ti_qry.filter(TaskInstance.execution_date == execution_date)
        ti_qry.delete()
        if not transaction:
            session.commit()

    @staticmethod
    @provide_session
    def get_dag_run_status(dag_id=None, execution_date=None, session=None):
        from airflow.models.dagrun import DagRun
        dagrun = session.query(DagRun).query(DagRun.dag_id == dag_id,
                                             DagRun.execution_date == execution_date).one_or_none()
        if dagrun:
            return dagrun.state
        else:
            return State.WAITING

    @staticmethod
    @provide_session
    def get_dag_run_log(dag_id=None, execution_date=None, session=None):
        from airflow.models.taskinstance import TaskInstance
        tis = session.query(TaskInstance).filter(TaskInstance.dag_id == dag_id,
                                              TaskInstance.execution_date == execution_date).all()
        genie_job_ids = []
        if tis:
            for ti in tis:
                ex_conf = ti.get_external_conf()
                if ex_conf:
                    tmp = ex_conf["genie_job_id"] if "genie_job_id" in ex_conf.keys() else None
                    if tmp:
                        genie_job_ids.append(tmp)
        cts = ""
        dag = get_dag(dag_id=dag_id)
        for task_id in dag.task_dict:
            if task_id == dag_id:
                cts = dag.task_dict[task_id].cluster_tags
        if cts and (isinstance(cts, str) or isinstance(cts, unicode)):
            cts = [i.strip() for i in cts.split(",")]
        region = None
        group = None
        for tag in cts:
            if tag.startswith("rbac.cluster:"):
                group = tag.split("-")[0].split('rbac.cluster:')[1]
            elif tag.startswith("region"):
                region = tag.split('region:')[1]
        from airflow.shareit.models.constants_map import ConstantsMap
        base_url = ConstantsMap.get_value(_type="log_url", key=region)
        cluster_id = ConstantsMap.get_value(_type="log_cluster", key=region)
        m_g_id = ""
        s_g_id = ""
        match_phrase = ""
        for genie_job_id in genie_job_ids:
            m_g_id += genie_job_id
            m_g_id += ","
            tmp = "'{}'".format(genie_job_id)
            s_g_id += tmp
            s_g_id += ","
            tmp_m = "(match_phrase:(labels.genie-job-id:'{}'))".format(genie_job_id)
            match_phrase += tmp_m
            match_phrase += ","
        m_g_id = m_g_id[0:-1]
        s_g_id = s_g_id[0:-1]
        match_phrase = match_phrase[0:-1]
        url_fmt = """{BASE_URL}#/discover?_g=(filters:!(),refreshInterval:(pause:!t,value:0),time:(from:now-7d,to:now))&_a=(columns:!(log_msg,log_level,pod),filters:!(('$state':(store:appState),meta:(alias:!n,disabled:!f,index:'{CLUSTER_ID}',key:labels.genie-job-id,negate:!f,params:!({S_G_ID}),type:phrases,value:'{M_G_ID}'),query:(bool:(minimum_should_match:1,should:!({MATCH_PHRASE}))))),index:'{CLUSTER_ID}',interval:auto,query:(language:kuery,query:''),sort:!(!('@timestamp',desc))) """.format(
            BASE_URL=base_url,
            S_G_ID=s_g_id,
            M_G_ID=m_g_id,
            CLUSTER_ID=cluster_id,
            MATCH_PHRASE=match_phrase
        )
        return url_fmt

    @staticmethod
    def trigger_manual(dag_id, execution_date):
        Trigger.sync_trigger_to_db(dag_id=dag_id, execution_date=execution_date, state=State.WAITING,
                                   trigger_type="manual", editor="DataPipeline")

    @staticmethod
    def generate_dataset_sign_manual(metadata_id, execution_date, state, run_id):
        # 粒度相同，为天级别
        DatasetPartitionStatus.sync_state(metadata_id=metadata_id, dataset_partition_sign=execution_date,
                                          run_id=run_id, state=state)
        ddrs = DagDatasetRelation.get_downstream_dags(metadata_id=metadata_id)
        for ddr in ddrs:
            task = TaskDesc.get_task(task_name=ddr.dag_id)
            # 当下游任务的粒度和对应下游任务的input粒度对应时才生成trigger
            if Granularity.formatGranularity(ddr.granularity) == Granularity.formatGranularity(
                    task.get_granularity()):
                if "5" == ddr.granularity:
                    # daily
                    exe_time = Granularity.getNextTime(execution_date, ddr.granularity, ddr.offset)
                else:
                    continue
            else:
                continue
            # 计算 trigger
            Trigger.sync_trigger_to_db(dag_id=ddr.dag_id, execution_date=exe_time, state=State.WAITING,
                                       trigger_type="manual", editor="DataPipeline")

    @staticmethod
    def render_api(content, context, task_name=None):
        import six
        import jinja2
        from datetime import timedelta
        from airflow.shareit.utils.task_util import TaskUtil
        if not isinstance(content, six.string_types):
            raise ValueError("Content should be string_types! ")

        if 'execution_date' not in context.keys():
            raise Exception("miss execution_date!")

        execution_date = context['execution_date']
        execution_date = execution_date.replace(tzinfo=beijing)
        context['execution_date'] = execution_date
        execution_date_utc0 = execution_date - timedelta(hours=8)
        context['execution_date_utc0'] = execution_date_utc0
        jinja_env = jinja2.Environment(cache_size=0)
        if task_name:
            context = TaskUtil.get_template_context(dag_id=task_name, execution_date=execution_date, context=context)
            return jinja_env.from_string(content).render(**context)


        ds = execution_date.strftime('%Y-%m-%d')
        ts = execution_date.isoformat()
        yesterday_ds = (execution_date - datetime.timedelta(1)).strftime('%Y-%m-%d')
        tomorrow_ds = (execution_date + datetime.timedelta(1)).strftime('%Y-%m-%d')
        lastmonth_date = DateCalculationUtil.datetime_month_caculation(execution_date, -1).strftime('%Y-%m')

        prev_execution_date = execution_date
        prev_2_execution_date = execution_date
        next_execution_date = execution_date

        next_ds = None
        next_ds_nodash = None
        if next_execution_date:
            next_ds = next_execution_date.strftime('%Y-%m-%d')
            next_ds_nodash = next_ds.replace('-', '')
            next_execution_date = pendulum.instance(next_execution_date)

        prev_ds = None
        prev_ds_nodash = None
        if prev_execution_date:
            prev_ds = prev_execution_date.strftime('%Y-%m-%d')
            prev_ds_nodash = prev_ds.replace('-', '')
            prev_execution_date = pendulum.instance(prev_execution_date)

        if prev_2_execution_date:
            prev_2_execution_date = pendulum.instance(prev_2_execution_date)

        ds_nodash = ds.replace('-', '')
        ts_nodash = execution_date.strftime('%Y%m%dT%H%M%S')
        ts_nodash_with_tz = ts.replace('-', '').replace(':', '')
        yesterday_ds_nodash = yesterday_ds.replace('-', '')
        tomorrow_ds_nodash = tomorrow_ds.replace('-', '')
        lastmonth_date_ds_nodash = lastmonth_date.replace('-','')

        ds_utc0 = execution_date_utc0.strftime('%Y-%m-%d')
        ts_utc0 = execution_date_utc0.isoformat()
        yesterday_ds_utc0 = (execution_date_utc0 - datetime.timedelta(1)).strftime('%Y-%m-%d')
        tomorrow_ds_utc0 = (execution_date_utc0 + datetime.timedelta(1)).strftime('%Y-%m-%d')
        lastmonth_date_utc0 = DateCalculationUtil.datetime_month_caculation(execution_date_utc0, -1).strftime('%Y-%m')

        ds_nodash_utc0 = ds_utc0.replace('-', '')
        ts_nodash_utc0 = execution_date_utc0.strftime('%Y%m%dT%H%M%S')
        ts_nodash_with_tz_utc0 = ts_utc0.replace('-', '').replace(':', '')
        yesterday_ds_nodash_utc0 = yesterday_ds_utc0.replace('-', '')
        tomorrow_ds_nodash_utc0 = tomorrow_ds_utc0.replace('-', '')
        lastmonth_date_ds_nodash_utc0 = lastmonth_date_utc0.replace('-', '')


        context['ds'] = ds
        context['ts'] = ts
        context['yesterday_ds'] = yesterday_ds
        context['tomorrow_ds'] = tomorrow_ds
        context['ds_nodash'] = ds_nodash
        context['ts_nodash'] = ts_nodash
        context['ts_nodash_with_tz'] = ts_nodash_with_tz
        context['yesterday_ds_nodash'] = yesterday_ds_nodash
        context['tomorrow_ds_nodash'] = tomorrow_ds_nodash
        context['lastmonth_date'] = lastmonth_date
        context['lastmonth_date_ds_nodash'] = lastmonth_date_ds_nodash

        context['ds_utc0'] = ds_utc0
        context['ts_utc0'] = ts_utc0
        context['yesterday_ds_utc0'] = yesterday_ds_utc0
        context['tomorrow_ds_utc0'] = tomorrow_ds_utc0
        context['ds_nodash_utc0'] = ds_nodash_utc0
        context['ts_nodash_utc0'] = ts_nodash_utc0
        context['ts_nodash_with_tz_utc0'] = ts_nodash_with_tz_utc0
        context['yesterday_ds_nodash_utc0'] = yesterday_ds_nodash_utc0
        context['tomorrow_ds_nodash_utc0'] = tomorrow_ds_nodash_utc0
        context['lastmonth_date_utc0'] = lastmonth_date_utc0
        context['lastmonth_date_ds_nodash_utc0'] = lastmonth_date_ds_nodash_utc0


        context['next_ds_nodash'] = next_ds_nodash
        context['prev_ds'] = prev_ds
        context['prev_ds_nodash'] = prev_ds_nodash
        context['prev_execution_date'] = prev_execution_date
        context['prev_2_execution_date'] = prev_2_execution_date
        context['next_execution_date'] = next_execution_date
        return jinja_env.from_string(content).render(**context)

    @staticmethod
    @provide_session
    def set_paused(task_name, is_paused,session=None):
        try :
            TaskService.set_paused_transaction(task_name,is_paused,transaction=True,session=session)
        except Exception as e:
            session.rollback()
            raise e

    @staticmethod
    @provide_session
    def set_paused_transaction_by_ds_task_id(ds_task_id, is_paused, tenant_id=1, session=None):
        task = TaskDesc.get_task_with_tenant(ds_task_id=ds_task_id, tenant_id=tenant_id, session=session)
        TaskService.set_paused_transaction(task.task_name, is_paused, transaction=True, session=session)

    @staticmethod
    @provide_session
    def set_paused_transaction(task_name, is_paused, transaction=False,session=None):
        from airflow.models import DagModel
        from airflow.models import TaskInstance
        from airflow.utils.state import State as Astate
        dm = DagModel.get_dagmodel(task_name,session=session)
        if dm:
            dm.set_is_paused(is_paused=is_paused, including_subdags=False, store_serialized_dags=False,transaction=transaction)
        else:
            return ValueError("This task is not exist right now! please try it later")
        try:
            now_date = make_naive(timezone.utcnow())
            max_exe_time = None
            # Set trigger state
            if is_paused:
                waiting_triggers = Trigger.find(dag_id=task_name, state=State.WAITING,session=session)
                for trigger in waiting_triggers:
                    trigger.sync_state_to_db(state=State.TASK_STOP_SCHEDULER, session=session, commit=False)
                    if (not max_exe_time or trigger.execution_date > max_exe_time) and trigger.execution_date < now_date:
                        max_exe_time = trigger.execution_date
                Trigger.delete_by_execution_date_range(task_name=task_name, start_date=make_naive(timezone.utcnow().replace(tzinfo=beijing)),
                                                       session=session)
                running_triggers = Trigger.find(dag_id=task_name, state=State.RUNNING,session=session)
                for trigger in running_triggers:
                    trigger.sync_state_to_db(state=State.FAILED, session=session, commit=False)
                    if (not max_exe_time or trigger.execution_date > max_exe_time) and trigger.execution_date < now_date:
                        max_exe_time = trigger.execution_date
                tis = session.query(TaskInstance).filter(TaskInstance.dag_id == task_name,TaskInstance.state.in_((Astate.SCHEDULED, Astate.UP_FOR_RETRY,Astate.RUNNING,Astate.QUEUED,)))
                sensors = SensorInfo.get_checking_info(dag_id=task_name,session=session)
                execs = set(si.execution_date for si in sensors)
                for execution_date in execs:
                    SensorInfo.update_state_by_dag_id(dag_id=task_name, execution_date=execution_date,
                                                      state=State.FAILED,transaction=transaction,session=session)

                for ti in tis:
                    ti.set_state(state=State.FAILED, session=session, commit=False)
                    session.merge(ti)
                if not transaction:
                    session.commit()

                if max_exe_time:
                    if TaskInstance.is_latest_runs(task_name, max_exe_time.replace(tzinfo=beijing)):
                        td = TaskDesc.get_task(task_name=task_name)
                        logging.info("send callback to dstask ,task_name:{},date:{}".format(task_name,max_exe_time.strftime('%Y-%m-%d %H:%M:%S')))
                        NormalUtil.callback_task_state(td,State.FAILED)
        except Exception as e:
            logging.error("[DataStudio pipeline] set paused error " + str(e))
            logging.error(traceback.format_exc())
            raise e

    @staticmethod
    @provide_session
    def update_ds_task_name(old_name,new_name,tenant_id=1,session=None):
        task_desc = TaskDesc.get_task_with_tenant(ds_task_name=old_name,tenant_id=tenant_id)
        if task_desc:
            task_desc.ds_task_name=new_name
            session.merge(task_desc)
            session.commit()

    @staticmethod
    @provide_session
    def update_task_name(old_name, new_name, session=None):
        try:
            TaskService.update_name_transaction(old_name,new_name,transaction=True,session=session)
        except Exception as e:
            session.rollback()
            raise e

    @staticmethod
    @provide_session
    def update_name_transaction(old_name, new_name,transaction=None, session=None):
        if TaskDesc.check_is_exists(old_name,session=session) is False:
            logging.info("task: [{}] not exists".format(old_name))
            return
        # 不能更新为已存在的任务名称
        if TaskDesc.check_is_exists(new_name,session=session) is True:
            logging.warning("update taskname failed, err : [{}] already exists".format(new_name))
            raise Exception("update taskname failed, err : [{}] already exists".format(new_name))

        try:
            from airflow.models.dag import DagModel
            from airflow.models.dagrun import DagRun
            from airflow.models.taskinstance import TaskInstance
            from airflow.shareit.models.taskinstance_log import TaskInstanceLog
            from airflow.shareit.models.gray_test import GrayTest
            TaskDesc.update_name(old_name, new_name, transaction=transaction, session=session)
            DagModel.update_name(old_name, new_name, transaction=transaction, session=session)
            DagDatasetRelation.update_name(old_name, new_name, transaction=transaction, session=session)
            TaskInstance.update_name(old_name, new_name, transaction=transaction, session=session)
            TaskInstanceLog.update_name(old_name, new_name, transaction=transaction, session=session)
            Trigger.update_name(old_name, new_name, transaction=transaction,session=session)
            EventDependsInfo.update_depend_id(old_name,new_name,transaction=transaction,session=session)
            SensorInfo.update_dag_id(old_name,new_name,transaction=transaction,session=session)
            GrayTest.update_task_name(old_name,new_name,transaction=transaction,session=session)
        except Exception as e:
            logging.error("update taskname failed,err:{}".format(e.message))
            raise Exception("task: [{}] update name failed".format(old_name))

    @classmethod
    def _get_instance_data(cls, name=None, execution_date=None, start_date=None, end_date=None, state=None, type=None,
                           duration=None, genie_job_id="", genie_job_url="", try_number="", external_conf=None,task_type=None,is_kyuubi_job=False
                           ,spark_ui=None,backfill_label=None):
        res = {"name": name,
               "execution_date": execution_date,
               "start_date": start_date,
               "end_date": end_date,
               "state": state,
               "type": type,
               "duration": duration,
               "genie_job_id": genie_job_id,
               "genie_job_url": genie_job_url,
               "try_number": str(try_number),
               "task_type":task_type,
               "is_kyuubi_job": is_kyuubi_job,
               "spark_ui": spark_ui,
               "backfill_label": "" if not backfill_label else backfill_label
               }
        if external_conf and isinstance(external_conf,dict):
            for k in external_conf:
                res[k] = external_conf[k]
        return res

    @staticmethod
    @provide_session
    def get_stastics(start_date=None, end_date=None, session=None):
        if end_date is None:
            end_date = datetime.datetime.utcnow()
        if start_date is None:
            start_date = end_date - datetime.timedelta(days=1)
        if start_date > end_date:
            raise ValueError("start_date or end_date error")
        num_of_dags_sql = "SELECT COUNT(dag_id) FROM dag WHERE is_active =TRUE"
        active_dags_sql = "SELECT COUNT(dag_id) FROM dag WHERE is_active =TRUE AND is_paused =FALSE"
        active_user_sql = 'SELECT COUNT(DISTINCT owner) FROM log WHERE dttm>="{START_TIME}" ' \
                          'and dttm<"{END_TIME}"and event="tree"'.format(START_TIME=start_date, END_TIME=end_date)
        total_task_instance_sql = "SELECT count(*) FROM task_instance WHERE " \
                                  "start_date>='{start_date}' and start_date <'{end_date}'".format(
            start_date=start_date,
            end_date=end_date)
        avg_exec_time_sql = "SELECT avg(end_date-start_date) FROM task_instance WHERE start_date>='{start_date}' " \
                            "AND start_date <'{end_date}' AND state='success'".format(start_date=start_date,
                                                                                      end_date=end_date)
        failed_task_sql = "SELECT count(*) FROM task_instance WHERE start_date>='{start_date}' AND " \
                          "start_date <'{end_date}' AND state ='failed'".format(start_date=start_date,
                                                                                end_date=end_date)
        num_of_dags = int(session.execute(num_of_dags_sql).fetchone()[0])
        active_dags = int(session.execute(active_dags_sql).fetchone()[0])
        active_user = int(session.execute(active_user_sql).fetchone()[0])
        total_task_instance = int(session.execute(total_task_instance_sql).fetchone()[0])
        avg_exec_time = session.execute(avg_exec_time_sql).fetchone()[0]
        failed_task = int(session.execute(failed_task_sql).fetchone()[0])
        res = {"Total_number_of_dags": num_of_dags,  # 任务总数
               "Total_number_of_active_dags": active_dags,  # 活跃任务数
               "Total_number_of_active_users": active_user,  # 活跃用户数
               "Total_number_of_task_instance": total_task_instance,  # 实例总数
               "Average_execution_time": avg_exec_time,  # 实例平均执行时间
               "Total_number_of_failed_task": failed_task,  # 实例失败数量
               "rate_of_success_task": 1 - failed_task / float(total_task_instance)}  # 实例成功率
        return res

    @classmethod
    def instance_relation_shell(cls, core_task, execution_date=None, level=1, is_downstream=True):
        instance_list = []
        gra = DagDatasetRelation.get_task_granularity(dag_id=core_task)
        if gra < 0:
            raise ValueError("Task may be have error can not get correct info in db!")
        if execution_date is not None:
            execution_date = Granularity.get_execution_date(execution_date, gra)

        core_task_id = cls.get_task_instance_id(dag_id=core_task, execution_date=execution_date)
        tmp_info_list, relation_list = cls.instance_relation(core_task, execution_date, level, is_downstream)
        for ti in tmp_info_list:
            tmp_info = cls._get_task_instance_info(dag_id=ti["dag_id"], execution_date=ti["execution_date"])
            num_of_upstream, num_of_downstream = cls._get_task_num_of_direct_up_down(dag_id=ti["dag_id"],
                                                                                     execution_date=ti[
                                                                                         "execution_date"])
            tmp_info["num_of_upstream"] = num_of_upstream
            tmp_info["num_of_downstream"] = num_of_downstream
            instance_list.append(tmp_info)

        return {"instance_list": instance_list, "relation": relation_list, "core_task_id": core_task_id}

    @classmethod
    def instance_relation(cls, core_task, execution_date=None, level=1, is_downstream=True):
        """
        获取当前任务的关联任务信息
        # 获取上游时如果 大粒度依赖小粒度的情况，是多对一的关系
        """
        instance_list = []
        relation_list = []
        instance_list.append({"dag_id": core_task, "execution_date": execution_date})
        if level < 1:
            return instance_list, relation_list
            # raise ValueError("Error param: {core_task} {execution_date} {level} {downstreams}".format(core_task,
            #                                                                                           execution_date,
            #                                                                                           level,
            #                                                                                           is_downstream))

        # gra = DagDatasetRelation.get_task_granularity(dag_id=core_task)
        # if gra < 0:
        #     raise ValueError("Task may be have error can not get correct info in db!")
        # if execution_date is not None:
        #     execution_date = Granularity.get_execution_date(execution_date, gra)

        # instance_list.append(cls._get_task_instance_info(dag_id=core_task, execution_date=execution_date))
        tis = cls._get_related_task_instance(core_task, execution_date, is_downstream)
        for ti in tis:
            # tmp_info = cls._get_task_instance_info(dag_id=ti["dag_id"], execution_date=ti["execution_date"])
            if is_downstream:
                tmp_relation = {"source": cls.get_task_instance_id(dag_id=core_task, execution_date=execution_date),
                                "target": cls.get_task_instance_id(dag_id=ti["dag_id"],
                                                                   execution_date=ti["execution_date"])}
            else:
                tmp_relation = {"source": cls.get_task_instance_id(dag_id=ti["dag_id"],
                                                                   execution_date=ti["execution_date"]),
                                "target": cls.get_task_instance_id(dag_id=core_task, execution_date=execution_date)}
            instance_list.append(ti)
            relation_list.append(tmp_relation)
            tmp_info_list, tmp_relation_list = cls.instance_relation(ti["dag_id"], ti["execution_date"], level - 1,
                                                                     is_downstream)
            instance_list.extend(tmp_info_list)
            relation_list.extend(tmp_relation_list)
        # 去除重复项
        instance_list = [dict(t) for t in set([tuple(sorted(d.items())) for d in instance_list])]
        relation_list = [dict(t) for t in set([tuple(sorted(d.items())) for d in relation_list])]
        return instance_list, relation_list

    @staticmethod
    def _get_related_task_instance(core_task, execution_date=None, is_downstream=True):
        from airflow.models.dag import DagModel
        from airflow.shareit.utils.task_cron_util import TaskCronUtil
        # 判断任务是否是上线状态

        """
        获取当前实例的直接依赖实例或者直接下游实例
        """
        instances = []
        if is_downstream:
            # 获取事件(任务成功)依赖的下游实例
            td = TaskDesc.get_task(task_name=core_task)
            e_depends = EventDependsInfo.get_downstream_event_depends(ds_task_id=td.ds_task_id, tenant_id=td.tenant_id)
            for e_depend in e_depends:
                downstream_dag_id = e_depend.dag_id
                if not DagModel.is_active_and_on(dag_id=downstream_dag_id):
                    continue

                downstream_exe_dates = DateCalculationUtil.get_downstream_all_execution_dates(execution_date=execution_date,
                                                                       calculation_param=e_depend.date_calculation_param,
                                                                       downstream_task=downstream_dag_id)

                if downstream_exe_dates is None or len(downstream_exe_dates) == 0:
                    continue
                else:
                    for exe_date in downstream_exe_dates:
                        instances.append({"dag_id": downstream_dag_id, "execution_date": exe_date})

        else:
            # 获取事件(任务成功)依赖的上游实例
            e_depends = EventDependsInfo.get_dag_event_depends(core_task, type=Constant.TASK_SUCCESS_EVENT)
            for e_depend in e_depends:
                upstream_task_id=e_depend.depend_id
                if not DagModel.is_active_and_on(dag_id=upstream_task_id):
                    continue

                upstream_exe_date_list = DateCalculationUtil.get_upstream_all_execution_dates(execution_date=execution_date,calculation_param=e_depend.date_calculation_param,upstream_task=upstream_task_id)
                for upstream_exe_date in upstream_exe_date_list:
                    instances.append({"dag_id": e_depend.depend_id, "execution_date": upstream_exe_date})
        return instances

    @classmethod
    def _get_task_instance_info(cls, dag_id, execution_date):
        """
        返回实例信息，包括 dag_id ,execution_date,state,start_date,end_date,id
        """
        gid = cls.get_task_instance_id(dag_id=dag_id, execution_date=execution_date)
        tmp_date = execution_date
        if isinstance(execution_date, datetime.datetime):
            tmp_date = int((execution_date.replace(tzinfo=beijing) - datetime.datetime(1970, 1, 1, tzinfo=beijing))
                           .total_seconds()) * 1000
        ti_info = {"dag_id": dag_id,
                   "execution_date": tmp_date,
                   "state": None,
                   "start_date": None,
                   "end_date": None,
                   "duration": None,
                   "id": gid
                   }
        if execution_date is None:
            return ti_info
        try:
            specified_info = cls.get_specified_task_instance_info(name=dag_id, page=1, size=1,
                                                                  start_date=execution_date,
                                                                  end_date=execution_date, ds_task_name=dag_id)
            for info in specified_info["task_info"]:
                ti_info = {"dag_id": info["name"],
                           "execution_date": info["execution_date"],
                           "start_date": info["start_date"],
                           "end_date": info["end_date"],
                           "state": info["state"],
                           "duration": info["duration"],
                           "id": gid
                           }
        except Exception:
            logging.error("Get {dag_id} last 7 instance info Failed!".format(dag_id=dag_id))
        return ti_info

    @classmethod
    def _get_task_num_of_direct_up_down(cls, dag_id, execution_date):
        """
        获取当前实例的直接上游和直接下游实例数
        """
        upstream, downstream = 0, 0
        if execution_date is None:
            return 0, 0
        ddrs = DagDatasetRelation.get_downstream_dags_by_dag_id(dag_id=dag_id)
        downstream = len(ddrs)
        input_ddrs = DagDatasetRelation.get_inputs(dag_id=dag_id)
        for ddr in input_ddrs:
            tasks = DagDatasetRelation.get_active_upstream_dags(metadata_id=ddr.metadata_id)
            task = None  # 依赖的上游任务
            for tmp_task in tasks:
                task = tmp_task
            if task is None:
                continue
            else:
                # 依赖的数据时间
                base_date = Granularity.getPreTime(baseDate=execution_date, gra=ddr.granularity, offset=ddr.offset)
                if ddr.detailed_gra is not None and ddr.detailed_dependency is not None:
                    exe_dates = Granularity.get_detail_time_range(base_gra=task.granularity,
                                                                  depend_gra=ddr.granularity,
                                                                  detailed_gra=ddr.detailed_gra,
                                                                  detailed_dependency=ddr.detailed_dependency,
                                                                  base_date=base_date)
                else:
                    end_date = Granularity.getNextTime(base_date, ddr.granularity, -1)
                    exe_dates = Granularity.splitTimeByGranularity(start_time=base_date, end_time=end_date,
                                                                   gra=task.granularity)

                upstream += len(exe_dates)
        return upstream, downstream

    @staticmethod
    def get_task_instance_id(dag_id, execution_date):
        """
        返回实例的唯一标识，{dag_id}_{execution_date}
        """
        if execution_date is None:
            gid = dag_id + "_None"
        elif isinstance(execution_date, six.string_types):
            gid = dag_id + "_" + execution_date
        elif isinstance(execution_date, datetime.datetime):
            gid = dag_id + "_" + execution_date.strftime("%Y%m%dT%H%M%S")
        return gid

    @classmethod
    def send_backfill_progress_reminder(cls, user_action_id=None, user_action=None):
        from airflow.shareit.utils.alarm_adapter import build_backfill_process_reminder_content, \
            _get_receiver_list_from_str,send_ding_notify
        instance_info_list = []
        if user_action is None:
            user_action = UserAction.find_a_user_action(user_action_id=user_action_id)
            if user_action:
                details = user_action.get_details()
            else:
                raise ValueError("未找到记录")
        else:
            user_action = UserAction.find_a_user_action(user_id=user_action.user_id,
                                                        action_type=user_action.action_type,
                                                        execution_time=user_action.execution_time)
            details = user_action.get_details()
        for ins in details["instance_list"]:
            exe_date = ins[1]
            if isinstance(ins[1], six.string_types):
                exe_date = pendulum.parse(ins[1])
            # TODO 这里之后舍弃掉这个方法
            ti_info = cls._get_task_instance_info(dag_id=ins[0], execution_date=exe_date)
            instance_info_list.append({"dag_id": ins[0],
                                       "execution_date": exe_date.strftime("%Y-%m-%dT%H"),
                                       "state": ti_info["state"]})

        receiver = _get_receiver_list_from_str(details["owners"])
        content = build_backfill_process_reminder_content(instance_info_list=instance_info_list)
        # from airflow.configuration import conf
        # try:
        #     base_url = conf.get("core", "back_fill_reminder_url")
        # except Exception:
        #     base_url = "https://www.baidu.com"
        # url_list = build_backfill_process_reminder_url_list(base_url=base_url, user_action_id=user_action.id)

        # send_ding_action_card(title="[DataStudio] 回算任务进度", content=content, url_list=url_list, receiver=receiver)
        notify_type = conf.get('email', 'notify_type')
        if "dingding" in notify_type:
            send_ding_notify(title="[DataStudio] 回算任务进度", content=content, receiver=receiver)

    @classmethod
    def send_backfill_progress_reminder_handle(cls,operator,core_task,start_date,end_date,receiver,user_action_id,email_receiver):
        from airflow.shareit.utils.alarm_adapter import build_backfill_process_reminder_url_list, \
            build_backfill_handle_content, _get_receiver_list_from_str, send_ding_action_card,send_email_by_notifyAPI
        content = build_backfill_handle_content(operator,core_task,start_date,end_date)
        from airflow.configuration import conf
        try:
            base_url = conf.get("core", "back_fill_reminder_url")
        except Exception:
            base_url = "https://www.baidu.com"
        url_list = build_backfill_process_reminder_url_list(base_url=base_url, user_action_id=user_action_id)
        notify_type = conf.get('email', 'notify_type')
        if "email" in notify_type:
            receiver = _get_receiver_list_from_str(email_receiver)
            send_email_by_notifyAPI(title="[DataStudio] 回算任务进度",content=content,receiver=receiver)
        if "dingding" in notify_type:
            receiver = _get_receiver_list_from_str(receiver)
            send_ding_action_card(title="[DataStudio] 回算任务进度", content=content, url_list=url_list, receiver=receiver)




    @staticmethod
    @provide_session
    def get_all_running_dag_id(session):
        from airflow.models.dag import DagModel
        from airflow.models.dagrun import DagRun
        from airflow.models.taskinstance import TaskInstance
        subquery = session.query(DagModel.dag_id).filter(DagModel.is_active == True,
                                                         DagModel.is_paused == False)
        tis = session.query(TaskInstance.dag_id) \
            .filter(TaskInstance.state == State.RUNNING,
                    TaskInstance.dag_id.in_(subquery)).all()
        drs = session.query(DagRun.dag_id) \
            .filter(DagRun.state == State.RUNNING,
                    DagRun.dag_id.in_(subquery)).all()
        tis.extend(drs)
        res = []
        for ti in tis:
            res.append(ti[0])
        return set(res)


    @staticmethod
    def get_date_preview(task_gra,task_crontab,task_depend=None,data_depend=None,tenant_id=1):
        if task_crontab is not None and task_crontab != '':
            # 校验crontab是否正确
            if not croniter.is_valid(task_crontab):
                raise ValueError("wrong crontab {}".format(task_crontab))
        else :
            task_crontab = TaskCronUtil.get_cron_by_gra(Granularity.gra_priority_map[task_gra])

        cron = croniter(task_crontab, timezone.utcnow())
        cron.tzinfo = beijing
        execution_date = cron.get_prev(datetime.datetime)
        executionDate = []
        checkPath = []
        if data_depend is not None and data_depend != "":
            if isinstance(data_depend, unicode) or isinstance(data_depend, str):
                data_depend = json.loads(data_depend)
                format_gra = Granularity.formatGranularity(data_depend['granularity'])
                granularity = Granularity.gra_priority_map[format_gra]

                if data_depend['use_date_calcu_param'] == False:
                    date_calculation_param = DateCalculationUtil.offset_2_data_calculation_param(granularity,
                                                                                                 data_depend['unitOffset'])
                else:
                    date_calculation_param = json.dumps(data_depend['date_calculation_param'])

                all_data_dates = DateCalculationUtil.get_upstream_all_data_dates(execution_date=execution_date,
                                                                                 calculation_param=date_calculation_param,
                                                                                 gra=granularity)
                upstream_dag_ddrs = DagDatasetRelation.get_active_upstream_dags(metadata_id=data_depend['id'])
                if len(upstream_dag_ddrs) > 0:
                    # 是内部数据
                    for data_date in all_data_dates:
                        tmp_exe = Granularity.getNextTime(data_date, gra=upstream_dag_ddrs[0].granularity, offset=upstream_dag_ddrs[0].offset)
                        exe_dates = TaskCronUtil.get_all_date_in_one_cycle(upstream_dag_ddrs[0].dag_id, tmp_exe)
                        for exe_date in exe_dates:
                            executionDate.append(exe_date.strftime("%Y-%m-%d %H:%M:%S"))
                else :
                    # 是外部数据
                    for data_date in all_data_dates:
                        check_path = data_depend['check_path'] if 'check_path' in  data_depend.keys() else ''
                        render_check_path = TaskUtil.render_datatime_one_path(content=check_path,
                                                                              execution_date=execution_date,
                                                                              data_date=data_date,
                                                                              crontab=task_crontab)
                        checkPath.append(render_check_path)

        if task_depend is not None and task_depend != "":
            if isinstance(task_depend, unicode) or isinstance(task_depend, str):
                task_depend = json.loads(task_depend)
                granularity = Granularity.gra_priority_map[task_depend['granularity']]
                upstream_task = '{}_{}'.format(str(tenant_id),str(task_depend['taskId']))
                upstream_crontab = task_depend['crontab']
                # 将偏移量也转换为date_calculation_param格式, 统一时间计算
                if task_depend['use_date_calcu_param'] == False:
                    date_calculation_param = DateCalculationUtil.offset_2_data_calculation_param(granularity,
                                                                                                 task_depend['unitOffset'])

                else:
                    date_calculation_param = json.dumps(task_depend['date_calculation_param'])

                all_exe_dates = DateCalculationUtil.get_upstream_all_execution_dates(execution_date=execution_date,
                                                                                     calculation_param=date_calculation_param,
                                                                                     upstream_crontab=upstream_crontab,
                                                                                     upstream_task=upstream_task)
                outputs = DagDatasetRelation.get_outputs(dag_id=upstream_task)
                for exe_date in all_exe_dates :
                    # output_data_date=''
                    # if len(outputs) > 0:
                        # output_data_date = Granularity.getPreTime(exe_date, outputs[0].granularity, outputs[0].offset)
                    executionDate.append(exe_date.strftime("%Y-%m-%d %H:%M:%S"))
        result = {
            "current_date" : utcnow().replace(tzinfo=beijing).strftime("%Y-%m-%d %H:%M:%S"),
            "curExecutionDate": execution_date.strftime("%Y-%m-%d %H:%M:%S"),
            "dependDate":{"dependDate":executionDate,"checkPath":checkPath}
        }
        # result["dependDate"]["executionDate"] = executionDate
        # result["dependDate"]["checkPath"] = checkPath
        return result

    @staticmethod
    @provide_session
    def task_execute(task_name,execution_date,external_conf=None,dag=None,transaction=False,session=None):
        from airflow.models.taskinstance import TaskInstance
        transaction=transaction
        Trigger.sync_trigger_to_db(dag_id=task_name, execution_date=execution_date, state=State.RUNNING,
                                   trigger_type=TRI_FORCE, editor="DataPipeline", transaction=transaction,
                                   session=session)
        if not dag:
            dag = get_dag(task_name)
        ti = TaskInstance.find_by_execution_date(dag_id=task_name,task_id=task_name,execution_date=execution_date,session=session)
        if ti:
            TaskInstance.clear_task_instance_list(ti,external_conf=external_conf,transaction=transaction,
                                   session=session)
        else:
            TriggerHelper.create_taskinstance(dag.task_dict,execution_date,task_type=BACKFILL, external_conf=external_conf,transaction=transaction,
                                       session=session)
        if not transaction:
            session.commit()
        # 将开始运行的状态返回给ds_task服务
        try:
            if TaskInstance.is_latest_runs(task_name, execution_date):
                td = TaskDesc.get_task(task_name=task_name)
                NormalUtil.callback_task_state(td, State.RUNNING)
        except Exception as e:
            log.error(
                "[DataStudio Pipeline] task:{} ,sync state to ds backend error: {}".format(task_name, str(e)))

    @staticmethod
    def get_task_state(task_name,execution_date):
        tri = Trigger.find_by_execution_dates(dag_id=task_name, execution_dates=[execution_date])
        if tri and tri[0].state in [State.WAITING,State.TASK_WAIT_UPSTREAM]:
            return tri[0].state
        from airflow.models import TaskInstance
        tis = TaskInstance.find_by_execution_date(dag_id=task_name, execution_date=execution_date)
        state = TaskService.state_merge(tis)
        if state in [State.QUEUED] :
            state = State.RUNNING

        if state in [State.TASK_STOP_SCHEDULER,State.TASK_UP_FOR_RETRY]:
            state = State.FAILED
        return state

    @staticmethod
    def get_child_dependencie(task_id,notebook):
        if task_id in notebook:
            return None
        notebook.add(task_id)
        children_list = []
        down_events = EventDependsInfo.get_downstream_event_depends(dag_id=task_id)
        for down in down_events:
            child = TaskService.get_child_dependencie(down.dag_id,notebook)
            if child:
                children_list.append(child)
        return {'name':task_id,'children':children_list}

    @staticmethod
    def get_ti_by_instance_id(task_name,instance_id,session):
        etm = EventTriggerMapping.get_etm(uuid=instance_id,session=session)
        if etm and etm.execution_date:
            tis = TaskInstance.find_by_execution_date(dag_id=task_name,task_id=task_name,execution_date=etm.execution_date.replace(tzinfo=beijing),session=session)
            if tis:
                return tis[0]
        return None