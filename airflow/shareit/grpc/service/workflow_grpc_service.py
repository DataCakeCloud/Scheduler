# -*- coding: utf-8 -*-
# encoding: utf-8
import base64
import copy
import json
import time
import traceback

from croniter import croniter
from google.protobuf import json_format

from airflow import LoggingMixin
from airflow.shareit.constant.taskInstance import PIPELINE
from airflow.shareit.constant.trigger import TRI_PIPELINE
from airflow.shareit.grpc.TaskServiceApi import WorkflowServiceApi_pb2,WorkflowServiceApi_pb2_grpc
from airflow.shareit.grpc.TaskServiceApi.entity import CommonResponse_pb2,CommonResponse_pb2_grpc
from airflow.shareit.models.dag_dataset_relation import DagDatasetRelation
from airflow.shareit.models.event_depends_info import EventDependsInfo
from airflow.shareit.models.sensor_info import SensorInfo
from airflow.shareit.models.task_desc import TaskDesc
from airflow.shareit.models.trigger import Trigger
from airflow.shareit.models.workflow import Workflow
from airflow.shareit.models.workflow_instance import WorkflowInstance
from airflow.shareit.models.workflow_task_relation import WorkflowTaskRelation
from airflow.shareit.service.dataset_service import DatasetService
from airflow.shareit.service.task_service import TaskService
from airflow.shareit.service.workflow_service import WorkflowService
from airflow.shareit.trigger.trigger_helper import TriggerHelper
from airflow.shareit.utils.date_calculation_util import DateCalculationUtil
from airflow.shareit.utils.date_util import DateUtil
from airflow.shareit.utils.granularity import Granularity
from airflow.shareit.utils.grpc_util import provide_userinfo
from airflow.shareit.utils.state import State
from airflow.shareit.utils.task_cron_util import TaskCronUtil
from airflow.shareit.utils.task_manager import get_dag,get_dag_by_soure_json
from airflow.shareit.utils.task_util import TaskUtil
from airflow.utils import timezone
from airflow.utils.db import provide_session
from airflow.utils.timezone import beijing, make_naive, utcnow
from datetime import datetime,timedelta
from airflow.shareit.grpc.TaskServiceApi.entity import UserInfo_pb2

class WorkflowGrpcService(WorkflowServiceApi_pb2_grpc.WorkflowRpcService,LoggingMixin):

    @provide_userinfo
    def online(self, request, context,user_info=None):
        self.log.info("grpc online workflow ,request : {}".format(json.dumps(json_format.MessageToDict(request, including_default_value_fields=True))))
        workflow_id = request.workflowId
        workflow_name = request.workflowName
        granularity = request.granularity
        crontab = request.crontab
        version = request.version
        jobs = list(request.jobs)
        tenant_id = user_info.tenantId
        try :
            self.online_workflow(workflow_id=workflow_id, workflow_name=workflow_name, granularity=granularity,
                                 crontab=crontab, version=version, jobs=jobs, tenant_id=tenant_id)
        except Exception as e:
            print e
            # self.log.error("grpc: workflow online failed " + str(e), exc_info=True)
            return CommonResponse_pb2.CommonResponse(code=1,message=str(e))
        return CommonResponse_pb2.CommonResponse(code=0,message='success')

    @provide_session
    def online_workflow(self, workflow_id, workflow_name, granularity, crontab, version, jobs, tenant_id=1,
                        session=None):
        transaction = True
        ds_task_id_list = []
        root_ds_task_id_list = []
        old_ds_task_id_list = []
        relations = []
        ds_task_name_set = set()
        # need_kill_task = False
        is_new_workflow = True
        is_new_task = True
        try:
            old_workflow = Workflow.get_workflow(workflow_id=workflow_id,tenant_id=tenant_id,session=session)
            if old_workflow:
                is_new_workflow = False
                old_ds_task_id_list = old_workflow.get_task_ids()
                # if old_workflow.crontab != crontab:
                #     need_kill_task = True

            one_task_id = ''# 用来判断是新任务还是老任务
            for task in jobs:
                if not task.HasField('taskCode'):
                    continue
                primary_task_name = '{}_{}'.format(str(tenant_id),str(task.taskId))
                ds_task_name_set.add(primary_task_name)
                one_task_id = task.taskId

            if is_new_workflow:
                one_task = TaskDesc.get_task_with_tenant(ds_task_id=one_task_id,tenant_id=tenant_id,session=session)
                if one_task:
                    is_new_task = False

            for task in jobs:
                if not task.HasField('taskCode'):
                    continue
                ds_task_id_list.append(str(task.taskId))
                ds_task_id = task.taskId
                task_code = task.taskCode
                # 调度过程中的taskname,由租户id+dstaskid组成
                task_name = '{}_{}'.format(str(tenant_id),str(task.taskId))
                ds_task_name = task.taskName
                task_code = self.proto_task_code_to_json(task_code)
                sub_relation,is_root = self.updata_task(task_name=task_name,ds_task_name=ds_task_name, ds_task_id=ds_task_id, task_code=task_code, workflow_id=workflow_id,is_new_workflow=is_new_workflow,
                                 ds_task_name_set=ds_task_name_set,crontab=crontab,granularity=granularity,tenant_id=tenant_id,transaction=transaction, session=session)
                relations.extend(sub_relation)
                if is_root:
                    root_ds_task_id_list.append(str(ds_task_id))

            offline_ds_task_id_list = list(set(old_ds_task_id_list).difference(set(ds_task_id_list)))
            for ds_task_id in offline_ds_task_id_list:
                TaskService.set_paused_transaction_by_ds_task_id(ds_task_id,True,tenant_id=tenant_id,session=session)
                TaskDesc.remove_workflow_info(ds_task_id=ds_task_id,tenant_id=tenant_id,transaction=transaction,session=session)

            Workflow.add_and_update(workflow_id=workflow_id, workflow_name=workflow_name, granularity=granularity,
                                    task_ids=','.join(ds_task_id_list), crontab=crontab, version=version,
                                    tenant_id=tenant_id,
                                    transaction=transaction, session=session)
            WorkflowTaskRelation.add_and_update(workflow_id=workflow_id, version=version,
                                                task_ids=','.join(ds_task_id_list),
                                                root_task_ids=','.join(root_ds_task_id_list),
                                                relations=json.dumps(relations), tenant_id=tenant_id,
                                                transaction=transaction,
                                                session=session)


            # 添加下一次的实例
            cron = croniter(crontab, utcnow().replace(tzinfo=beijing))
            cron.tzinfo = beijing
            # 删除未来的实例
            WorkflowInstance.delete_by_execution_date_range(workflow_id,utcnow(),tenant_id=tenant_id,transaction=transaction,session=session)
            '''
            如果是新建工作流.
            1.新建工作流,纯历史任务导入的, 生成下一次周期的实例
            2.新建工作流,纯新建任务的,生成前一次周期的实例
            '''
            if is_new_workflow and is_new_task:
                execution_date = cron.get_prev(datetime)
            else :
                execution_date = cron.get_next(datetime)
            WorkflowInstance.add_wi(workflow_id=workflow_id, execution_date=execution_date,
                                    version=version,tenant_id=tenant_id,transaction=transaction,session=session)


            session.commit()
        except Exception as e:
            session.rollback()
            s = traceback.format_exc()
            self.log.error(s)
            raise e
        return

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
    def _get_value_from_dict(dict_obj, key):
        if not isinstance(dict_obj, dict):
            return None
        return dict_obj[key] if key in dict_obj.keys() else None

    def add_temp_debug_task(self, task_name, ds_task_id, task_code, tenant_id=1,user=None, is_online=True, is_temp=True,all_workflow_task=None,
                            chat_id=None, transaction=False, session=None):
        try:
            if isinstance(task_code, unicode) or isinstance(task_code, str):
                task_code = json.loads(task_code)
            emails = self._get_value_from_dict(task_code, 'emails')
            output_datasets = self._get_value_from_dict(task_code, 'output_datasets')
            extra_params = self._get_value_from_dict(task_code, 'extra_params')
            task_items = self._get_value_from_dict(task_code, 'task_items')
            start_date = None
            end_date = None
            event_depends = self._get_value_from_dict(task_code, 'event_depends')
            depend_types = self._get_value_from_dict(task_code, 'depend_types')
            task_version = self._get_value_from_dict(task_code, 'version')

            # 判断任务是否已经存在
            task = TaskDesc.get_task(task_name=task_name)
            if task :
                self.log.error('Debugging task {} already exists'.format(task_name))
                raise ValueError('Debugging task {} already exists'.format(task_name))
            else :
                task = TaskDesc()

            event_depend_flag = False
            if 'event' in str(depend_types):
                event_depend_flag = True

            task_gra = Granularity.gra_priority_map['daily']

            task.sync_task_desc_to_db(task_name=task_name,
                                      owner=user,
                                      retries=0,
                                      email=emails,
                                      start_date=start_date,
                                      end_date=end_date,
                                      output_datasets=output_datasets,
                                      extra_param=extra_params,
                                      tasks=task_items,
                                      source_json=task_code,
                                      crontab='0 0 * * *',
                                      granularity=task_gra,
                                      task_version=task_version,
                                      workflow_name='',
                                      is_paused=False,
                                      is_temp=is_temp,
                                      ds_task_name=None,
                                      ds_task_id=ds_task_id,
                                      tenant_id=tenant_id,
                                      session=session
                                      )

            # 插入测试任务的产出数据集
            DatasetService.update_task_dataset_relation(dag_id=task_name,
                                                        metadata_id=task_name,
                                                        dataset_type=False,
                                                        granularity=task_gra,
                                                        offset=-1,
                                                        transaction=transaction,
                                                        session=session)
            if event_depend_flag and event_depends:
                for e_depend in event_depends:
                    depend_info = EventDependsInfo()
                    if all_workflow_task and e_depend['depend_id'] not in all_workflow_task:
                        # 不属于工作流内的依赖直接过滤掉
                        continue

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
                        type=e_depend['type'],
                        date_calculation_param=date_calculation_param,
                        depend_id='{}_{}'.format(e_depend['depend_id'],chat_id),
                        depend_gra=e_depend['depend_gra'],
                        transaction=transaction,
                        session=session
                    )

            if is_online:
                dag = get_dag_by_soure_json(task_code,task_name)
                # dag = get_dag(task_name)
                dag.sync_to_db_transaction(is_online=is_online,transaction=transaction, session=session)
            if not transaction:
                session.commit()
        except Exception as e:
            self.log.error(traceback.format_exc())
            raise e

    def updata_task(self, task_name,ds_task_name,ds_task_id, task_code, workflow_id, tenant_id=1,is_new_workflow=True, is_online=True,
                    is_temp=False, ds_task_name_set=None, crontab=None, granularity=None, transaction=False,
                    session=None):
        try:
            if isinstance(task_code, unicode) or isinstance(task_code, str):
                task_code = json.loads(task_code)
            # TaskService._new_task_pre_check(task_code)
            emails = self._get_value_from_dict(task_code, 'emails')
            owner = self._get_value_from_dict(task_code, 'owner')
            retries = self._get_value_from_dict(task_code, 'retries')
            input_datasets = self._get_value_from_dict(task_code, 'input_datasets')
            output_datasets = self._get_value_from_dict(task_code, 'output_datasets')
            deadline = None
            property_weight = None
            extra_params = self._get_value_from_dict(task_code, 'extra_params')
            task_items = self._get_value_from_dict(task_code, 'task_items')
            start_date = self._get_value_from_dict(task_code, 'start_date')
            end_date = self._get_value_from_dict(task_code, 'end_date')
            event_depends = self._get_value_from_dict(task_code, 'event_depend')
            trigger_param = self._get_value_from_dict(task_code, 'trigger_param')
            depend_types = self._get_value_from_dict(task_code, 'depend_types')
            task_version = self._get_value_from_dict(task_code, 'version')
            user_group_info = self._get_value_from_dict(task_code,'user_group_info')
            template_code = self._get_value_from_dict(task_code,'template_code')
            is_paused = None
            # 最后需要返回的relation
            relation = []

            # 是不是工作流内的root节点
            is_root = True

            # 是否有数据依赖
            data_depend_flag = False
            # 是否有事件依赖
            event_depend_flag = False
            if 'dataset' in str(depend_types):
                data_depend_flag = True
            if 'event' in str(depend_types):
                event_depend_flag = True
            task_gra = Granularity.gra_priority_map[granularity]
            # # 任务的触发类型
            # crontab = cronjob

            # 如果是新建工作流, 要用任务名去判断任务存不存在,因为旧的任务没有dstaskid
            # if is_new_workflow:
            #     # 判断任务是否已经存在
            #     task = TaskDesc.get_task(task_name=task_name,session=session)
            #     if not task:
            #         task = TaskDesc()
            # else:
            # 判断任务是否已经存在
            task = TaskDesc.get_task_with_tenant(tenant_id=tenant_id,ds_task_id=ds_task_id,session=session)
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
                                      deadline=deadline,
                                      property_weight=property_weight,
                                      extra_param=extra_params,
                                      tasks=task_items,
                                      source_json=task_code,
                                      crontab=crontab,
                                      granularity=task_gra,
                                      task_version=task_version,
                                      workflow_name=workflow_id,
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

            in_ids = DatasetService.get_all_input_datasets(dag_id=task_name,session=session)
            out_ids = DatasetService.get_all_output_datasets(dag_id=task_name,session=session)

            if is_temp:
                temp_ts = DateUtil.get_timestamp(timezone.utcnow().replace(tzinfo=beijing))
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
                                                                transaction=transaction,
                                                                session=session)
            else:
                # update task_dataset_relation to db
                if data_depend_flag:
                    from airflow.shareit.models.trigger import Trigger

                    tris = Trigger.get_untreated_triggers(dag_id=task_name, session=session)
                    execution_dates = [tri.execution_date for tri in tris if
                                       DateUtil.date_compare(timezone.utcnow().replace(tzinfo=beijing),
                                                             tri.execution_date)]

                    for exe_date in execution_dates:
                        SensorInfo.delete_by_execution_date(task_name=task_name, execution_date=exe_date, transaction=transaction,
                                                            session=session)

                    for input_data in task.get_input_datasets():
                        check_path = input_data.get('check_path',None)
                        ready_time = input_data.get('ready_time',None)
                        if input_data['id'] in in_ids:
                            in_ids.remove(input_data['id'])
                        format_gra = Granularity.formatGranularity(input_data['granularity'])
                        if format_gra:
                            granularity = Granularity.gra_priority_map[format_gra]
                        else:
                            granularity = task_gra
                            # raise ValueError("[DataPipeline] illegal granularity in input dataset " + input_data['id'])
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
                                                                    detailed_dependency=None,
                                                                    detailed_gra=None,
                                                                    date_calculation_param=date_calculation_param,
                                                                    transaction=transaction,
                                                                    session=session
                                                                    )

                        datasets = DagDatasetRelation.get_active_downstream_dags(metadata_id=input_data['id'],
                                                                                 session=session)
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
                                                               cluster_tags=task_items[0]["cluster_tags"])
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
                                                           completion_time=completion_time,
                                                           transaction=transaction,
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
                EventDependsInfo.delete(dag_id=task_name, transaction=transaction,session=session)
                if event_depend_flag:
                    # 再添加
                    for e_depend in event_depends:
                        cur_relation = {}
                        cur_relation['source'] = str(e_depend['task_id'])
                        cur_relation['target'] = str(ds_task_id)
                        depend_id = '{}_{}'.format(str(tenant_id),str(e_depend['task_id']),)
                        if depend_id not in ds_task_name_set:
                            cur_relation['sourceIsOutside'] = True
                        else :
                            is_root = False
                            cur_relation['sourceIsOutside'] = False
                        relation.append(cur_relation)


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
                    Trigger.delete_by_execution_date_range(task_name=task_name, start_date=make_naive(now_date),session=session)
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
                # sync to airflow, and open the dag
            if is_online:
                from airflow.models import DagModel
                dag = get_dag_by_soure_json(task_code,task_name)
                # dag = get_dag(task_name)
                dag.sync_to_db_transaction(is_online=is_online,transaction=transaction, session=session)
                # dm = DagModel.get_dagmodel(task_name, session=session)
                # if dm:
                #     dm.is_paused = False
                #     session.merge(dm)
            if not transaction:
                session.commit()
            return relation,is_root
        except Exception as e:
            s = traceback.format_exc()
            self.log.error(s)
            raise e

    @provide_userinfo
    def offline(self, request, context,user_info=None):
        workflow_id = request.workflowId
        tenant_id= user_info.tenantId
        try :
            self.offline_workflow(workflow_id=workflow_id,tenant_id=tenant_id)
        except Exception as e:
            self.log.error("grpc: workflow offline failed " + str(e), exc_info=True)
            return CommonResponse_pb2.CommonResponse(code=1, message=str(e))
        return CommonResponse_pb2.CommonResponse(code=0, message='success')

    @provide_session
    def offline_workflow(self,workflow_id,tenant_id=1,session=None):
        transaction = True
        # 下线工作流就是将所有工作流内的任务下线
        try:
            workflow = Workflow.get_workflow(workflow_id=workflow_id,tenant_id=tenant_id,session=session)
            if workflow:
                WorkflowInstance.stop_instance(workflow_id=workflow_id, tenant_id=tenant_id,transaction=True, session=session)
                tasks = workflow.get_tasks()
                for task in tasks:
                    TaskService.set_paused_transaction(task_name=task.task_name,is_paused=True,transaction=transaction,session=session)
                workflow.is_online = False
                session.merge(workflow)
                session.commit()
        except Exception as e:
            self.log.error(traceback.format_exc())
            session.rollback()
            raise e

    @provide_userinfo
    def deleteWorkflow(self, request, context,user_info=None):
        workflow_id = request.workflowId
        tenant_id = user_info.tenantId
        try:
            self.delete_workflow(workflow_id=workflow_id,tenant_id=tenant_id)
        except Exception as e:
            self.log.error("grpc: workflow delete failed " + str(e), exc_info=True)
            return CommonResponse_pb2.CommonResponse(code=1, message=str(e))
        return CommonResponse_pb2.CommonResponse(code=0,message='success')

    @provide_session
    def delete_workflow(self,workflow_id,tenant_id=1,session=None):
        transaction = True
        try:
            workflow = Workflow.get_workflow(workflow_id=workflow_id,tenant_id=tenant_id,session=session)
            if workflow:
                tasks = workflow.get_tasks()
                for task in tasks:
                    TaskService.delete_task_transaction(task.task_name,transaction=transaction,session=session)
                Workflow.delete_workfow(workflow_id=workflow_id,tenant_id=tenant_id,transaction=transaction,session=session)
                session.commit()
        except Exception as e:
            session.rollback()
            self.log.error(traceback.format_exc())
            raise e
    @provide_userinfo
    def debugTask(self, request, context,user_info=None):
        self.log.info("grpc debug tasks ,request : {}".format(
            json.dumps(json_format.MessageToDict(request, including_default_value_fields=True))))
        chat_id = request.chatId
        jobs = list(request.scheduleJob)
        user_name = request.userName
        tenant_id = user_info.tenantId
        try:
            self.debug_task(jobs, user_name, chat_id,tenant_id=tenant_id)
        except Exception as e:
            self.log.error("grpc: workflow debug task failed " + str(e), exc_info=True)
            return CommonResponse_pb2.CommonResponse(code=1, message=str(e))
        return CommonResponse_pb2.CommonResponse(code=0, message='success')


    @provide_session
    def debug_task(self,jobs,user_name,chat_id,tenant_id=1,session=None):
        transaction=True
        execution_date = DateUtil.format_millisecond(timezone.utcnow().replace(tzinfo=beijing))

        ds_task_name_set = set()
        external_conf = {'chat_id':chat_id}
        try:
            for task in jobs:
                if not task.HasField('taskCode'):
                    continue
                ds_task_name_set.add(task.taskName)

            for task in jobs:
                if not task.HasField('taskCode'):
                    continue
                ds_task_id = task.taskId
                task_name = '{}_{}_{}'.format(str(tenant_id),str(ds_task_id), chat_id)
                task_code = task.taskCode
                task_code.name = '{}_{}_{}'.format(str(tenant_id),str(ds_task_id),chat_id)
                task_code = self.proto_task_code_to_json(task_code)
                self.add_temp_debug_task(task_name, ds_task_id, task_code, user=user_name,all_workflow_task=ds_task_name_set, is_online=True,
                                    is_temp=True,chat_id=chat_id,
                                    transaction=transaction, session=session)
                Trigger.sync_trigger_to_db(dag_id=task_name,trigger_type=TRI_PIPELINE,execution_date=execution_date,state=State.WAITING,priority=5,transaction=transaction,session=session)
                session.flush()
                dag = get_dag(task_name,session=session)
                sub_ex_conf = copy.copy(external_conf)
                sub_ex_conf['ds_task_id'] = ds_task_id
                TriggerHelper.create_taskinstance(dag.task_dict, execution_date, task_type=PIPELINE,
                                                  external_conf=json.dumps(sub_ex_conf),is_debug=True, transaction=True,
                                                  session=session)
            session.commit()
        except Exception as e:
            session.rollback()
            self.log.error(traceback.format_exc())
            raise e

    @provide_userinfo
    def stopTask(self, request, context,user_info=None):
        chat_id = request.chatId
        ds_task_id_list = list(request.taskId)
        # task_name_list = list(request.taskName)
        user_name = request.userName
        tenant_id = user_info.tenantId
        new_task_name_list = []
        for ds_task_id in ds_task_id_list:
            new_task_name_list.append('{}_{}_{}'.format(str(tenant_id),str(ds_task_id),chat_id))
        try :
            self.stop_task(new_task_name_list)
        except Exception as e:
            self.log.error("grpc: workflow debug task failed " + str(e), exc_info=True)
            return CommonResponse_pb2.CommonResponse(code=1, message=str(e))
        return CommonResponse_pb2.CommonResponse(code=0,message='success')

    @provide_session
    def stop_task(self,task_name_list,session=None):
        transaction = True
        try:
            for task_name in task_name_list:
                TaskService.set_paused_transaction(task_name,is_paused=False,transaction=transaction,session=session)

            for task_name in task_name_list:
                TaskService.delete_task_transaction(task_name,transaction=transaction,session=session)

            session.commit()
        except Exception as e:
            session.rollback()
            self.log.error(traceback.format_exc())
            raise e
