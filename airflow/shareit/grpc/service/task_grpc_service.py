# -*- coding: utf-8 -*-

import json
import time
import random
import logging
from collections import defaultdict

import pendulum
import threading

from croniter import croniter_range

from airflow import macros
from airflow.shareit.grpc.TaskServiceApi import TaskSchedulerApi_pb2, TaskSchedulerApi_pb2_grpc
from airflow.shareit.models.alert import Alert
from airflow.shareit.models.fast_execute_task import FastExecuteTask
from airflow.shareit.models.task_desc import TaskDesc
from airflow.shareit.service.task_service import TaskService
from airflow.shareit.service.dataset_service import DatasetService
from airflow.shareit.service.diagnosis_service import DiagnosisService
from airflow.shareit.utils.airflow_date_param_transform import AirlowDateParamTransform
from airflow.shareit.utils.date_util import DateUtil
from airflow.shareit.utils.granularity import Granularity
from airflow.shareit.utils.grpc_util import provide_userinfo
from airflow.shareit.utils.state import State
from airflow.shareit.utils.uid.uuid_generator import UUIDGenerator
from airflow.utils import timezone
from airflow.utils.timezone import beijing
from google.protobuf import json_format

uuid_generator = UUIDGenerator()

def fast_execute(ds_task_id, task_name, task_code, callback_url, extra_info,uniq_id,tenant_id=1,):
    # extra_info_dict = json.loads(extra_info.lstrip("'").rstrip("'"))
    # if 'more_graphic_id' in extra_info_dict.keys():
    #     uniq_id = extra_info_dict['more_graphic_id']
    # else:
    #     uniq_id = DateUtil.get_timestamp(DateUtil.format_millisecond(timezone.utcnow().replace(tzinfo=beijing)))

    if task_name:
        try:
            '''
                临时执行任务的原理是，生成一个新的任务并且注册到task_desc中，将新任务的名字保存在taskinstance的extenal_conf中
                在taskinstance开始执行时，获取真实要执行的任务名，并且get对应的task内容
                为什么这样做？因为临时任务可能是同一时间调起多次的，他们的任务实际内容可能不同，但task_desc本身只保存一份任务的内容，
                这就导致当同时调起多个临时任务时，他们的执行内容都是相同的，为了满足这种情况，我们保存每次临时任务的内容，在执行时去获取
                每个taskinstance应该执行的那个任务
            '''
            real_task_name = '{}_{}'.format(str(tenant_id),str(ds_task_id))
            new_task_name = '{}_{}'.format(str(ds_task_id), str(uniq_id))
            ex_conf_dict = {'actual_dag_id': new_task_name}
            callback_dict = {'callback_url': callback_url, 'callback_id': ds_task_id, 'extra_info': extra_info}
            ex_conf_dict['callback_info'] = callback_dict
            external_conf = json.dumps(ex_conf_dict)
            TaskService.update_task(task_id=new_task_name,ds_task_name=task_name, ds_task_id=ds_task_id,task_code=task_code, is_temp=True)
            FastExecuteTask.add_and_update(task_name=new_task_name, real_task_name=real_task_name,external_conf=external_conf)
            # '''
            #     注意这里执行的仍是原有的任务名！虽然实际上是两个任务，但是对于用户来说只有一个任务
            # '''
            # TaskService.task_execute(task_name=task_name, execution_date=execution_date,external_conf=external_conf)
            # Alert.add_alert_job(task_name=task_name,execution_date=execution_date)
        except Exception as e:
            logging.error("fast_execute error code:2 " + str(e), exc_info=True)
            return


class TaskServiceGrpcService(TaskSchedulerApi_pb2_grpc.TaskSchedulerRpcApiServicer):
    @provide_userinfo
    def getTaskRenderInfo(self, request, context, user_info=None):
        """
        原接口：/render
        :return: 获取render信息
        """
        execution_date = request.executionDate
        content = request.content
        ds_task_name = request.taskName
        if ds_task_name:
            tenant_id = user_info.tenantId if user_info.HasField('tenantId') else 1
            tenant_task = TaskDesc.get_task_with_tenant(tenant_id=tenant_id, ds_task_name=ds_task_name)
            if tenant_task:
                task_name = tenant_task.task_name
            else:
                logging.error('/render code:1  there is no task_name : tenant_id={}, ds_task_name={}'.format(str(tenant_id), str(ds_task_name)), exc_info=True)
                return TaskSchedulerApi_pb2.TaskCommonResponse(code=0, data='{}', message=None)
        else:
            task_name = None

        context = {}
        if execution_date:
            try:
                execution_date = pendulum.parse(execution_date)
                context["execution_date"] = execution_date
                context["macros"] = macros
                res = TaskService.render_api(content=content, context=context, task_name=task_name)
            except Exception as e:
                logging.error('/render code:2 ' + str(e), exc_info=True)
                return TaskSchedulerApi_pb2.TaskCommonResponse(code=2, data=str({'state': 'FAILED'}), message=str(e))
            return TaskSchedulerApi_pb2.TaskCommonResponse(code=0, data=res, message=None)
        else:
            logging.error('/render code:1 Missing required parameters!', exc_info=True)
            return TaskSchedulerApi_pb2.RenderRequest(code=2, data='state: FAILED', message=None)

    @provide_userinfo
    def dateTransform(self, request, context, user_info=None):
        """
        :原接口：/task/date/transform
        :return: 对airflow的日期做转换
        """
        is_sql = request.isSql
        task_gra = request.taskGra
        airflow_crontab = request.airflowCrontab
        airflow_content = request.airflowContent
        is_data_trigger = request.isDataTrigger
        tenant_id = user_info.tenantId if user_info.HasField('tenantId') else 1
        try:
            data = AirlowDateParamTransform.date_transform(is_data_trigger=is_data_trigger,
                                                           airflow_crontab=airflow_crontab, task_gra=task_gra,
                                                           airflow_content=airflow_content, is_sql=is_sql)
        except Exception as e:
            logging.error('/task/date/transform code:2 ' + str(e), exc_info=True)
            return TaskSchedulerApi_pb2.TaskCommonResponse(code=2, data=str({'state': 'FAILED'}), message=str(e))
        return TaskSchedulerApi_pb2.TaskCommonResponse(code=0, data=data, message=None)

    @provide_userinfo
    def datasetInfo(self, request, context, user_info=None):
        """
        :原接口：/dataset/info
        :return: 获取数据集信息
        """
        name = request.name  # name=metadataId
        tenant_id = user_info.tenantId if user_info.HasField('tenantId') else 1
        if isinstance(name, str) or isinstance(name, unicode):
            try:
                data = DatasetService.get_dataset_info(name=name.strip())
            except Exception as e:
                return TaskSchedulerApi_pb2.TaskCommonResponse(code=2, data=str({'state': 'FAILED'}), message=str(e))
            return TaskSchedulerApi_pb2.TaskCommonResponse(code=0, data=str(json.dumps(data)), message=None)
        else:
            logging.error('/dataset/info code:1 Missing required parameters!', exc_info=True)
            return TaskSchedulerApi_pb2.TaskCommonResponse(code=2, data=str({'state': 'FAILED'}),
                                                           message='Missing required parameters or type is error!')

    @provide_userinfo
    def datePreview(self, request, context, user_info=None):
        """
        :原接口：/task/date/preview
        :return: 日期预览
        """
        task_gra = request.taskGra
        task_depend = request.taskDepend
        data_depend = request.dataDepend
        task_crontab = request.taskCrontab
        tenant_id = user_info.tenantId if user_info.HasField('tenantId') else 1
        try:
            res = TaskService.get_date_preview(task_gra=task_gra, task_crontab=task_crontab, task_depend=task_depend,
                                               data_depend=data_depend,tenant_id=tenant_id)
        except Exception as e:
            logging.error('/task/date/preview code:2 ' + str(e), exc_info=True)
            return TaskSchedulerApi_pb2.TaskCommonResponse(code=2, data=str({'state': 'FAILED'}), message=str(e))
        return TaskSchedulerApi_pb2.TaskCommonResponse(code=0, data=str(json.dumps(res)), message=None)

    @provide_userinfo
    def setSuccess(self, request, context, user_info=None):
        """
        :原接口：/success
        :return: 设置success
        """
        ds_task_name = request.name
        execution_date = request.executionDate
        tenant_id = user_info.tenantId if user_info.HasField('tenantId') else 1
        tenant_task = TaskDesc.get_task_with_tenant(tenant_id=tenant_id, ds_task_name=ds_task_name)
        if tenant_task:
            name = tenant_task.task_name
        else:
            logging.error(
                '/success code:1  there is no task_name : tenant_id={}, ds_task_name={}'.format(
                    str(tenant_id), str(ds_task_name)), exc_info=True)
            return TaskSchedulerApi_pb2.TaskCommonResponse(code=0, data='{}', message=None)

        try:
            if name and execution_date:
                execution_dates = execution_date.split(',')
                for dt in execution_dates:
                    dt = pendulum.parse(dt)
                    TaskService.set_success(dag_id=name, execution_date=dt)
            else:
                logging.error('task name  not exists : {}'.format(name))
                return TaskSchedulerApi_pb2.TaskCommonResponse(code=0, data='{}', message=None)
        except Exception as e:
            logging.error('/success code:2 ' + str(e), exc_info=True)
            return TaskSchedulerApi_pb2.TaskCommonResponse(code=2, data=str({'state': 'FAILED'}), message=str(e))
        return TaskSchedulerApi_pb2.TaskCommonResponse(code=0, data=str({'state': 'SUCCESS'}), message=None)

    @provide_userinfo
    def setFail(self, request, context, user_info=None):
        """
        :原接口：/failed
        :return: 设置failed
        """
        ds_task_name = request.name
        execution_date = request.executionDate
        tenant_id = user_info.tenantId if user_info.HasField('tenantId') else 1
        tenant_task = TaskDesc.get_task_with_tenant(tenant_id=tenant_id, ds_task_name=ds_task_name)
        if tenant_task:
            name = tenant_task.task_name
        else:
            logging.error('/failed code:1  there is no task_name : tenant_id={}, ds_task_name={}'.format(
                str(tenant_id), str(ds_task_name)), exc_info=True)
            return TaskSchedulerApi_pb2.TaskCommonResponse(code=0, data='{}', message=None)

        try:
            if name and execution_date:
                execution_dates = execution_date.split(',')
                for dt in execution_dates:
                    dt = pendulum.parse(dt)
                    TaskService.set_failed(dag_id=name, execution_date=dt)
            else:
                logging.error('task name  not exists : {}'.format(name))
                return TaskSchedulerApi_pb2.TaskCommonResponse(code=0, data='{}', message=None)
        except Exception as e:
            logging.error('/failed code:2 ' + str(e), exc_info=True)
            return TaskSchedulerApi_pb2.TaskCommonResponse(code=2, data=str({'state': 'FAILED'}), message=str(e))
        return TaskSchedulerApi_pb2.TaskCommonResponse(code=0, data=str({'state': 'SUCCESS'}), message=None)

    @provide_userinfo
    def clear(self, request, context, user_info=None):
        """
        :原接口：/clear
        :return: 任务重跑（离线）
        """
        ds_task_name = request.taskName
        execution_date = request.executionDate
        is_check_upstream = request.isCheckUpstream
        tenant_id = user_info.tenantId if user_info.HasField('tenantId') else 1
        tenant_task = TaskDesc.get_task_with_tenant(tenant_id=tenant_id, ds_task_name=ds_task_name)
        ignore_check = not is_check_upstream
        if tenant_task:
            name = tenant_task.task_name
        else:
            logging.error('/clear code:1  there is no task_name : tenant_id={}, ds_task_name={}'.format(
                str(tenant_id), str(ds_task_name)), exc_info=True)
            return TaskSchedulerApi_pb2.TaskCommonResponse(code=0, data='{}', message=None)

        try:
            if name and execution_date:
                parse_dates = []
                execution_dates = execution_date.split(',')
                for dt in execution_dates:
                    dt = pendulum.parse(dt)
                    parse_dates.append(dt)

                count = TaskService.clear_instance(dag_id=name, execution_dates=parse_dates,ignore_check=ignore_check)
            else:
                logging.error('task name  not exists : {}'.format(name))
                return TaskSchedulerApi_pb2.TaskCommonResponse(code=0, data='{}', message=None)
        except Exception as e:
            logging.error('/clear code:2 ' + str(e), exc_info=True)
            return TaskSchedulerApi_pb2.TaskCommonResponse(code=2, data=str({'state': 'FAILED'}), message=str(e))
        return TaskSchedulerApi_pb2.TaskCommonResponse(code=0, data=str({'state': 'SUCCESS'}), message='{0} task instances have been cleared'.format(count))

    @provide_userinfo
    def getDagRunLatest(self, request, context, user_info=None):
        """
        :原接口：/dagruns/laststatus
        :return: 获取任务实例历史信息
        :todo: 接口废弃
        """
        names = request.names
        tenant_id = user_info.tenantId if user_info.HasField('tenantId') else 1
        if isinstance(names, str) or isinstance(names, unicode):
            try:
                name_list = names.split(',')
                name_tuple_list = self.get_task_name_tuple_list(tenant_id, name_list)
                data = TaskService.get_last_instance_info(name_tuple_list=name_tuple_list)
            except Exception as e:
                return TaskSchedulerApi_pb2.TaskCommonResponse(code=2, data=str({'state': 'FAILED'}), message=str(e))
            return TaskSchedulerApi_pb2.TaskCommonResponse(code=0, data=str(json.dumps(data)), message=None)
        else:
            logging.error('/dagruns/laststatus code:1 Missing required parameters!', exc_info=True)
            return TaskSchedulerApi_pb2.TaskCommonResponse(code=2, data=str({'state': 'FAILED'}), message='Missing required parameters or type is error!')

    @provide_userinfo
    def getTaskDiagnose(self, request, context, user_info=None):
        """
        :原接口：/task/diagnose
        :return: 运维页-检查上游
        """
        state = request.state
        ds_task_name = request.name
        execution_date = request.executionDate
        tenant_id = user_info.tenantId if user_info.HasField('tenantId') else 1
        tenant_task = TaskDesc.get_task_with_tenant(tenant_id=tenant_id, ds_task_name=ds_task_name)
        if tenant_task:
            task_id = tenant_task.task_name
        else:
            logging.error('/task/diagnose code:1  there is no task_name : tenant_id={}, ds_task_name={}'.format(
                str(tenant_id), str(ds_task_name)), exc_info=True)
            return TaskSchedulerApi_pb2.TaskCommonResponse(code=0, data='{}', message=None)

        try:
            execution_date = pendulum.parse(execution_date)
            execution_date = execution_date.replace(tzinfo=beijing)
            resp_data = DiagnosisService().diagnose_new(task_id, execution_date, state,ds_task_name=ds_task_name)
        except Exception as e:
            logging.error('/task/diagnose code:2 ' + str(e), exc_info=True)
            return TaskSchedulerApi_pb2.TaskCommonResponse(code=2, data=str({'state': 'FAILED'}), message=str(e))
        return TaskSchedulerApi_pb2.TaskCommonResponse(code=0, data=str(json.dumps(resp_data)), message=None)

    @provide_userinfo
    def taskDelete(self, request, context, user_info=None):
        """
        :原接口：/task/delete
        :return: 删除任务
        """
        ds_task_name = request.name
        tenant_id = user_info.tenantId if user_info.HasField('tenantId') else 1
        tenant_task = TaskDesc.get_task_with_tenant(tenant_id=tenant_id, ds_task_name=ds_task_name)
        if tenant_task:
            task_id = tenant_task.task_name
        else:
            logging.error('/dagruns/latest code:1  there is no task_name : tenant_id={}, ds_task_name={}'.format(
                str(tenant_id), str(ds_task_name)), exc_info=True)
            return TaskSchedulerApi_pb2.TaskCommonResponse(code=0, data='{}', message=None)

        try:
            if task_id:
                TaskService.delete_task(task_id=task_id)
            else:
                return TaskSchedulerApi_pb2.TaskCommonResponse(code=1, data=str({'state': 'FAILED'}), message='can not get correct param: name ')
        except ValueError as ve:
            logging.error('/task/delete code:3 ' + str(ve), exc_info=True)
            return TaskSchedulerApi_pb2.TaskCommonResponse(code=3, data=str({'state': 'FAILED'}), message=str(ve))
        except Exception as e:
            logging.error('/task/delete code:2 ' + str(e), exc_info=True)
            return TaskSchedulerApi_pb2.TaskCommonResponse(code=2, data=str({'state': 'FAILED'}), message=str(e))
        return TaskSchedulerApi_pb2.TaskCommonResponse(code=0, data=str({'state': 'SUCCESS'}), message=None)

    @provide_userinfo
    def taskUpdatename(self, request, context, user_info=None):
        """
        :原接口：/task/updatename
        :return: 更新任务的ds_task_name
        """
        old_name = request.oldName
        new_name = request.newName
        tenant_id = user_info.tenantId if user_info.HasField('tenantId') else 1
        # tenant_task = TaskDesc.get_task_with_tenant(tenant_id=tenant_id, ds_task_name=old_name)
        # if tenant_task:
        #     old_name = tenant_task.task_name
        # else:
        #     logging.error('/task/updatename code:1  there is no task_name : tenant_id={}, ds_task_name={}'.format(
        #         str(tenant_id), str(old_name)), exc_info=True)
        #     return TaskSchedulerApi_pb2.TaskCommonResponse(code=0, data='{}', message=None)
        try:
            TaskService.update_ds_task_name(old_name, new_name,tenant_id=tenant_id)
        except Exception as e:
            logging.error('/task/updatename code:2 ' + str(e), exc_info=True)
            return TaskSchedulerApi_pb2.TaskCommonResponse(code=2, data=str({'state': 'FAILED'}), message=str(e))
        return TaskSchedulerApi_pb2.TaskCommonResponse(code=0, data=str({'state': 'SUCCESS'}), message=None)

    @provide_userinfo
    def getLogUrl(self, request, context, user_info=None):
        """
        :ds: /task/logsui
        :pipeline：/logurl
        :return: 运维页--更多--查看日志(kibana日志url)
        """
        ds_task_name = request.name
        execution_date = request.executionDate
        tenant_id = user_info.tenantId if user_info.HasField('tenantId') else 1
        tenant_task = TaskDesc.get_task_with_tenant(tenant_id=tenant_id, ds_task_name=ds_task_name)
        if tenant_task:
            name = tenant_task.task_name
        else:
            logging.error('/task/logsui code:1  there is no task_name : tenant_id={}, ds_task_name={}'.format(
                str(tenant_id), str(ds_task_name)), exc_info=True)
            return TaskSchedulerApi_pb2.TaskCommonResponse(code=0, data='{}', message=None)

        if name and execution_date:
            try:
                execution_date = pendulum.parse(execution_date)
                log_url = TaskService.get_dag_run_log(dag_id=name, execution_date=execution_date)
            except Exception as e:
                logging.error('/logurl code:2 ' + str(e), exc_info=True)
                return TaskSchedulerApi_pb2.TaskCommonResponse(code=2, data=str({'state': 'FAILED'}), message=str(e))
            return TaskSchedulerApi_pb2.TaskCommonResponse(code=0, data=str({'logurl': log_url}), message=None)
        else:
            logging.error('/logurl code:1 Missing required parameters!', exc_info=True)
            return TaskSchedulerApi_pb2.TaskCommonResponse(code=0, data=str({'logurl': ''}), message=None)

    @staticmethod
    def get_task_name(tenant_id, task_ids):
        result = []
        for ds_task_name in task_ids:
            tenant_task = TaskDesc.get_task_with_tenant(tenant_id=tenant_id, ds_task_name=ds_task_name)
            if tenant_task:
                result.append(tenant_task.task_name)
        return result

    @staticmethod
    def get_task_name_tuple_list(tenant_id, task_ids):
        result = []
        for ds_task_name in task_ids:
            tenant_task = TaskDesc.get_task_with_tenant(tenant_id=tenant_id, ds_task_name=ds_task_name, specified_field=1)
            if tenant_task:
                result.append((tenant_task.task_name, tenant_task.ds_task_name))
        return result


    @provide_userinfo
    def taskBackfill(self, request, context, user_info=None):
        """
        :ds: /task/backfill
        :pipeline：/task/backfill
        :return: 首页-补数
        """
        task_ids = request.ids
        start_date = request.startDate
        end_date = request.endDate
        operator = request.operator
        core_task_name = request.coreTaskName
        is_send_notify = request.isSendNotify
        is_check_dependency = request.isCheckDependency
        ignore_check = not is_check_dependency
        label = request.label if request.label else 'default'
        tenant_id = user_info.tenantId if user_info.HasField('tenantId') else 1

        core_tenant_task = TaskDesc.get_task_with_tenant(tenant_id=tenant_id, ds_task_name=core_task_name)
        if core_tenant_task:
            core_task_name = core_tenant_task.task_name
        else:
            logging.error('/task/backfill code:1  there is no task_name : tenant_id={}, ds_task_name={}'.format(
                str(tenant_id), str(core_task_name)), exc_info=True)
            return TaskSchedulerApi_pb2.TaskCommonResponse(code=0, data='{}', message=None)

        if is_send_notify == -1:
            return TaskSchedulerApi_pb2.TaskCommonResponse(code=1, data={'state': 'WARNING'}, message='Wrong parameter type: is_send_notify should be string or bool')
        if is_check_dependency == -1:
            return TaskSchedulerApi_pb2.TaskCommonResponse(code=1, data={'state': 'WARNING'}, message='Wrong parameter type: is_check_dependency should be string or bool')

        try:
            task_ids = ['{}_{}'.format(str(tenant_id), str(id)) for id in task_ids.strip().split(',')] if task_ids.strip() else []
            start_date = pendulum.parse(start_date)
            end_date = pendulum.parse(end_date)
            TaskService.backfill(sub_task_name_list=task_ids,
                                 core_task_name=core_task_name,
                                 operator=operator, ignore_check=ignore_check,
                                 start_date=start_date, end_date=end_date,
                                 is_send_notify=is_send_notify,label=label)
        except ValueError as ve:
            logging.error('/task/backfill code:1 ' + str(ve), exc_info=True)
            return TaskSchedulerApi_pb2.TaskCommonResponse(code=1, data=str({'state': 'WARNING'}), message=str(ve))
        except Exception as e:
            logging.error('/task/backfill code:2 ' + str(e), exc_info=True)
            return TaskSchedulerApi_pb2.TaskCommonResponse(code=2, data=str({'state': 'FAILED'}), message=str(e))
        return TaskSchedulerApi_pb2.TaskCommonResponse(code=0, data=str({'state': 'SUCCESS'}), message=None)

    def progressReminder(self, request, context):
        """
        :原接口：/backfill/progress/reminder
        :return: 补数行为记录
        """
        user_action_id = request.userActionId
        try:
            TaskService.send_backfill_progress_reminder(user_action_id=int(user_action_id))
        except Exception as e:
            logging.error('/backfill/progress/reminder code:2 ' + str(e), exc_info=True)
            return TaskSchedulerApi_pb2.TaskCommonResponse(code=2, data=str({'state': 'FAILED'}), message=str(e))
        return TaskSchedulerApi_pb2.TaskCommonResponse(code=0, data=str({'state': 'SUCCESS'}), message=None)

    @provide_userinfo
    def getInstanceRelation(self, request, context, user_info=None):
        """
        :原接口：/task/instance/relation
        :return: 首页-实例血缘
        :todo: 该接口被舍弃
        """
        level = request.level
        core_task = request.coreTask
        execution_date = request.executionDate
        is_downstream = request.isDownstream  # default=True
        tenant_id = user_info.tenantId if user_info.HasField('tenantId') else 1
        tenant_task = TaskDesc.get_task_with_tenant(tenant_id=tenant_id, ds_task_name=core_task)
        if tenant_task:
            core_task = tenant_task.task_name
        else:
            logging.error('/task/instance/relation code:1  there is no task_name : tenant_id={}, ds_task_name={}'.format(
                str(tenant_id), str(core_task)), exc_info=True)
            return TaskSchedulerApi_pb2.TaskCommonResponse(code=0, data='{}', message=None)

        if core_task and execution_date:
            try:
                execution_date = pendulum.parse(execution_date)
                level = int(level)
                res = TaskService.instance_relation_shell(core_task=core_task,
                                                          execution_date=execution_date,
                                                          level=level - 1,
                                                          is_downstream=is_downstream)
            except Exception as e:
                return TaskSchedulerApi_pb2.TaskCommonResponse(code=2, data={'state': 'FAILED'}, message=str(e))
            return TaskSchedulerApi_pb2.TaskCommonResponse(code=0, data=res, message=None)
        else:
            logging.error('/task/instance/relation code:1 Missing required parameters!', exc_info=True)
            return TaskSchedulerApi_pb2.TaskCommonResponse(code=2, data={'state': 'FAILED'}, message='Missing required parameters or type is error!')

    @provide_userinfo
    def pauseTask(self, request, context, user_info=None):
        """
        :ds: /task/onlineAndOffline
        :pipeline：/paused
        :return: 任务上下线
        """
        ds_task_name = request.name
        is_online = request.isOnline    # False表示下线，True不起作用
        is_paused = False if is_online is True else True
        tenant_id = user_info.tenantId if user_info.HasField('tenantId') else 1
        tenant_task = TaskDesc.get_task_with_tenant(tenant_id=tenant_id, ds_task_name=ds_task_name)
        if tenant_task:
            name = tenant_task.task_name
        else:
            logging.error('/task/onlineAndOffline code:1  there is no task_name : tenant_id={}, ds_task_name={}'.format(
                str(tenant_id), str(ds_task_name)), exc_info=True)
            return TaskSchedulerApi_pb2.TaskCommonResponse(code=0, data='{}', message=None)

        try:
            TaskService.set_paused(task_name=name, is_paused=is_paused)
        except ValueError as ve:
            return TaskSchedulerApi_pb2.TaskCommonResponse(code=1, data=str({'state': 'WARNING'}), message=str(ve))
        except Exception as e:
            logging.error('/paused code:2 ' + str(e), exc_info=True)
            return TaskSchedulerApi_pb2.TaskCommonResponse(code=2, data=str({'state': 'FAILED'}), message=str(e))
        return TaskSchedulerApi_pb2.TaskCommonResponse(code=0, data=str({'state': 'SUCCESS'}), message=None)

    @provide_userinfo
    def getLast7Status(self, request, context, user_info=None):
        """
        :原接口：/dagruns/last7instancestatus
        :return: 获取last7状态
        """
        names = request.names
        tenant_id = user_info.tenantId if user_info.HasField('tenantId') else 1
        if isinstance(names, str) or isinstance(names, unicode):
            try:
                name_list = names.split(',')
                name_tuple_list = self.get_task_name_tuple_list(tenant_id, name_list)
                data = TaskService.get_result_last_7_instance(name_tuple_list=name_tuple_list)
            except Exception as e:
                return TaskSchedulerApi_pb2.TaskCommonResponse(code=2, data=str({'state': 'FAILED'}), message=str(e))
            return TaskSchedulerApi_pb2.TaskCommonResponse(code=0, data=str(json.dumps(data)), message=None)
        else:
            logging.error('/dagruns/last7instancestatus code:1 Missing required parameters!', exc_info=True)
            return TaskSchedulerApi_pb2.TaskCommonResponse(code=2, data=str({'state': 'FAILED'}), message='Missing required parameters or type is error!')

    @provide_userinfo
    def updateTask(self, request, context, user_info=None):
        """
        :原接口：/task/update
        :return: 更新任务
        """
        schedule_job = request.scheduleJob
        ds_task_name = schedule_job.taskName
        ds_task_id = schedule_job.taskId
        task_code = schedule_job.taskCode
        tenant_id = user_info.tenantId if user_info.HasField('tenantId') else 1
        task_name = '{}_{}'.format(str(tenant_id), str(ds_task_id))
        # task_name = TaskDesc.get_task_with_tenant(tenant_id=tenant_id, ds_task_name=ds_task_name)

        try:
            TaskService.update_task(task_id=task_name, task_code=task_code,tenant_id=tenant_id,ds_task_name=ds_task_name,ds_task_id=ds_task_id)
        except Exception as e:
            logging.error('/task/update code:2 ' + str(e), exc_info=True)
            return TaskSchedulerApi_pb2.TaskCommonResponse(code=2, data=str({'state': 'FAILED'}), message=str(e))
        return TaskSchedulerApi_pb2.TaskCommonResponse(code=0, data=str({'state': 'SUCCESS'}), message=None)

    @provide_userinfo
    def updateAndExec(self, request, context, user_info=None):
        """
        :ds：
        :pipeline: /task/updateAndExe
        :return:
        """
        task_id = request.id  # 任务id
        extra_info = request.args
        callback_url = request.callbackUrl
        schedule_job = request.scheduleJob

        ds_task_name = schedule_job.taskName
        task_code = schedule_job.taskCode
        uuid = uuid_generator.generate_uuid()
        th = threading.Thread(target=fast_execute, args=(task_id, ds_task_name, task_code, callback_url, extra_info,uuid))
        th.start()
        return TaskSchedulerApi_pb2.TaskCommonResponse(code=0, data=str({'state': 'SUCCESS'}), message=None)

    @provide_userinfo
    def PageListInfo(self, request, context, user_info=None):
        """
        :原接口：/dagruns/latest
        :return: 运维页实例
        """
        page = request.page
        size = request.size
        ds_task_name = request.name
        state = request.state if request.state else None
        start_date = request.startDate if request.startDate else None
        end_date = request.endDate if request.endDate else None
        tenant_id = user_info.tenantId if user_info.HasField('tenantId') else 1
        tenant_task = TaskDesc.get_task_with_tenant(tenant_id=tenant_id, ds_task_name=ds_task_name, specified_field=1)
        if tenant_task:
            name = tenant_task.task_name
        else:
            logging.error('/dagruns/latest code:1  there is no task_name : tenant_id={}, ds_task_name={}'.format(str(tenant_id), str(ds_task_name)), exc_info=True)
            return TaskSchedulerApi_pb2.TaskCommonResponse(code=0, data='{}', message=None)

        try:
            if name and page and size:
                page = int(page)
                size = int(size)
                if state:
                    state = state.lower()
                    if state not in State.task_state:
                        return TaskSchedulerApi_pb2.TaskCommonResponse(code=2, data=str({'state': 'FAILED'}), message='Illegal parameters : state')
                if start_date:
                    start_date = pendulum.parse(start_date)
                if end_date:
                    end_date = pendulum.parse(end_date)
            else:
                return TaskSchedulerApi_pb2.TaskCommonResponse(code=0, data='{}', message=None)
        except Exception as e:
            logging.error('/dagruns/latest code:1  Illegal parameters :' + str(e), exc_info=True)
            return TaskSchedulerApi_pb2.TaskCommonResponse(code=2, data=str({'state': 'FAILED'}), message='Illegal parameters :' + str(e))
        try:
            data = TaskService.get_specified_task_instance_info(name=name, state=state, page=page, size=size,
                                                                start_date=start_date, end_date=end_date, ds_task_name=ds_task_name)
        except Exception as e:
            logging.error('/dagruns/latest code:2 ' + str(e), exc_info=True)
            return TaskSchedulerApi_pb2.TaskCommonResponse(code=2, data=str({'state': 'FAILED'}), message=str(e))
        return TaskSchedulerApi_pb2.TaskCommonResponse(code=0, data=str(json.dumps(data)), message=None)

    @provide_userinfo
    def fastBackfill(self, request, context, user_info=None):
        ds_task_id = request.taskId
        ds_task_name = request.taskName
        callback_url = request.callbackUrl
        extra_info = request.args
        start_date = request.startDate
        end_date = request.endDate
        tenant_id = user_info.tenantId if user_info.HasField('tenantId') else 1
        tenant_task = TaskDesc.get_task_with_tenant(tenant_id=tenant_id, ds_task_name=ds_task_name, contain_temp=False)
        try:
            if ds_task_name:
                if start_date and end_date:
                    start_date = pendulum.parse(start_date)
                    end_date = pendulum.parse(end_date)
                    if tenant_task:
                        if TaskDesc.check_is_crontab_task(task_name=tenant_task.task_name):
                            times = croniter_range(start_date, end_date, tenant_task.crontab)
                        else:
                            gra = tenant_task.granularity
                            times = Granularity.splitTimeByGranularity(start_time=start_date, end_time=end_date,
                                                                       gra=gra)
                    else:
                        return TaskSchedulerApi_pb2.TaskCommonResponse(code=1, message="task not exists")
                else:
                    times = [DateUtil.format_millisecond(timezone.utcnow().replace(tzinfo=beijing))]

                for execution_date in times :
                    ex_conf_dict = {}
                    if callback_url:
                        callback_dict = {'callback_url': callback_url, 'callback_id': ds_task_id, 'extra_info': extra_info}
                        ex_conf_dict['callback_info'] = callback_dict
                        external_conf = json.dumps(ex_conf_dict)
                        TaskService.task_execute(task_name=tenant_task.task_name, execution_date=execution_date,
                                                 external_conf=external_conf)
                        Alert.add_alert_job(task_name=tenant_task.task_name, execution_date=execution_date)
                    else:
                        TaskService.task_execute(task_name=tenant_task.task_name, execution_date=execution_date)
        except Exception as e:
            logging.error("/task/fastBackfill code:2 " + str(e), exc_info=True)
            return TaskSchedulerApi_pb2.TaskCommonResponse(code=1, message=str(e))
        return TaskSchedulerApi_pb2.TaskCommonResponse(code=0,message='success')


    @provide_userinfo
    def batchClear(self, request, context, user_info=None):
        tenant_id = user_info.tenantId if user_info.HasField('tenantId') else 1
        batches = request.batches
        name_dict = defaultdict(str)
        for b in batches:
            try:
                ds_task_name = b.name
                execution_date = b.executionDate
                ignore_check = not b.isCheckUpstream
                if ds_task_name and execution_date:
                    if not name_dict[ds_task_name]:
                        tenant_task = TaskDesc.get_task_with_tenant(tenant_id=tenant_id, ds_task_name=ds_task_name)
                        if not tenant_task:
                            continue
                        name_dict[ds_task_name] = tenant_task.task_name
                    parse_dates = []
                    execution_dates = execution_date.split(',')
                    for dt in execution_dates:
                        dt = pendulum.parse(dt)
                        parse_dates.append(dt)

                    TaskService.clear_instance(dag_id=name_dict[ds_task_name], execution_dates=parse_dates,ignore_check=ignore_check)
            except Exception as e:
                logging.error('/batchClear code:2 ' + str(e), exc_info=True)
                return TaskSchedulerApi_pb2.TaskCommonResponse(code=2, data=str({'state': 'FAILED'}), message=str(e))
        return TaskSchedulerApi_pb2.TaskCommonResponse(code=0, data=str({'state': 'SUCCESS'}), message='')

    @provide_userinfo
    def batchSetSuccess(self, request, context, user_info=None):
        tenant_id = user_info.tenantId if user_info.HasField('tenantId') else 1
        batches = request.batches
        name_dict = defaultdict(str)
        for b in batches:
            try:
                ds_task_name = b.name
                execution_date = b.executionDate
                if ds_task_name and execution_date:
                    if not name_dict[ds_task_name]:
                        tenant_task = TaskDesc.get_task_with_tenant(tenant_id=tenant_id, ds_task_name=ds_task_name)
                        if not tenant_task:
                            continue
                        name_dict[ds_task_name] = tenant_task.task_name
                    execution_dates = execution_date.split(',')
                    for dt in execution_dates:
                        dt = pendulum.parse(dt)
                        TaskService.set_success(dag_id=name_dict[ds_task_name], execution_date=dt)
            except Exception as e:
                logging.error('/batchSetSuccess code:2 ' + str(e), exc_info=True)
                return TaskSchedulerApi_pb2.TaskCommonResponse(code=2, data=str({'state': 'FAILED'}), message=str(e))
        return TaskSchedulerApi_pb2.TaskCommonResponse(code=0, data=str({'state': 'SUCCESS'}), message='')


    @provide_userinfo
    def batchSetFailed(self, request, context, user_info=None):
        tenant_id = user_info.tenantId if user_info.HasField('tenantId') else 1
        batches = request.batches
        name_dict = defaultdict(str)
        for b in batches:
            try:
                ds_task_name = b.name
                execution_date = b.executionDate
                if ds_task_name and execution_date:
                    if not name_dict[ds_task_name]:
                        tenant_task = TaskDesc.get_task_with_tenant(tenant_id=tenant_id, ds_task_name=ds_task_name)
                        if not tenant_task:
                            continue
                        name_dict[ds_task_name] = tenant_task.task_name
                    execution_dates = execution_date.split(',')
                    for dt in execution_dates:
                        dt = pendulum.parse(dt)
                        TaskService.set_failed(dag_id=name_dict[ds_task_name], execution_date=dt)
            except Exception as e:
                logging.error('/batchSetFailed code:2 ' + str(e), exc_info=True)
                return TaskSchedulerApi_pb2.TaskCommonResponse(code=2, data=str({'state': 'FAILED'}), message=str(e))
        return TaskSchedulerApi_pb2.TaskCommonResponse(code=0, data=str({'state': 'SUCCESS'}), message='')

