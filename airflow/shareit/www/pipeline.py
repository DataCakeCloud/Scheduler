# -*- coding: utf-8 -*-
"""
Author: zhangtao
CreateDate: 2021/4/22
"""
import json
import logging
import time

import pendulum
import six
import threading

from croniter import croniter
from flask import Blueprint, jsonify, request

from airflow.models import TaskInstance
from airflow.shareit.constant.kyuubi_state import KyuubiState
from airflow.shareit.constant.return_code import ReturnCodeConstant
from airflow.shareit.hooks.kyuubi_hook import KyuubiHook
from airflow.shareit.models.alert import Alert
from airflow.shareit.models.event_trigger_mapping import EventTriggerMapping
from airflow.shareit.models.task_desc import TaskDesc
from airflow.shareit.models.fast_execute_task import FastExecuteTask
from airflow.shareit.service.diagnosis_service import DiagnosisService
from airflow.shareit.service.task_service import TaskService
from airflow.shareit.service.dataset_service import DatasetService
from airflow.shareit.service.config_service import ConfigService
from airflow.shareit.service.taskinstance_service import TaskinstanceService
from airflow.shareit.utils.airflow_date_param_transform import AirlowDateParamTransform
from airflow.shareit.utils.alarm_adapter import send_ding_notify
from airflow.shareit.utils.date_util import DateUtil
from airflow.shareit.utils.state import State
from airflow.shareit.utils.uid.uuid_generator import UUIDGenerator
from airflow.utils import timezone
from airflow.utils.timezone import beijing, utcnow
from datetime import datetime
from airflow.www.app import csrf

pipeline_api = Blueprint("pipeline_api", __name__)


def procees_bool(param, default=False):
    if isinstance(param, six.string_types):
        param = True if param == "True" or param == "TRUE" or param == "true" else False
    elif isinstance(param, bool):
        pass
    elif param is None:
        param = default
    else:
        return -1
    return param


def request_process(re, content):
    if re.method == "GET":
        comment = re.args.get(content)
        # comment = request.values.get(content)
    elif re.method == "POST":
        if re.content_type.startswith('application/json'):
            # comment = request.get_json()[content]
            comment = re.json.get(content)
        elif re.content_type.startswith('multipart/form-data'):
            comment = re.form.get(content)
        else:
            comment = re.values.get(content)
    return comment

def request_get_user_info(re):
    user_info = re.headers.get('current_login_user')

    return user_info

def request_get_tenantId(re):
    user_info = request_get_user_info(re)
    user_info_dict = json.loads(user_info)
    return user_info_dict['tenantId']

@csrf.exempt
@pipeline_api.route('/task/delete', methods=["GET", "POST"])
def task_delete():
    task_id = request_process(re=request, content="name")
    try:
        if task_id:
            TaskService.delete_task(task_id=task_id)
        else:
            return jsonify(code=1, data={'state': 'FAILED'}, msg="can not get correct param: name ")
    except ValueError as ve:
        logging.error("/task/delete code:3 " + str(ve), exc_info=True)
        return jsonify(code=3, data={'state': 'FAILED'}, msg=str(ve))
    except Exception as e:
        logging.error("/task/delete code:2 " + str(e), exc_info=True)
        return jsonify(code=2, data={'state': 'FAILED'}, msg=str(e))
    return jsonify(code=0, data={'state': 'SUCCESS'}, msg=None)


@csrf.exempt
@pipeline_api.route('/task/update', methods=["GET", "POST"])
def task_update():
    task_id = request_process(re=request, content="name")
    task_code = request_process(re=request, content="task_code")
    try:
        TaskService.update_task(task_id=task_id, task_code=task_code)
    except Exception as e:
        logging.error("/task/update code:2 " + str(e), exc_info=True)
        return jsonify(code=2, data={'state': 'FAILED'}, msg=str(e))
    return jsonify(code=0, data={'state': 'SUCCESS'}, msg=None)


@csrf.exempt
@pipeline_api.route('/task/backfill', methods=["GET", "POST"])
def backfill():
    task_ids = request_process(re=request, content="names")
    start_date = request_process(re=request, content="start_date")
    end_date = request_process(re=request, content="end_date")
    is_send_notify = request_process(re=request, content="is_send_notify")
    is_check_dependency = request_process(re=request, content="is_check_dependency")
    core_task_name = request_process(re=request, content="core_task_name")
    operator = request_process(re=request, content="operator")

    is_send_notify = procees_bool(is_send_notify,default=False)
    is_check_dependency = procees_bool(is_check_dependency,default=True)

    if is_send_notify == -1:
        return jsonify(code=1, data={'state': 'WARNING'},
                       msg="Wrong parameter type: is_send_notify should be string or bool")
    if is_check_dependency == -1:
        return jsonify(code=1, data={'state': 'WARNING'},
                       msg="Wrong parameter type: is_check_dependency should be string or bool")

    try:
        task_ids = task_ids.split(',')
        start_date = pendulum.parse(start_date)
        end_date = pendulum.parse(end_date)
        # TaskService.backfill(task_ids, start_date, end_date, is_send_notify, is_check_dependency)
        TaskService.backfill(task_ids=task_ids,
                                           core_task_name=core_task_name,
                                           operator=operator,
                                           start_date=start_date, end_date=end_date,
                                           is_send_notify=is_send_notify, is_check_dependency=is_check_dependency)
    except ValueError as ve:
        logging.error("/task/backfill code:1 " + str(ve), exc_info=True)
        return jsonify(code=1, data={'state': 'WARNING'}, msg=str(ve))
    except Exception as e:
        logging.error("/task/backfill code:2 " + str(e), exc_info=True)
        return jsonify(code=2, data={'state': 'FAILED'}, msg=str(e))
    return jsonify(code=0, data={'state': 'SUCCESS'}, msg=None)


@csrf.exempt
@pipeline_api.route('/paused', methods=["GET", "POST"])
def paused():
    name = request_process(re=request, content="name")
    is_online = request_process(re=request, content="is_online")
    is_paused = False if is_online == "True" or is_online == "true" else True
    try:
        TaskService.set_paused(task_name=name, is_paused=is_paused)
    except ValueError as ve:
        return jsonify(code=1, data={'state': 'WARNING'},
                       msg=str(ve))
    except Exception as e:
        logging.error("/paused code:2 " + str(e), exc_info=True)
        return jsonify(code=2, data={'state': 'FAILED'}, msg=str(e))
    return jsonify(code=0, data={'state': 'SUCCESS'}, msg=None)
    # task should be exist
    # is_online is bool


@csrf.exempt
@pipeline_api.route('/success', methods=["GET", "POST"])
def set_success():
    name = request_process(re=request, content="name")
    execution_date = request_process(re=request, content="execution_date")
    try:
        if name and execution_date:
            execution_dates = execution_date.split(',')
            for dt in execution_dates:
                dt = pendulum.parse(dt)
                TaskService.set_success(dag_id=name, execution_date=dt)
        else:
            return jsonify(code=2, data={'state': 'FAILED'},
                           msg="Missing required parameters！")
    except Exception as e:
        logging.error("/success code:2 " + str(e), exc_info=True)
        return jsonify(code=2, data={'state': 'FAILED'}, msg=str(e))
    return jsonify(code=0, data={'state': 'SUCCESS'}, msg=None)


@csrf.exempt
@pipeline_api.route('/failed', methods=["GET", "POST"])
def set_failed():
    name = request_process(re=request, content="name")
    execution_date = request_process(re=request, content="execution_date")
    try:
        if name and execution_date:
            execution_dates = execution_date.split(',')
            for dt in execution_dates:
                dt = pendulum.parse(dt)
                TaskService.set_failed(dag_id=name, execution_date=dt)
        else:
            return jsonify(code=2, data={'state': 'FAILED'},
                           msg="Missing required parameters！")
    except Exception as e:
        logging.error("/failed code:2 " + str(e), exc_info=True)
        return jsonify(code=2, data={'state': 'FAILED'}, msg=str(e))
    return jsonify(code=0, data={'state': 'SUCCESS'}, msg=None)


@csrf.exempt
@pipeline_api.route('/clear', methods=["GET", "POST"])
def clear_instance():
    name = request_process(re=request, content="name")
    execution_date = request_process(re=request, content="execution_date")
    try:
        if name and execution_date:
            parse_dates=[]
            execution_dates = execution_date.split(',')
            for dt in execution_dates:
                dt = pendulum.parse(dt)
                parse_dates.append(dt)

            count = TaskService.clear_instance(dag_id=name, execution_dates=parse_dates)
        else:
            return jsonify(code=2, data={'state': 'FAILED'},
                           msg="Missing required parameters！")
    except Exception as e:
        logging.error("/clear code:2 " + str(e), exc_info=True)
        return jsonify(code=2, data={'state': 'FAILED'}, msg=str(e))
    return jsonify(code=0, data={'state': 'SUCCESS'}, msg="{0} task instances have been cleared".format(count))


@csrf.exempt
@pipeline_api.route('/dagruns/latest', methods=["GET", "POST"])
def dagrun_latest():
    from airflow.shareit.utils.state import State
    ds_task_name = request_process(re=request, content="name")
    page = request_process(re=request, content="page")
    size = request_process(re=request, content="size")
    state = request_process(re=request, content="state")
    start_date = request_process(re=request, content="start_date")
    end_date = request_process(re=request, content="end_date")
    instance_id = request_process(re=request, content="instanceId")
    tenant_id = request_get_tenantId(re=request)
    tenant_task = TaskDesc.get_task_with_tenant(tenant_id=tenant_id, ds_task_name=ds_task_name, specified_field=1)
    if tenant_task:
        name = tenant_task.task_name
    else:
        logging.error(
            '/dagruns/latest code:1  there is no task_name : tenant_id={}, ds_task_name={}'.format(str(tenant_id),
                                                                                                   str(ds_task_name)),
            exc_info=True)
        return jsonify(code=0, data={'count':0,'task_info':[]},msg="")

    try:
        if name and page and size:
            page = int(page)
            size = int(size)
            if state:
                state = state.lower()
                if state not in State.task_state:
                    return jsonify(code=2, data={'state': 'FAILED'},
                                   msg="Illegal parameters : state")
            if start_date:
                start_date = pendulum.parse(start_date)
            if end_date:
                end_date = pendulum.parse(end_date)
        else:
            return jsonify(code=2, data={'state': 'FAILED'},
                           msg="Missing required parameters！")
    except Exception as e:
        logging.error("/dagruns/latest code:1  Illegal parameters :" + str(e), exc_info=True)
        return jsonify(code=2, data={'state': 'FAILED'}, msg="Illegal parameters :" + str(e))
    try:
        data = TaskService.get_specified_task_instance_info(name=name, state=state, page=page, size=size,
                                                            start_date=start_date, end_date=end_date,
                                                            ds_task_name=ds_task_name,instance_id=instance_id)
    except Exception as e:
        logging.error("/dagruns/latest code:2 " + str(e), exc_info=True)
        return jsonify(code=2, data={'state': 'FAILED'}, msg=str(e))
    return jsonify(code=0, data=data, msg=None)


@csrf.exempt
@pipeline_api.route('/dagruns/laststatus', methods=["GET", "POST"])
def laststatus():
    names = request_process(re=request, content="names")
    if isinstance(names, str) or isinstance(names, unicode):
        try:
            name_list = names.split(",")
            data = TaskService.get_last_instance_info(nameList=name_list)
        except Exception as e:
            return jsonify(code=2, data={'state': 'FAILED'}, msg=str(e))
        return jsonify(code=0, data=data, msg=None)
    else:
        logging.error("/dagruns/laststatus code:1 Missing required parameters！", exc_info=True)
        return jsonify(code=2, data={'state': 'FAILED'}, msg="Missing required parameters or type is error！")


@csrf.exempt
@pipeline_api.route('/dagruns/delete', methods=["GET", "POST"])
def dag_run_delete():
    name = request_process(re=request, content="name")
    execution_date = request_process(re=request, content="execution_date")
    if name and execution_date:
        try:
            execution_date = pendulum.parse(execution_date)
            TaskService.dagrun_delete(dag_id=name, execution_date=execution_date)
        except Exception as e:
            logging.error("/dagruns/delete code:2 " + str(e), exc_info=True)
            return jsonify(code=2, data={'state': 'FAILED'}, msg=str(e))
        return jsonify(code=0, data={'state': 'SUCCESS'}, msg=None)
    else:
        logging.error("/dagruns/delete code:1 Missing required parameters！", exc_info=True)
        return jsonify(code=2, data={'state': 'FAILED'}, msg="Missing required parameters！")


@csrf.exempt
@pipeline_api.route('/status', methods=["GET", "POST"])
def dag_run_status():
    name = request_process(re=request, content="name")
    execution_date = request_process(re=request, content="execution_date")
    if name and execution_date:
        try:
            execution_date = pendulum.parse(execution_date)
            state = TaskService.get_dag_run_status(dag_id=name, execution_date=execution_date)
        except Exception as e:
            logging.error("/status code:2 " + str(e), exc_info=True)
            return jsonify(code=2, data={'state': 'FAILED'}, msg=str(e))
        return jsonify(code=0, data={'state': state}, msg=None)
    else:
        logging.error("/status code:1 Missing required parameters！", exc_info=True)
        return jsonify(code=2, data={'state': 'FAILED'}, msg="Missing required parameters！")


@csrf.exempt
@pipeline_api.route('/logurl', methods=["GET", "POST"])
def get_log_url():
    name = request_process(re=request, content="name")
    execution_date = request_process(re=request, content="execution_date")
    if name and execution_date:
        try:
            execution_date = pendulum.parse(execution_date)
            log_url = TaskService.get_dag_run_log(dag_id=name, execution_date=execution_date)
        except Exception as e:
            logging.error("/logurl code:2 " + str(e), exc_info=True)
            return jsonify(code=2, data={'state': 'FAILED'}, msg=str(e))
        return jsonify(code=0, data={'logurl': log_url}, msg=None)
    else:
        logging.error("/logurl code:1 Missing required parameters！", exc_info=True)
        return jsonify(code=2, data={'state': 'FAILED'}, msg="Missing required parameters！")


@csrf.exempt
@pipeline_api.route('/dagruns/trigger', methods=["GET", "POST"])
def trigger():
    name = request_process(re=request, content="name")
    execution_date = request_process(re=request, content="execution_date")
    if name and execution_date:
        try:
            execution_date = pendulum.parse(execution_date)
            TaskService.trigger_manual(dag_id=name, execution_date=execution_date)
            # log_url = TaskService.get_dag_run_log(dag_id=name, execution_date=execution_date)
        except Exception as e:
            logging.error("/dagruns/trigger code:2 " + str(e), exc_info=True)
            return jsonify(code=2, data={'state': 'FAILED'}, msg=str(e))
        return jsonify(code=0, data={'state': "SUCCESS"}, msg=None)
    else:
        logging.error("/dagruns/trigger code:1 Missing required parameters！", exc_info=True)
        return jsonify(code=2, data={'state': 'FAILED'}, msg="Missing required parameters！")


@csrf.exempt
@pipeline_api.route('/dataset/sync_state', methods=["GET", "POST"])
def sync_state():
    metadata_id = request_process(re=request, content="metadata_id")
    execution_date = request_process(re=request, content="execution_date")
    state = request_process(re=request, content="state")
    run_id = request_process(re=request, content="run_id")
    if metadata_id and execution_date and state and run_id:
        try:
            execution_date = pendulum.parse(execution_date)
            TaskService.generate_dataset_sign_manual(metadata_id, execution_date, state, run_id)
        except Exception as e:
            logging.error("/dataset/sync_state code:2 " + str(e), exc_info=True)
            return jsonify(code=2, data={'state': 'FAILED'}, msg=str(e))
        return jsonify(code=0, data={'state': "SUCCESS"}, msg=None)
    else:
        logging.error("/dataset/sync_state code:1 Missing required parameters！", exc_info=True)
        return jsonify(code=2, data={'state': 'FAILED'}, msg="Missing required parameters！")


@csrf.exempt
@pipeline_api.route('/test', methods=["GET", "POST", "PUT"])
def test():
    # from airflow.shareit.scheduler.scheduler_helper import SchedulerManager
    # SchedulerManager.main_scheduler_process()
    content = request_process(re=request, content="content")
    print(content)
    # from airflow.shareit.utils.task_util import TaskUtil
    # ta = TaskUtil()
    # ta.external_data_processing()
    return jsonify(code=0, data={'state': 'SUCCESS'}, msg="hello world!!")


@csrf.exempt
@pipeline_api.route('/render', methods=["GET", "POST"])
def my_render():
    import six
    import json
    from airflow import macros
    content = request_process(re=request, content="content")
    execution_date = request_process(re=request, content="execution_date")
    extra_parameters = request_process(re=request, content="extra_parameters")
    task_name = request_process(re=request, content="task_name")
    context={}
    if execution_date:
        try:
            execution_date = pendulum.parse(execution_date)
            if extra_parameters:
                context = json.loads(extra_parameters) if isinstance(extra_parameters, six.string_types) else {}
            context["execution_date"] = execution_date
            context["macros"] = macros
            res = TaskService.render_api(content=content, context=context,task_name=task_name)
        except Exception as e:
            logging.error("/render code:2 " + str(e), exc_info=True)
            return jsonify(code=2, data={'state': 'FAILED'}, msg=str(e))
        return jsonify(code=0, data=res, msg=None)
    else:
        logging.error("/render code:1 Missing required parameters！", exc_info=True)
        return jsonify(code=2, data={'state': 'FAILED'}, msg="Missing required parameters！")


@csrf.exempt
@pipeline_api.route('/task/diagnose', methods=["GET", "POST"])
def task_diagnose():
    task_id = request_process(re=request, content="name")
    execution_date = request_process(re=request, content="execution_date")
    state = request_process(re=request, content="state")
    try:
        execution_date = pendulum.parse(execution_date)
        execution_date = execution_date.replace(tzinfo=beijing)
        resp_data = DiagnosisService().diagnose_new(task_id, execution_date, state)
    except Exception as e:
        logging.error("/task/diagnose code:2 " + str(e), exc_info=True)
        return jsonify(code=2, data={'state': 'FAILED'}, msg=str(e))
    return jsonify(code=0, data=resp_data, msg=None)

# todo
@csrf.exempt
@pipeline_api.route('/task/dagid/count', methods=['GET', 'POST'])
def dagid_count():
    ds_task_name = request_process(re=request, content='name')
    tenant_id = request_get_tenantId(re=request)
    td = TaskDesc.get_task_with_tenant(ds_task_name=ds_task_name, tenant_id=tenant_id)
    if td:
        task_name = td.task_name
    else:
        logging.error("task not exists,ds_task_name :{},tenant_id:{} ".format(ds_task_name, str(tenant_id)),
                      exc_info=True)
        return jsonify(code=0, data={}, msg=None)
    try:
        resp_data = DiagnosisService.dagid_count(task_name)
    except Exception as e:
        logging.error("/task/dagid/count code:2 " + str(e), exc_info=True)
        return jsonify(code=500, data={'state': 'FAILED'}, msg=str(e))
    return jsonify(code=0, data=resp_data, msg=None)

# todo
@csrf.exempt
@pipeline_api.route('/task/instance/dates', methods=['GET', 'POST'])
def task_instance_dates():
    ds_task_name = request_process(re=request, content='name')
    execution_date = request_process(re=request, content='execution_date')
    tenant_id = request_get_tenantId(re=request)
    td = TaskDesc.get_task_with_tenant(ds_task_name=ds_task_name, tenant_id=tenant_id)
    if td:
        task_name = td.task_name
    else:
        logging.error("task not exists,ds_task_name :{},tenant_id:{} ".format(ds_task_name, str(tenant_id)),
                      exc_info=True)
        return jsonify(code=0, data={}, msg=None)
    try:
        if not execution_date:
            import pytz
            from datetime import datetime
            execution_date = datetime.fromtimestamp(int(time.time()), beijing)
        else:
            # execution_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            execution_date = pendulum.parse(execution_date).replace(tzinfo=beijing)
        resp_data = DiagnosisService.task_dates(task_name, execution_date)
    except Exception as e:
        logging.error("/task/instance/dates code:2 " + str(e), exc_info=True)
        return jsonify(code=500, data={'state': 'FAILED'}, msg=str(e))
    return jsonify(code=0, data=resp_data, msg=None)

# todo
@csrf.exempt
@pipeline_api.route('/task/data/lineage', methods=['GET', 'POST'])
def data_lineage():
    ds_task_name = request_process(re=request, content="name")
    depth = int(request_process(re=request, content='depth'))
    stream = int(request_process(re=request, content='upDown'))
    execution_date = request_process(re=request, content="execution_date")
    tenant_id = request_get_tenantId(re=request)
    td = TaskDesc.get_task_with_tenant(ds_task_name=ds_task_name,tenant_id=tenant_id)
    if td:
        task_name = td.task_name
    else:
        logging.error("/task/data/lineage task not exists,ds_task_name :{},tenant_id:{} ".format(ds_task_name,str(tenant_id)), exc_info=True)
        return jsonify(code=0, data={}, msg=None)
    try:
        execution_date = pendulum.parse(execution_date).replace(tzinfo=beijing)
        resp_data = DiagnosisService().data_lineage(task_name, execution_date, depth, stream,tenant_id=tenant_id,ds_task_id=td.ds_task_id)
    except Exception as e:
        logging.error("/task/data/lineage code:2 " + str(e), exc_info=True)
        return jsonify(code=500, data={'state': 'FAILED'}, msg=str(e))
    return jsonify(code=0, data=resp_data, msg=None)

# todo
@csrf.exempt
@pipeline_api.route('/task/data/link/analysis', methods=['GET', 'POST'])
def data_link_analysis():
    ds_task_name = request_process(re=request, content="name")
    tenant_id = request_get_tenantId(re=request)
    td = TaskDesc.get_task_with_tenant(ds_task_name=ds_task_name, tenant_id=tenant_id)
    if td:
        task_name = td.task_name
    else:
        logging.error("task not exists,ds_task_name :{},tenant_id:{} ".format(ds_task_name, str(tenant_id)),
                      exc_info=True)
        return jsonify(code=0, data={}, msg=None)
    gantt = int(request_process(re=request, content="gantt"))
    if gantt == 1:  # 甘特图
        num = 1
    else:  # 柱状图
        num = int(request_process(re=request, content="num"))
    if num > 1:
        gantt = 0

    execution_date = request_process(re=request, content="execution_date")
    try:
        execution_date = pendulum.parse(execution_date).replace(tzinfo=beijing)
        resp_data = DiagnosisService().data_link_analysis(task_name, execution_date, num, gantt=gantt,tenant_id=tenant_id,ds_task_name=ds_task_name)
    except Exception as e:
        logging.error("/task/data/link/analysis code:2 " + str(e), exc_info=True)
        return jsonify(code=500, data={'state': 'FAILED'}, msg=str(e))
    return jsonify(code=0, data=resp_data, msg=None)


@csrf.exempt
@pipeline_api.route('/task/updatename', methods=["GET", "POST"])
def update_name():
    old_name = request_process(re=request, content="oldName")
    new_name = request_process(re=request, content="newName")
    try:
        TaskService.update_task_name(old_name, new_name)
    except Exception as e:
        logging.error("/task/diagnose code:2 " + str(e), exc_info=True)
        return jsonify(code=2, data={'state': 'FAILED'}, msg=str(e))
    return jsonify(code=0, data={'state': 'SUCCESS'}, msg=None)


@csrf.exempt
@pipeline_api.route('/dagruns/last7instancestatus', methods=["GET", "POST"])
def get_last_7_instance_status():
    names = request_process(re=request, content="names")
    if isinstance(names, str) or isinstance(names, unicode):
        try:
            name_list = names.split(",")
            # data = TaskService.get_result_last_7_instance(nameList=name_list)
            data = TaskService.get_last7_instances(name_list)  # 多进程改为多线程，适配gevent
        except Exception as e:
            return jsonify(code=2, data={'state': 'FAILED'}, msg=str(e))
        return jsonify(code=0, data=data, msg=None)
    else:
        logging.error("/dagruns/last7instancestatus code:1 Missing required parameters！", exc_info=True)
        return jsonify(code=2, data={'state': 'FAILED'}, msg="Missing required parameters or type is error！")


@csrf.exempt
@pipeline_api.route('/dataset/info', methods=["GET", "POST"])
def get_dataset_info():
    name = request_process(re=request, content="name")
    if isinstance(name, str) or isinstance(name, unicode):
        try:
            data = DatasetService.get_dataset_info(name=name.strip())
        except Exception as e:
            return jsonify(code=2, data={'state': 'FAILED'}, msg=str(e))
        return jsonify(code=0, data=data, msg=None)
    else:
        logging.error("/dataset/info code:1 Missing required parameters！", exc_info=True)
        return jsonify(code=2, data={'state': 'FAILED'}, msg="Missing required parameters or type is error！")


@csrf.exempt
@pipeline_api.route('/connections', methods=["GET", "POST"])
def get_connections():
    conn_ids = request_process(re=request, content="conn_ids")
    conn_type = request_process(re=request, content="conn_type")
    try:
        data = ConfigService.get_conn(conn_ids, conn_type)
    except Exception as e:
        return jsonify(code=2, data={'state': 'FAILED'}, msg=str(e))
    return jsonify(code=0, data=data, msg=None)


@csrf.exempt
@pipeline_api.route('/connection/update', methods=["GET", "POST"])
def create_update_conn():
    conn_id = request_process(re=request, content="conn_id")
    conn_type = request_process(re=request, content="conn_type")
    host = request_process(re=request, content="host")
    schema = request_process(re=request, content="schema")
    login = request_process(re=request, content="login")
    password = request_process(re=request, content="login")
    port = request_process(re=request, content="port")
    extra = request_process(re=request, content="extra")
    if conn_type and conn_id:
        try:
            ConfigService.create_or_update_conn(conn_id=conn_id, conn_type=conn_type, host=host,
                                                schema=schema, login=login, password=password,
                                                port=port, extra=extra)
        except Exception as e:
            return jsonify(code=2, data={'state': 'FAILED'}, msg=str(e))
        return jsonify(code=0, data={'state': 'SUCCESS'}, msg=None)
    else:
        logging.error("/connection/update code:1 Missing required parameters！", exc_info=True)
        return jsonify(code=2, data={'state': 'FAILED'}, msg="Missing required parameters or type is error！")


@csrf.exempt
@pipeline_api.route('/connection/delete', methods=["GET", "POST"])
def delete_conn():
    conn_ids = request_process(re=request, content="conn_ids")
    if conn_ids:
        try:
            ConfigService.delete_conn(conn_ids)
        except Exception as e:
            return jsonify(code=2, data={'state': 'FAILED'}, msg=str(e))
        return jsonify(code=0, data={'state': 'SUCCESS'}, msg=None)
    else:
        logging.error("/connection/delete code:1 Missing required parameters！", exc_info=True)
        return jsonify(code=2, data={'state': 'FAILED'}, msg="Missing required parameters or type is error！")


@csrf.exempt
@pipeline_api.route('/task/instance/relation', methods=["GET", "POST"])
def instance_relation():
    core_task = request_process(re=request, content="core_task")
    execution_date = request_process(re=request, content="execution_date")
    level = request_process(re=request, content="level")
    is_downstream = request_process(re=request, content="is_downstream")
    if core_task and execution_date:
        try:
            execution_date = pendulum.parse(execution_date)
            is_downstream = procees_bool(param=is_downstream, default=True)
            level = int(level)
            res = TaskService.instance_relation_shell(core_task=core_task,
                                                      execution_date=execution_date,
                                                      level=level-1,
                                                      is_downstream=is_downstream)
        except Exception as e:
            return jsonify(code=2, data={'state': 'FAILED'}, msg=str(e))
        return jsonify(code=0, data=res,msg=None)
    else:
        logging.error("/task/instance/relation code:1 Missing required parameters！", exc_info=True)
        return jsonify(code=2, data={'state': 'FAILED'}, msg="Missing required parameters or type is error！")


@csrf.exempt
@pipeline_api.route('/backfill/progress/reminder', methods=["GET", "POST"])
def backfill_progress_reminder():
    user_action_id = request_process(re=request, content="user_action_id")
    try:
        TaskService.send_backfill_progress_reminder(user_action_id=int(user_action_id))
    except Exception as e:
        logging.error("/backfill/progress/reminder code:2 " + str(e), exc_info=True)
        return jsonify(code=2, data={'state': 'FAILED'}, msg=str(e))
    return jsonify(code=0, data={'state': 'SUCCESS'}, msg=None)

@csrf.exempt
@pipeline_api.route('/task/date/transform', methods=["GET", "POST"])
def date_transform():
    '''
    对airflow的日期做转换
    '''
    is_data_trigger = request_process(re=request, content="is_data_trigger")
    is_sql = request_process(re=request, content="is_sql")
    airflow_crontab = request_process(re=request, content="airflow_crontab")
    task_gra = request_process(re=request, content="task_gra")
    airflow_content = request_process(re=request, content="airflow_content")
    is_data_trigger = procees_bool(is_data_trigger, default=False)
    is_sql = procees_bool(is_sql, default=False)
    try:
        data = AirlowDateParamTransform.date_transform(is_data_trigger=is_data_trigger,airflow_crontab=airflow_crontab,task_gra=task_gra,airflow_content=airflow_content,is_sql=is_sql)
    except Exception as e:
        logging.error("/task/date/transform code:2 " + str(e), exc_info=True)
        return jsonify(code=2, data={'state': 'FAILED'}, msg=str(e))
    return jsonify(code=0, data=data, msg=None)


@csrf.exempt
@pipeline_api.route('/task/force/run',methods=["GET", "POST"])
def run():
    from airflow.models.taskinstance import TaskInstance
    from airflow.shareit.utils.task_manager import get_dag
    from airflow.executors import get_default_executor
    from airflow.utils.state import State
    dag_id = request_process(re=request, content="dag_id")
    task_id = request_process(re=request, content="task_id")
    execution_date = request_process(re=request, content="execution_date")
    ignore_all_deps = request_process(re=request, content="ignore_all_deps")
    ignore_task_deps = request_process(re=request, content="ignore_task_deps")
    ignore_ti_state = request_process(re=request, content="ignore_ti_state")

    ignore_all_deps = procees_bool(ignore_all_deps,True)
    ignore_task_deps = procees_bool(ignore_task_deps,True)
    ignore_ti_state = procees_bool(ignore_ti_state,True)
    try:
        execution_date = pendulum.parse(execution_date)

        # 只有celery和kubernetes两种是可以的，现在使用的是kubernetes
        executor = get_default_executor()
        dag = get_dag(dag_id)
        task = dag.get_task(task_id)
        ti = TaskInstance(task=task, execution_date=execution_date)
        if ti.state not in [State.RUNNING,State.SUCCESS,State.QUEUED,State.SCHEDULED]:
            ti.refresh_from_db()
            executor.start()
            executor.queue_task_instance(
                ti,
                ignore_all_deps=ignore_all_deps,
                ignore_task_deps=ignore_task_deps,
                ignore_ti_state=ignore_ti_state)
            executor.heartbeat()
    except Exception as e:
        logging.error("/task/force/run code:2 " + str(e), exc_info=True)
        return jsonify(code=2, data={'state': 'FAILED'}, msg=str(e))
    return jsonify(code=0, data="success", msg=None)


@csrf.exempt
@pipeline_api.route('/task/date/preview',methods=["GET", "POST"])
def date_preview():
    task_gra = request_process(re=request, content="task_gra")
    task_crontab = request_process(re=request, content="task_crontab")
    task_depend = request_process(re=request, content="task_depend")
    data_depend = request_process(re=request, content="data_depend")
    try:
        res = TaskService.get_date_preview(task_gra=task_gra,task_crontab=task_crontab,task_depend=task_depend,data_depend=data_depend)
    except Exception as e:
        logging.error("/task/force/run code:2 " + str(e), exc_info=True)
        return jsonify(code=2, data={'state': 'FAILED'}, msg=str(e))
    return jsonify(code=0, data=res, msg=None)

@csrf.exempt
@pipeline_api.route('/taskinstance/record',methods=["GET", "POST"])
def get_taskinstance_record():
    ds_task_name = request_process(re=request, content="taskName")
    execution_date = request_process(re=request, content="executionDate")
    tenant_id = request_get_tenantId(re=request)
    td = TaskDesc.get_task_with_tenant(ds_task_name=ds_task_name, tenant_id=tenant_id)
    if td:
        task_name = td.task_name
    else:
        logging.error("/taskinstance/record task not exists,{} ".format(ds_task_name), exc_info=True)
        return jsonify(code=0, codeStr='SUCCESS', data={}, msg=None)

    try:
        execution_date = pendulum.parse(execution_date)
        res = TaskinstanceService.get_taskinstance_record(task_name=task_name,execution_date=execution_date,ds_task_name=ds_task_name)
    except Exception as e:
        logging.error("/task/force/run code:2 " + str(e), exc_info=True)
        return jsonify(code=500, codeStr='SYS_ERR',data={'state': 'FAILED'}, msg=str(e))
    return jsonify(code=0,codeStr='SUCCESS', data=res, msg=None)

@csrf.exempt
@pipeline_api.route('/test/bingfa',methods=["GET", "POST"])
def test_bingfa():
    time.sleep(30)
    return jsonify(code=0, codeStr='SUCCESS', data={}, msg=None)


def fast_execute(id, task_name, task_code, callback_url, extra_info):
    extra_info_dict = json.loads(extra_info.lstrip("'").rstrip("'"))
    if 'more_graphic_id' in extra_info_dict.keys():
        uniq_id = extra_info_dict['more_graphic_id']
    else:
        uniq_id = DateUtil.get_timestamp(DateUtil.format_millisecond(timezone.utcnow().replace(tzinfo=beijing)))

    if task_name:
        try:
            '''
                临时执行任务的原理是，生成一个新的任务并且注册到task_desc中，将新任务的名字保存在taskinstance的extenal_conf中
                在taskinstance开始执行时，获取真实要执行的任务名，并且get对应的task内容
                为什么这样做？因为临时任务可能是同一时间调起多次的，他们的任务实际内容可能不同，但task_desc本身只保存一份任务的内容，
                这就导致当同时调起多个临时任务时，他们的执行内容都是相同的，为了满足这种情况，我们保存每次临时任务的内容，在执行时去获取
                每个taskinstance应该执行的那个任务
            '''
            new_task_name = format('{}_{}'.format(task_name, str(uniq_id)))
            ex_conf_dict = {'actual_dag_id': new_task_name}
            callback_dict = {'callback_url': callback_url, 'callback_id': id, 'extra_info': extra_info}
            ex_conf_dict['callback_info'] = callback_dict
            external_conf = json.dumps(ex_conf_dict)
            TaskService.update_task(task_id=new_task_name, task_code=task_code, is_temp=True)
            FastExecuteTask.add_and_update(task_name=new_task_name, real_task_name=task_name,external_conf=external_conf)
            # '''
            #     注意这里执行的仍是原有的任务名！虽然实际上是两个任务，但是对于用户来说只有一个任务
            # '''
            # TaskService.task_execute(task_name=task_name, execution_date=execution_date,external_conf=external_conf)
            # Alert.add_alert_job(task_name=task_name,execution_date=execution_date)
        except Exception as e:
            logging.error("fast_execute error code:2 " + str(e), exc_info=True)
            return

@csrf.exempt
@pipeline_api.route('/task/updateAndExe', methods=["GET", "POST"])
def task_update_and_execute():
    id = request_process(re=request, content="id")
    task_name = request_process(re=request, content="name")
    execution_date = request_process(re=request, content="execution_date")
    task_code = request_process(re=request, content="task_code")
    callback_url = request_process(re=request, content="callbackUrl")
    extra_info = request_process(re=request, content="args")

    th = threading.Thread(target=fast_execute,args=(id,task_name,task_code,callback_url,extra_info))
    th.start()
    return jsonify(code=0, data={'state': "SUCCESS"}, msg=None)
    # if execution_date is None:
    #     execution_date = DateUtil.format_millisecond_random(timezone.utcnow().replace(tzinfo=beijing))
    # else:
    #     execution_date = pendulum.parse(execution_date).replace(tzinfo=beijing)
    # if task_name:
    #     try:
    #         timestamp = DateUtil.get_timestamp(execution_date)
    #         '''
    #             临时执行任务的原理是，生成一个新的任务并且注册到task_desc中，将新任务的名字保存在taskinstance的extenal_conf中
    #             在taskinstance开始执行时，获取真实要执行的任务名，并且get对应的task内容
    #             为什么这样做？因为临时任务可能是同一时间调起多次的，他们的任务实际内容可能不同，但task_desc本身只保存一份任务的内容，
    #             这就导致当同时调起多个临时任务时，他们的执行内容都是相同的，为了满足这种情况，我们保存每次临时任务的内容，在执行时去获取
    #             每个taskinstance应该执行的那个任务
    #         '''
    #         new_task_name = format('{}_{}'.format(task_name,str(timestamp)))
    #         ex_conf_dict = {'actual_dag_id':new_task_name}
    #         callback_dict = {'callback_url':callback_url,'callback_id':id,'extra_info':extra_info}
    #         ex_conf_dict['callback_info']=callback_dict
    #         external_conf =json.dumps(ex_conf_dict)
    #         TaskService.update_task(task_id=new_task_name, task_code=task_code,is_temp=True)
    #         '''
    #             注意这里执行的仍是原有的任务名！虽然实际上是两个任务，但是对于用户来说只有一个任务
    #         '''
    #         TaskService.task_execute(task_name=task_name, execution_date=execution_date,external_conf=external_conf)
    #         Alert.add_alert_job(task_name=task_name,execution_date=execution_date)
    #     except Exception as e:
    #         logging.error("/task/execute code:2 " + str(e), exc_info=True)
    #         return jsonify(code=2, data={'state': 'FAILED'}, msg=str(e))
    #     return jsonify(code=0, data={'state': "SUCCESS"}, msg=None)
    # else:
    #     logging.error("/task/execute code:1 Missing required parameters！", exc_info=True)
    #     return jsonify(code=2, data={'state': 'FAILED'}, msg="Missing required parameters！")

@csrf.exempt
@pipeline_api.route('/test/callback', methods=["GET", "POST"])
def callback():
    # 用来测试callback的接口
    state = request_process(re=request, content="state")
    callback_id = request_process(re=request, content="callback_id")
    instance_id = request_process(re=request, content="instance_id")
    args = request_process(re=request,content="args")
    try:
        content = '{},{},{},{}'.format(state,callback_id,instance_id,args)
        send_ding_notify(title='callback',content=content,receiver=['wuhaorui'])
    except Exception as e:
        logging.error("/task/execute code:2 " + str(e), exc_info=True)
        return jsonify(code=2, data={'state': 'FAILED'}, msg=str(e))
    return jsonify(code=0, data={'state': "SUCCESS"}, msg=None)


@csrf.exempt
@pipeline_api.route('/task/getChildDependencies', methods=["GET", "POST"])
def get_child():
    # 用来测试callback的接口
    task_id = request_process(re=request, content="id")
    try:
        notebook=set()
        data = TaskService.get_child_dependencie(task_id,notebook)
    except Exception as e:
        logging.error("/task/execute code:2 " + str(e), exc_info=True)
        return jsonify(code=2, data={'state': 'FAILED'}, msg=str(e))
    return jsonify(code=0, data=data, msg=None)

@csrf.exempt
@pipeline_api.route('/task/fastBackfill', methods=["GET", "POST"])
def task_fast_backfill():
    task_id = request_process(re=request, content="taskId")
    task_name = request_process(re=request, content="taskName")
    callback_url = request_process(re=request, content="callbackUrl")
    extra_info = request_process(re=request, content="args")
    execution_date = DateUtil.format_millisecond(timezone.utcnow().replace(tzinfo=beijing))
    if task_name:
        try:
            ex_conf_dict = {}
            if callback_url:
                callback_dict = {'callback_url':callback_url,'callback_id':task_id,'extra_info':extra_info}
                ex_conf_dict['callback_info']=callback_dict
            external_conf =json.dumps(ex_conf_dict)
            TaskService.task_execute(task_name=task_name, execution_date=execution_date,external_conf=external_conf)
            Alert.add_alert_job(task_name=task_name,execution_date=execution_date)
        except Exception as e:
            logging.error("/task/fastBackfill code:2 " + str(e), exc_info=True)
            return jsonify(code=2, data={'state': 'FAILED'}, msg=str(e))
        return jsonify(code=0, data={'state': "SUCCESS"}, msg=None)
    else:
        logging.error("/task/fastBackfill code:1 Missing required parameters！", exc_info=True)
        return jsonify(code=2, data={'state': 'FAILED'}, msg="Missing required parameters！")


@csrf.exempt
@pipeline_api.route('/task/eventTrigger', methods=["GET", "POST"])
def event_trigger():
    ds_task_id = request_process(re=request, content="taskId")
    callback_url = request_process(re=request, content="callbackUrl")
    template_params = request_process(re=request, content="templateParams")
    tenant_id = request_get_tenantId(re=request)
    td = TaskDesc.get_task_with_tenant(ds_task_id=ds_task_id, tenant_id=tenant_id)
    if td:
        task_name = td.task_name
        uuid = UUIDGenerator.generate_uuid_no_index()
        callback_dict = None
        if callback_url:
            callback_dict = {'callback_url': callback_url, 'callback_id': ds_task_id, 'instance_id': uuid}

        try:
            json.dumps(template_params)
        except Exception as e:
            logging.error("/task/eventTrigger code:2 t" + str(e), exc_info=True)
            return jsonify(code=2, data={'state': 'FAILED'}, msg='template_params is not json object')
        EventTriggerMapping.add(uuid=uuid, task_name=task_name,
                                callback_info=json.dumps(callback_dict) if callback_dict else None,
                                template_params=template_params)
        return jsonify(code=0, data={'uuid': uuid}, msg=None)
    return jsonify(code=2, data={}, msg='task not found')



@csrf.exempt
@pipeline_api.route('/taskInstance/uuid/get', methods=["GET", "POST"])
def get_uuid_status():
    uuid = request_process(re=request, content="uuid")
    try:
        state = TaskinstanceService.get_ti_state_by_uuid(uuid)
    except Exception as e:
        logging.error("/taskInstance/uuid/get code:2 " + str(e), exc_info=True)
        return jsonify(code=2, data={'state': 'FAILED'}, msg=str(e))
    return jsonify(code=0, data={'ti_state': state}, msg=None)

@csrf.exempt
@pipeline_api.route('/cron/get', methods=["GET", "POST"])
def get_last_cron_date():
    crontab = request_process(re=request, content="crontab")
    num = request_process(re=request, content="num")
    nowDate = timezone.utcnow().replace(tzinfo=beijing)

    try :
        if not croniter.is_valid(crontab):
            logging.error("/cron/get code:2 ,error: wrong corntab {}".format(crontab), exc_info=True)
            raise ValueError("wrong corntab {}".format(crontab))
        cron = croniter(crontab, nowDate)
        cron.tzinfo = beijing
        result = []
        for _ in range(int(num)):
            e_date = cron.get_next(datetime)
            result.append(e_date.strftime('%Y-%m-%d %H:%M:%S'))
    except Exception as e :
        return jsonify(code=ReturnCodeConstant.CODE_ERROR, data={'state': 'FAILED'}, msg=str(e))
    return jsonify(code=ReturnCodeConstant.CODE_SUCCESS, data=result, msg=None)

@csrf.exempt
@pipeline_api.route('/ti/page', methods=["GET", "POST"])
def ti_page():
    from airflow.shareit.utils.state import State
    ds_task_name = request_process(re=request, content="name")
    page = request_process(re=request, content="page")
    size = request_process(re=request, content="size")
    state = request_process(re=request, content="state")
    start_date = request_process(re=request, content="start_date")
    end_date = request_process(re=request, content="end_date")
    cycle = request_process(re=request,content="cycle")
    schedule_type = request_process(re=request, content="schedule_type")
    template_code = request_process(re=request, content="template_code")
    group_uuid = request_process(re=request, content="group_uuid")
    owner = request_process(re=request, content="owner")
    backfill_label = request_process(re=request, content="backfill_label")
    tenant_id = request_get_tenantId(re=request)

    if not page or not size:
        return jsonify(code=2, data={'state': 'FAILED'},
                       msg="Missing required parameters！")

    now = utcnow()
    end_date = pendulum.parse(end_date) if end_date else now
    if DateUtil.date_compare(end_date,now):
        end_date = now

    task_filter = {
        "ds_task_name":ds_task_name,
        "cycle":cycle,
        "template_code":template_code,
        "group_uuid":group_uuid,
        "owner": owner,
    }
    ti_filter = {
        "state":state,
        "start_date":pendulum.parse(start_date) if start_date else None,
        "end_date": end_date,
        "schedule_type":schedule_type,
        "backfill_label":backfill_label
    }
    page_filter = {
        "page":int(page),
        "size":int(size)
    }

    try:
        data = TaskinstanceService.get_ti_page(task_filter=task_filter,ti_filter=ti_filter,page_filter=page_filter,tenant_id=tenant_id)
    except Exception as e:
        logging.error("/ti/paget code:2 " + str(e), exc_info=True)
        return jsonify(code=2, data={'state': 'FAILED'}, msg=str(e))
    return jsonify(code=0, data=data, msg=None)
