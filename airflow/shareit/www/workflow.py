# -*- coding: utf-8 -*-
"""
Author: Tao Zhang
Date: 2022/6/15
If you are doing your best,you will not have to worry about failure.
"""
import json
import logging
import multiprocessing
import traceback

import pendulum
import six
from flask import Blueprint, jsonify, request

from airflow.shareit.constant.return_code import ReturnCodeConstant
from airflow.shareit.models.task_desc import TaskDesc
from airflow.shareit.service.workflow_chart_service import WorkflowChartService
from airflow.shareit.service.workflow_service import WorkflowService
from airflow.utils import timezone
from airflow.utils.timezone import utcnow

from airflow.www.app import csrf

workflow_api = Blueprint("workflow_api", __name__)




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

def batch_backfill(workflow_id, start_date, end_date,is_send_notify,tenant_id):
    try:
        WorkflowService.batch_backfill(workflow_id=workflow_id, start_date=start_date, end_date=end_date,
                                       is_send_notify=is_send_notify,tenant_id=tenant_id)
    except Exception :
        logging.error(traceback.format_exc())

def request_get_user_info(re):
    user_info = re.headers.get('current_login_user')

    return user_info

def request_get_tenantId(re):
    user_info = request_get_user_info(re)
    user_info_dict = json.loads(user_info)
    return user_info_dict['tenantId']

@csrf.exempt
@workflow_api.route('/instance/date/list', methods=["GET", "POST"])
def list_instance_date():
    workflow_id = request_process(re=request, content="workflowID")
    page_size = request_process(re=request, content="pageSize")
    page_num = request_process(re=request, content="pageNum")
    tenant_id = request_get_tenantId(re=request)
    try:
        data = WorkflowService.list_instance_date(workflow_id,page_size,page_num,tenant_id=tenant_id)
    except Exception as e:
        logging.error("/instance/date/list " + str(e), exc_info=True)
        return jsonify(code=ReturnCodeConstant.CODE_ERROR, data={'state': 'FAILED'}, msg=str(e))
    return jsonify(code=ReturnCodeConstant.CODE_SUCCESS, data=data, msg=None)


@csrf.exempt
@workflow_api.route('/instance/list', methods=["GET", "POST"])
def list_instance():
    workflow_id = request_process(re=request, content="workflowID")
    page_size = request_process(re=request, content="pageSize")
    page_num = request_process(re=request, content="pageNum")
    start_date = request_process(re=request, content="startDate")
    end_date = request_process(re=request, content="endDate")
    tenant_id = request_get_tenantId(re=request)
    try:
        if start_date :
            start_date = pendulum.parse(start_date).replace(tzinfo=timezone.beijing)
        if end_date :
            end_date = pendulum.parse(end_date).replace(tzinfo=timezone.beijing)
        data = WorkflowService.list_instance(workflow_id,page_size,page_num,tenant_id=tenant_id,start_date=start_date,end_date=end_date)
    except Exception as e:
        logging.error("/instance/list " + str(e), exc_info=True)
        return jsonify(code=ReturnCodeConstant.CODE_ERROR, data={'state': 'FAILED'}, msg=str(e))
    return jsonify(code=ReturnCodeConstant.CODE_SUCCESS, data=data, msg=None)

@csrf.exempt
@workflow_api.route('/taskinstance/batch/clear', methods=["GET", "POST"])
def batch_clear():
    workflow_id = request_process(re=request, content="workflowID")
    execution_date = request_process(re=request, content="executionDate")
    tasks = request_process(re=request, content="tasks")
    tenant_id = request_get_tenantId(re=request)
    try:
        parse_dates=[]
        execution_dates = execution_date.split(',')
        for dt in execution_dates:
            dt = pendulum.parse(dt).replace(tzinfo=timezone.beijing)
            parse_dates.append(dt)
        for task in tasks:
            ds_task_name = task['taskName']
            td = TaskDesc.get_task_with_tenant(tenant_id=tenant_id,ds_task_name=ds_task_name)
            task['taskName'] = td.task_name
        WorkflowService.batch_clear(workflow_id,parse_dates,tasks,tenant_id=tenant_id)
    except Exception as e:
        logging.error("/task/diagnose code:2 " + str(e), exc_info=True)
        return jsonify(code=ReturnCodeConstant.CODE_ERROR, data={'state': 'FAILED'}, msg=str(e))
    return jsonify(code=ReturnCodeConstant.CODE_SUCCESS, data={'state': 'SUCCESS'}, msg=None)

@csrf.exempt
@workflow_api.route('/backfill', methods=["GET", "POST"])
def backfill():
    workflow_id = request_process(re=request, content="workflowID")
    start_date = request_process(re=request, content="startDate")
    end_date = request_process(re=request, content="endDate")
    is_send_notify = request_process(re=request, content="isSendNotify")
    is_send_notify = procees_bool(is_send_notify, False)
    tenant_id = request_get_tenantId(re=request)
    try:
        start_date = pendulum.parse(start_date).replace(tzinfo=timezone.beijing)
        end_date = pendulum.parse(end_date).replace(tzinfo=timezone.beijing)
        # 异步执行
        process = multiprocessing.Process(
            target=batch_backfill,
            args=(workflow_id,
                  start_date,
                  end_date,
                  is_send_notify,
                  tenant_id),
        )
        process.start()
        # WorkflowService.batch_backfill(workflow_id=workflow_id, start_date=start_date, end_date=end_date,
        #                          is_send_notify=is_send_notify)
    except Exception as e:
        logging.error("/task/diagnose code:2 " + str(e), exc_info=True)
        return jsonify(code=ReturnCodeConstant.CODE_ERROR, data={'state': 'FAILED'}, msg=str(e))
    return jsonify(code=ReturnCodeConstant.CODE_SUCCESS, data={'state': 'SUCCESS'}, msg=None)



@csrf.exempt
@workflow_api.route('/taskinstance/baseinfo', methods=["GET", "POST"])
def get_taskinstance_baseinfo():
    workflow_id = request_process(re=request, content="workflowID")
    execution_date = request_process(re=request, content="executionDate")
    ds_task_name = request_process(re=request, content="taskName")
    ds_task_id = request_process(re=request, content="taskID")
    tenant_id = request_get_tenantId(re=request)
    try:
        task_name = '{}_{}'.format(str(tenant_id),str(ds_task_id))
        execution_date = pendulum.parse(execution_date).replace(tzinfo=timezone.beijing)
        data = WorkflowService.get_taskinstance_baseinfo(workflow_id=workflow_id, execution_date=execution_date, task_name=task_name,
                                 ds_task_id=ds_task_id,tenant_id=tenant_id,ds_task_name=ds_task_name)
    except Exception as e:
        logging.error("/taskinstance/baseinfo " + str(e), exc_info=True)
        return jsonify(code=ReturnCodeConstant.CODE_ERROR, data={'state': 'FAILED'}, msg=str(e))
    return jsonify(code=ReturnCodeConstant.CODE_SUCCESS, data=data, msg=None)


@csrf.exempt
@workflow_api.route('/nextdate/list', methods=["GET", "POST"])
def list_workflow_next_execution_date():
    workflow_id_list = request_process(re=request, content="workflowList")
    tenant_id = request_get_tenantId(re=request)
    try:
        data = WorkflowService.list_workflow_next_execution_date(workflow_id_list=workflow_id_list,tenant_id=tenant_id)
    except Exception as e:
        logging.error("/task/diagnose code:2 " + str(e), exc_info=True)
        return jsonify(code=ReturnCodeConstant.CODE_ERROR, data={'state': 'FAILED'}, msg=str(e))
    return jsonify(code=ReturnCodeConstant.CODE_SUCCESS, data=data, msg=None)

# @csrf.exempt
# @workflow_api.route('/downtasks/list', methods=["GET", "POST"])
# def list_workflow_downtask():
#     workflow_id = request_process(re=request, content="workflowID")
#     tenant_id = request_get_tenantId(re=request)
#     try:
#         data = WorkflowService.list_workflow_down_task(workflow_id=workflow_id)
#     except Exception as e:
#         logging.error("/downtasks/list " + str(e), exc_info=True)
#         return jsonify(code=ReturnCodeConstant.CODE_ERROR, data={'state': 'FAILED'}, msg=str(e))
#     return jsonify(code=ReturnCodeConstant.CODE_SUCCESS, data=data, msg=None)


@csrf.exempt
@workflow_api.route('/lineagechart', methods=["GET", "POST"])
def get_lineagechart():
    workflow_id = request_process(re=request, content="workflowID")
    # version = request_process(re=request, content="version")
    execution_date = request_process(re=request, content="executionDate")
    tenant_id = request_get_tenantId(re=request)
    try:
        if execution_date :
            execution_date = pendulum.parse(execution_date).replace(tzinfo=timezone.beijing)
        else :
            raise ValueError('missing execution_date')

        data = WorkflowChartService.get_lineagechart(workflow_id=workflow_id,execution_date=execution_date,tenant_id=tenant_id)
    except Exception as e:
        logging.error("/lineagechart " + str(e), exc_info=True)
        return jsonify(code=ReturnCodeConstant.CODE_ERROR, data={'state': 'FAILED'}, msg=str(e))
    return jsonify(code=ReturnCodeConstant.CODE_SUCCESS, data=data, msg=None)


@csrf.exempt
@workflow_api.route('/gridchart', methods=["GET", "POST"])
def get_gridchart():
    workflow_id = request_process(re=request, content="workflowID")
    version = request_process(re=request, content="version")
    start_date = request_process(re=request, content="startDate")
    end_date = request_process(re=request, content="endDate")
    tenant_id = request_get_tenantId(re=request)
    print 'tenant_id:{}'.format(tenant_id)
    try:
        if start_date:
            start_date = pendulum.parse(start_date).replace(tzinfo=timezone.beijing)
        if end_date:
            end_date = pendulum.parse(end_date).replace(tzinfo=timezone.beijing)
        else:
            end_date = utcnow().replace(tzinfo=timezone.beijing)
        data = WorkflowChartService.get_gridchart(workflow_id=int(workflow_id), version=int(version),
                                                  start_date=start_date, end_date=end_date, tenant_id=tenant_id)
    except Exception as e:
        logging.error("/gridchart " + str(e), exc_info=True)
        return jsonify(code=ReturnCodeConstant.CODE_ERROR, data={'state': 'FAILED'}, msg=str(e))
    return jsonify(code=ReturnCodeConstant.CODE_SUCCESS, data=data, msg=None)

@csrf.exempt
@workflow_api.route('/ganttchart', methods=["GET", "POST"])
def get_ganttchart():
    workflow_id = request_process(re=request, content="workflowID")
    # version = request_process(re=request, content="version")
    execution_date = request_process(re=request, content="executionDate")
    tenant_id = request_get_tenantId(re=request)
    try:
        if execution_date and execution_date != '':
            execution_date = pendulum.parse(execution_date).replace(tzinfo=timezone.beijing)
        else :
            raise Exception('execution_date cannot be empty')
        data = WorkflowChartService.get_ganttchart(workflow_id=int(workflow_id),execution_date=execution_date,tenant_id=tenant_id)
    except Exception as e:
        logging.error("/ganttchart " + str(e), exc_info=True)
        return jsonify(code=ReturnCodeConstant.CODE_ERROR, data={'state': 'FAILED'}, msg=str(e))
    return jsonify(code=ReturnCodeConstant.CODE_SUCCESS, data=data, msg=None)


@csrf.exempt
@workflow_api.route('/linechart', methods=["GET", "POST"])
def get_linechart():
    workflow_id = request_process(re=request, content="workflowID")
    start_date = request_process(re=request, content="startDate")
    end_date = request_process(re=request, content="endDate")
    version = request_process(re=request, content="version")
    tenant_id = request_get_tenantId(re=request)
    try:
        if start_date:
            start_date = pendulum.parse(start_date).replace(tzinfo=timezone.beijing)
        if end_date:
            end_date = pendulum.parse(end_date).replace(tzinfo=timezone.beijing)
        else :
            end_date = utcnow().replace(tzinfo=timezone.beijing)
        data = WorkflowChartService.get_linechart(workflow_id=int(workflow_id), version=int(version), start_date=start_date,
                                      end_date=end_date,tenant_id=tenant_id)
    except Exception as e:
        logging.error("/linechart " + str(e), exc_info=True)
        return jsonify(code=ReturnCodeConstant.CODE_ERROR, data={'state': 'FAILED'}, msg=str(e))
    return jsonify(code=ReturnCodeConstant.CODE_SUCCESS, data=data, msg=None)


@csrf.exempt
@workflow_api.route('/draw', methods=["GET", "POST"])
def draw():
    tasks = request_process(re=request, content="tasks")
    tenant_id = request_get_tenantId(re=request)
    try:
        data = WorkflowChartService.draw(ds_task_list=tasks,tenant_id=tenant_id)
    except Exception as e:
        logging.error("/draw " + str(e), exc_info=True)
        return jsonify(code=ReturnCodeConstant.CODE_ERROR, data={'state': 'FAILED'}, msg=str(e))
    return jsonify(code=ReturnCodeConstant.CODE_SUCCESS, data=data, msg=None)

@csrf.exempt
@workflow_api.route('/instance/latest', methods=["GET", "POST"])
def list_workflow_taskinstance_latest():
    workflow_id_list = request_process(re=request, content="workflowList")
    num = request_process(re=request, content="num")
    tenant_id = request_get_tenantId(re=request)
    try:
        data = WorkflowService.list_workflow_taskinstance_latest(workflow_id_list=workflow_id_list,num=num,tenant_id=tenant_id)
    except Exception as e:
        logging.error("/draw " + str(e), exc_info=True)
        return jsonify(code=ReturnCodeConstant.CODE_ERROR, data={'state': 'FAILED'}, msg=str(e))
    return jsonify(code=ReturnCodeConstant.CODE_SUCCESS, data=data, msg=None)
