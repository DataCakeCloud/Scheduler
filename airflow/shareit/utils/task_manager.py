# -*- coding: utf-8 -*-
"""
Author: zhangtao
CreateDate: 2021/4/27
"""
import re
import sys
import json

import future
import pendulum
import datetime
import base64
import urllib

import six

from airflow.shareit.models.gray_test import GrayTest
from airflow.shareit.models.task_desc import TaskDesc
from airflow.configuration import conf


# _analysis_datetime = pendulum.from_timestamp
from airflow.shareit.utils.normal_util import NormalUtil


def _analysis_datetime(date_time):
    if isinstance(date_time, float):
        return pendulum.from_timestamp(date_time)
    elif isinstance(date_time, datetime.datetime):
        return date_time
    elif isinstance(date_time, six.string_types):
        if date_time.strip() == "":
            return None
        return pendulum.parse(date_time)
    else:
        raise ValueError("Unknown time type，The recommended time format is string '2021-09-01 12:30:00' ")

def _analysis_timedelta(seconds):
    return datetime.timedelta(seconds=seconds)


gray_class_model_map = {
    "GenieJobOperator": "airflow.shareit.operators.kyuubi_operator",
    "GeniePodOperator": "airflow.shareit.operators.kyuubi_pod_operator",
    "ImportSharestoreOperator": "airflow.shareit.operators.kyuubi_import_sharestore_operator",
    "ExportSharestoreOperator": "airflow.operators.export_sharestore_operator",
    "FlinkBatchOperator": "airflow.shareit.operators.flink_batch_operator",
    "ScriptOperator": "airflow.shareit.operators.kyuubi_script_operator",
    "KyuubiTrinoOperator": "airflow.shareit.operators.kyuubi_trino_operator",
    "KyuubiOperator": "airflow.shareit.operators.kyuubi_operator",
    "KyuubiPodOperator": "iarflow.shareit.operators.kyuubi_pod_operator",
    "KyuubiImportSharestoreOperator": "airflow.shareit.operators.kyuubi_import_sharestore_operator",
    "KyuubiScriptOperator": "airflow.shareit.operators.kyuubi_script_operator",
    "KyuubiHiveOperator": "airflow.shareit.operators.kyuubi_hive_operator",
    "BashOperator":"airflow.shareit.operators.bash_operator",
    "DatacakeQeOperator":"airflow.shareit.operators.datacake_qe_operator"
}

new_operator_dict = {
    "GenieJobOperator": "KyuubiOperator",
    "GeniePodOperator": "KyuubiPodOperator",
    "ImportSharestoreOperator": "KyuubiImportSharestoreOperator",
    "ExportSharestoreOperator": "ExportSharestoreOperator",
    "FlinkBatchOperator": "FlinkBatchOperator",
    "ScriptOperator": "KyuubiScriptOperator",
    "KyuubiTrinoOperator": "KyuubiTrinoOperator",
    "KyuubiOperator": "KyuubiOperator",
    "KyuubiPodOperator": "KyuubiPodOperator",
    "KyuubiImportSharestoreOperator": "KyuubiImportSharestoreOperator",
    "KyuubiScriptOperator": "KyuubiScriptOperator",
    "BashOperator":"BashOperator"
}


def get_dag_by_disguiser(task_name,session=None):
    '''
        disguiser！！伪装者
        push服务的任务，通过临时任务存储任务信息，但是最终得到对象，dag名和task名是原任务
    '''
    dag=None
    if session:
        td = TaskDesc.get_task(task_name=task_name,session=session)
    else:
        td = TaskDesc.get_task(task_name=task_name)
    if not td:
        return None
    task_desc_source = td.source_json
    primary_task_name = '{}_{}'.format(str(td.tenant_id),str(td.ds_task_id))
    if task_desc_source:
        if isinstance(task_desc_source, dict):
            dag = from_dict(task_desc_source, primary_task_name,session=session)
        else:
            dag = from_json(task_desc_source, primary_task_name,session=session)
    return dag

def get_dag(dag_id,session=None):
    """
    从数据库中查询出对应信息，并根据信息解析出Airflow需要的DAG和TASK实例

    :param session:
    :param dag_id:
    :return:
    """
    dag = None
    if session:
        td = TaskDesc.get_task(task_name=dag_id, session=session)
    else:
        td = TaskDesc.get_task(task_name=dag_id)

    if not td:
        return dag

    task_desc_source = td.source_json
    primary_task_name = dag_id
    # tenant_task = '{}_{}'.format(td.tenant_id, td.ds_task_id)
    # if dag_id != tenant_task:
    #     primary_task_name = dag_id
    # else :
    #     primary_task_name = tenant_task
    if task_desc_source:
        if isinstance(task_desc_source, dict):
            dag = from_dict(task_desc_source, primary_task_name,session=session)
        else:
            dag = from_json(task_desc_source, primary_task_name,session=session)

    return dag

def get_dag_by_soure_json(source_json,primary_task_name):
    """
    从数据库中查询出对应信息，并根据信息解析出Airflow需要的DAG和TASK实例

    :param session:
    :param dag_id:
    :return:
    """
    dag = None
    if source_json:
        if isinstance(source_json, dict):
            dag = from_dict(source_json,primary_task_name)
        else:
            dag = from_json(source_json,primary_task_name)
    return dag


def from_dict(task_desc_obj,primary_task_name,session=None):
    """
    从字典解析出DAG
    :param task_desc_obj:
    :return:
    """
    return analysis_dag(task_desc_obj,primary_task_name,session=session)


def from_json(task_json,primary_task_name,session=None):
    """
    从JSON格式解析DAG
    :param task_json:
    :return:
    """
    return from_dict(json.loads(task_json),primary_task_name,session=session)


def partition(words):
    res = {}
    for word1 in words:
        flag = True
        for word2 in res.keys():
            if hasSameChr(word1, word2):
                res[word2].append(word1)
                flag = False
                break
        if flag:
            res[word1] = [word1]
    return res


def hasSameChr(word1, word2):
    if len(word1) != len(word2):
        return False
    for cr in word1:
        if cr not in word2:
            return False
    return True


def analysis_dag(dag_dict,primary_task_name,session=None):
    """
    解析出DAG
    :param dag_dict:
    :return:
    """
    # TODO 所有的多余参数应该下推至主Task（或在存入数据库的时候进行处理）
    from airflow.models.dag import DAG
    # dag = DAG(dag_id=dag_dict["name"])
    job_name = dag_dict["name"]
    dag = DAG(dag_id=primary_task_name)
    extra_params = dag_dict['extra_params'] if 'extra_params' in dag_dict.keys() else {}
    default_args = {}
    for k, v in dag_dict.items():
        is_dag_param = True
        if k == "task_items":
            pass
        elif k == "schedule_interval":
            v = "None"
        elif k in ["start_date","end_date"]:
            continue
        elif k.endswith("_date"):
            # v should be timestamp
            v = _analysis_datetime(v)
        elif k == "max_active_runs":
            v = int(v)
        elif k == "retry_interval":
            v = int(v)
        elif k == "concurrency":
            v = int(v)
        elif k == "dagrun_timeout":
            if isinstance(v, datetime.timedelta):
                pass
            elif isinstance(v, int):
                v = datetime.timedelta(seconds=v)
        elif k == "default_args":
            default_args.update(v)
        elif k == "name":
            default_args["dag_name"] = primary_task_name
        else:
            is_dag_param = False
            default_args[k] = v
        if is_dag_param:
            setattr(dag, k, v)
    if dag.start_date is None:
        from airflow.utils import timezone
        dag.start_date = datetime.datetime(2018, 1, 1, 0, 0, tzinfo=timezone.beijing)
    task_value = {}
    for task in dag_dict['task_items']:
        if task.has_key('task_id'):
            if task['task_id'] == "createCheckFile":
                continue
        try:
            task_id = task['task_id']
            if str(task_id).endswith('_import_sharestore'):
                task_id = '{}_import_sharestore'.format(primary_task_name)
                job_name = '{}_import_sharestore'.format(job_name)
            else:
                task_id = primary_task_name
        except KeyError:
            # task_id = dag_dict["name"]
            task_id = primary_task_name

        task_value[task_id] = analysis_task(dag, task, default_args,task_id,extra_params,job_name=job_name,session=session)
    for task in dag_dict['task_items']:
        if task.has_key('task_id'):
            if task['task_id'] == "createCheckFile":
                continue
        try:
            task_id = task['task_id']
            if str(task_id).endswith('_import_sharestore'):
                task_id = '{}_import_sharestore'.format(primary_task_name)
                job_name = '{}_import_sharestore'.format(job_name)
            else :
                # 只有hive2sharestore有两个任务,其他直接跳过了
                continue
                # task_id = primary_task_name
        except KeyError:
            # task_id = dag_dict["name"]
            task_id = primary_task_name
        if "upstream_tasks" in task.keys():
            for up_t in task["upstream_tasks"]:
                if up_t in task_value.keys():
                    # 其实这里只有一种情况就是hive2sharestore任务,他的上游也一定是primary_task_name
                    # task_value[task_id].set_upstream(task_value[up_t])
                    task_value[task_id].set_upstream(task_value[primary_task_name])
                else:
                    # TODO upstream task not define
                    pass
    setattr(dag, "task_dict", task_value)
    return dag


def analysis_task(dag, task_dict, default_args,task_id=None,extra_params=None,job_name=None,session=None):
    """
    解析出DAG中的task
    :param task_dict:
    :return:
    """
    reload(sys)
    sys.setdefaultencoding("utf-8")
    # try:
    #     task_id = task_dict['task_id']
    # except KeyError:
    #     task_id = default_args["dag_name"]
    task_class = task_dict["task_type"]
    is_psql = False
    if 'command_tags' in task_dict.keys():
        if 'type:psql' in task_dict['command_tags']:
            is_psql = True
    try:
        # task_model = class_model_map[task_class]
        if is_psql:
            task_model = "airflow.shareit.operators.kyuubi_psql_operator"
            task_class = "KyuubiPsqlOperator"
        else:
            task_model = gray_class_model_map[task_class]
            task_class = new_operator_dict.get(task_class,task_class)
    except KeyError:
        # Get from config (GenieJobOperator,airflow.operators.genie_job_operator)
        task_model = conf.get("core", "default_task_model")
        task_class = conf.get("core", "default_task_type")
    import importlib
    model = importlib.import_module(task_model)
    class_t = getattr(model, task_class)
    op = class_t(task_id=task_id)
    # 这里的queue其实是future.types.newstr.newstr类型,但是这个类型会在后面插入数据库时报错,所以先在这里转换成str
    op.queue = str(op.queue)
    if job_name:
        setattr(op,'job_name',job_name)
    for k, v in task_dict.items():
        if k in ["retry_delay", "execution_timeout"]:
            v = _analysis_timedelta(v)
        elif k == 'task_id':
            continue
        elif k.endswith("_date"):
            v = _analysis_datetime(v)
        elif k in ["task_type", "task_model"]:
            continue
        elif k == 'cluster_tags':
            v = NormalUtil.cluster_tag_mapping_replace(v)

        if isinstance(v, str) or isinstance(v, unicode):
            tmp_list = v.split('/*zheshiyigebianmabiaoshi*/')
            if len(tmp_list) == 3:
                # base64 解码
                base64Decode = base64.b64decode(tmp_list[1])
                # URL 解码
                base64Decode = urllib.unquote(base64Decode)
                tmp_list[1] = base64Decode
                v = '/*zheshiyigebianmabiaoshi*/'.join(tmp_list)

        if isinstance(v, str) or isinstance(v, unicode):
            tmp_list = v.split('/*datacakebianma*/')
            if len(tmp_list) == 3:
                # base64 解码
                base64Decode = base64.b64decode(tmp_list[1])
                # URL 解码
                base64Decode = urllib.unquote(base64Decode)
                tmp_list[1] = base64Decode
                v = '/*datacakebianma*/'.join(tmp_list)

        setattr(op, k, v)
    for k, v in default_args.items():
        if k in {"retry_delay", "execution_timeout"}:
            v = _analysis_timedelta(v)
        elif k.endswith("_date"):
            v = _analysis_datetime(v)
        if isinstance(v, str) or isinstance(v, unicode):
            tmp_list = v.split('/*zheshiyigebianmabiaoshi*/')
            if len(tmp_list) == 3:
                # base64 解码
                base64Decode = base64.b64decode(tmp_list[1])
                # URL 解码
                base64Decode = urllib.unquote(base64Decode)
                tmp_list[1] = base64Decode
                v = '/*zheshiyigebianmabiaoshi*/'.join(tmp_list)

        if isinstance(v, str) or isinstance(v, unicode):
            tmp_list = v.split('/*datacakebianma*/')
            if len(tmp_list) == 3:
                # base64 解码
                base64Decode = base64.b64decode(tmp_list[1])
                # URL 解码
                base64Decode = urllib.unquote(base64Decode)
                tmp_list[1] = base64Decode
                v = '/*datacakebianma*/'.join(tmp_list)
        setattr(op, k, v)

    if extra_params:
        for k,v in extra_params.items():
            setattr(op, k, v)
    try:
        op.my_init()
    except Exception as e:
        pass
    op.dag = dag
    return op


def format_task_params(task_obj, default_args):
    task_dict = {}
    for k, v in default_args.items():
        setattr(task_dict, k, v)
    for k, v in task_obj.items():
        setattr(task_dict, k, v)
    if "_task_module" not in task_dict.keys():
        # Get from config (GenieJobOperator,airflow.operators.genie_job_operator)
        setattr(task_dict, "_task_module", conf.get("core", "default_task_model"))
        setattr(task_dict, "_task_type", conf.get("core", "default_task_type"))
    return task_dict


def format_dag_params(dag_obj):
    """

    :param dag_obj:
    :return:
    """
    dag_dict = {}
    default_args = {}
    for k, v in dag_obj.items():
        if k == "task_name":
            setattr(dag_dict, "dag_id", v)
            setattr(default_args, k, v)
        # elif k == k.endswith("_date"):
        #     setattr(dag_dict, k, _analysis_datetime(v))
        elif k == "schedule_interval":
            setattr(dag_dict, k, None)
        elif k in ["concurrency", "max_active_runs", "dagrun_timeout", "catchup", "tags"]:
            setattr(dag_dict, k, v)
        elif k == "default_args":
            if isinstance(v, str):
                v = json.loads(v)
            if isinstance(v, dict):
                default_args.update(v)
        elif k == "tasks":
            # Will process out of cycle
            pass
        else:
            setattr(default_args, k, v)
    if "dag_id" not in dag_dict.keys():
        raise ValueError("Missing parameter: task_name")
    if "tasks" not in dag_dict.keys():
        raise ValueError("Missing parameter: tasks")
    tasks = dag_dict["tasks"]
    if isinstance(tasks, str):
        tasks = json.loads(tasks)
    if isinstance(tasks, dict):
        setattr(dag_dict, "tasks", [format_task_params(tasks, default_args)])
    elif isinstance(tasks, list):
        setattr(dag_dict, "tasks", [format_task_params(task, default_args) for task in tasks])
    return dag_dict
