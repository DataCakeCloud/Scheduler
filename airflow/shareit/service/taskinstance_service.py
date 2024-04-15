# -*- coding: utf-8 -*-
import json
import logging
import traceback
from collections import defaultdict

from airflow.utils import timezone

from airflow.models import TaskInstance
from airflow.shareit.constant.kyuubi_state import KyuubiState
from airflow.shareit.constant.taskInstance import FIRST, PIPELINE
from airflow.shareit.constant.trigger import TRI_PIPELINE, TRI_FORCE, TRI_CRON
from airflow.shareit.hooks.kyuubi_hook import KyuubiHook
from airflow.shareit.models.constants_map import ConstantsMap
from airflow.shareit.models.event_trigger_mapping import EventTriggerMapping
from airflow.shareit.models.task_desc import TaskDesc
from airflow.shareit.models.taskinstance_log import TaskInstanceLog
from airflow.shareit.models.trigger import Trigger
from airflow.shareit.utils.date_util import DateUtil
from airflow.shareit.utils.granularity import Granularity
from airflow.shareit.utils.state import State
from airflow.utils.db import provide_session
from airflow.utils.timezone import beijing


class TaskinstanceService:
    def __init__(self):
        pass

    @staticmethod
    def get_taskinstance_record(task_name,execution_date,ds_task_name=None):
        from airflow.shareit.service.task_service import TaskService
        result = []
        records = TaskInstanceLog.find_by_execution_date(task_name=task_name,execution_date=execution_date)
        # 获取一些需要的参数
        appurl_dict = ConstantsMap.get_value_to_dict(_type='spark_appurl')
        for ti_log in records:
            data = TaskService.get_format_taskinstance_data(ti_log,ds_task_name=ds_task_name,appurl_dict=appurl_dict)
            result.append(data)
        return result

    @staticmethod
    def get_ti_state_by_uuid(uuid):
        etm = EventTriggerMapping.get_etm(uuid=uuid)
        state = State.SCHEDULED
        if etm:
            try:
                task_name = etm.task_name
                execution_date = etm.execution_date
                if execution_date:
                    tis = TaskInstance.find_by_execution_date(dag_id=task_name, task_id=task_name,
                                                              execution_date=execution_date.replace(tzinfo=beijing))
                    if tis and len(tis) > 0:
                        ti = tis[0]
                        state = ti.state
                        if state == State.QUEUED:
                            state = State.RUNNING
                        if state == State.FAILED:
                            try:
                                kh = KyuubiHook()
                                kh.batch_id = uuid
                                kh.header = {}
                                job_info = kh.get_job_info()
                                if job_info and job_info['state'] == KyuubiState.KYUUBI_CANCELED:
                                    state = 'killed'
                            except Exception as e:
                                logging.error('Cannot connect to kyuubi server,{}'.format(e))
            except Exception as e:
                logging.error(traceback.format_exc())
                raise e
        return state

    @staticmethod
    @provide_session
    def get_ti_page(task_filter,ti_filter,page_filter,tenant_id=1,session=None):
        ans=[]
        task_name_list = []
        ti_dict = defaultdict(str)
        count = 0
        page = page_filter['page']
        size = page_filter['size']
        offset = (page - 1) * size
        task_dict = defaultdict(dict)
        ### 先筛选task
        if task_filter:
            ds_task_name = task_filter.get('ds_task_name')
            granularity = Granularity.getIntGra(task_filter.get('cycle')) if task_filter.get('cycle') else None
            template_code = task_filter.get('template_code')
            group_uuid = task_filter.get('group_uuid')
            owner = task_filter.get('owner')
            tuples = TaskDesc.get_task_name_list(ds_task_name=ds_task_name,owner=owner,granularity=granularity,template_code=template_code,tenant_id=tenant_id)
            for t in tuples:
                if t[1] and json.loads(t[1]).get('uuid') == group_uuid:
                    task_name_list.append(t[0])
                    task_dict[t[0]]['name'] = t[2]
                    task_dict[t[0]]['granularity'] = t[3]
                    task_dict[t[0]]['template_code'] = t[4]
                    task_dict[t[0]]['owner'] = t[5]

        if not task_name_list:
            return {'count': 0, 'task_info': []}

        # 获取一些需要的参数
        appurl_dict = ConstantsMap.get_value_to_dict(_type='spark_appurl', session=session)
        ### 再筛选ti
        if ti_filter:
            state = ti_filter.get('state')
            transform_states = State.taskinstance_state_transform(state)
            start_date = ti_filter.get('start_date')
            end_date = ti_filter.get('end_date')
            schedule_type = ti_filter.get('schedule_type')
            backfill_label = ti_filter.get('backfill_label')
            if schedule_type == TRI_PIPELINE:
                tri_types = [TRI_PIPELINE,TRI_FORCE,TRI_CRON]
                ti_types = [FIRST,PIPELINE]
            elif schedule_type :
                tri_types = [schedule_type]
                ti_types = [schedule_type]
            else:
                tri_types = []
                ti_types = []

            need_find_ti = True
            if state:
                if state in [State.TASK_STOP_SCHEDULER, State.TASK_WAIT_UPSTREAM,State.SUCCESS,State.FAILED]:
                    if state in [State.TASK_STOP_SCHEDULER, State.TASK_WAIT_UPSTREAM]:
                        # 这两个状态不需要再去查实例了
                        need_find_ti = False

                    count = Trigger.count_with_date_range(task_name_list=task_name_list, start_date=start_date, end_date=end_date,
                                                                  states=[state],tri_types=tri_types,backfill_label=backfill_label, session=session)
                    triggers = Trigger.find_with_date_range(task_name_list=task_name_list, start_date=start_date, end_date=end_date,
                                                                  states=[state],tri_types=tri_types,size=size,page=page,backfill_label=backfill_label,
                                                            offset=offset, session=session)
                else:
                    # 其他的状态，因为不存在于trigger表，所以不需要查询trigger表，只需要查询taskinstance表
                    count = TaskInstance.count_with_date_range(task_name_list=task_name_list, start_date=start_date, end_date=end_date,
                                                               ti_types=ti_types, states=transform_states,
                                                               backfill_label=backfill_label, session=session)
                    tis = TaskInstance.find_with_date_range(task_name_list=task_name_list, start_date=start_date,
                                                            end_date=end_date, ti_types=ti_types,
                                                            states=transform_states, size=size, offset=offset,
                                                            page=page, backfill_label=backfill_label, session=session)
                    temp_dict = defaultdict(list)
                    for ti in tis:
                        temp_dict[ti.dag_id].append(ti.execution_date.replace(tzinfo=timezone.beijing))

                    for ti in tis:
                        ti_data = TaskinstanceService.get_format_taskinstance_data(ti, ds_task_name=task_dict[ti.dag_id]['name'],appurl_dict=appurl_dict,
                                                                                   granularity=task_dict[ti.dag_id]['granularity'],template_code=task_dict[ti.dag_id]['template_code'],
                                                                                   owner=task_dict[ti.dag_id]['owner'],backfill_label=ti.backfill_label)
                        ans.append(ti_data)

                    return {'count': count, 'task_info': ans}
            else:
                count = Trigger.count_with_date_range(task_name_list=task_name_list, start_date=start_date,
                                                      end_date=end_date, tri_types=tri_types,
                                                      backfill_label=backfill_label, session=session)
                triggers = Trigger.find_with_date_range(task_name_list=task_name_list, start_date=start_date,
                                                        end_date=end_date, tri_types=tri_types, size=size,
                                                        offset=offset,
                                                        backfill_label=backfill_label, page=page, session=session)

            temp_dict = defaultdict(list)
            if need_find_ti:
                for t in triggers:
                    temp_dict[t.dag_id].append(t.execution_date.replace(tzinfo=timezone.beijing))

                for task_name,dates in temp_dict.items():
                    res = TaskInstance.find_by_execution_dates(dag_id=task_name,execution_dates=dates)
                    for ti in res:
                        k = '{},{}'.format(ti.dag_id,ti.execution_date.strftime('%Y-%m-%d %H:%M:%S.%f'))
                        ti_dict[k] = ti

            for tri in triggers:
                k = '{},{}'.format(tri.dag_id,tri.execution_date.strftime('%Y-%m-%d %H:%M:%S.%f'))
                task_type=TaskinstanceService.schedule_type_format(trigger_type=tri.trigger_type)
                if tri.state in [State.TASK_WAIT_UPSTREAM, State.TASK_STOP_SCHEDULER, State.BF_WAIT_DEP] \
                        or not ti_dict[k]:
                    # 如果trigger存在,但是taskInstance不存在,则插入一条假的instance,状态为等待上游就绪
                    # 毫秒时间戳
                    exe_date = DateUtil.get_timestamp(tri.execution_date) if tri.execution_date else None
                    ti_data = TaskinstanceService._get_instance_data(name=task_dict[tri.dag_id]['name'], execution_date=exe_date,task_type=task_type,
                                                     state=State.TASK_WAIT_UPSTREAM if tri.state in [State.TASK_WAIT_UPSTREAM, State.BF_WAIT_DEP] else State.TASK_STOP_SCHEDULER,
                                                    granularity=task_dict[tri.dag_id]['granularity'],template_code=task_dict[tri.dag_id]['template_code'],
                                                                     owner=task_dict[tri.dag_id]['owner'],backfill_label=tri.backfill_label)
                    ans.append(ti_data)

                    continue

                ti = ti_dict[k]
                ti_data = TaskinstanceService.get_format_taskinstance_data(ti,  ds_task_name=task_dict[tri.dag_id]['name'],appurl_dict=appurl_dict,
                                                                           granularity=task_dict[ti.dag_id]['granularity'],template_code=task_dict[ti.dag_id]['template_code'],
                                                                           owner=task_dict[ti.dag_id]['owner'],backfill_label=tri.backfill_label)
                ans.append(ti_data)
        return {'count': count, 'task_info': ans}


    @classmethod
    def _get_instance_data(cls, name=None, execution_date=None, start_date=None, end_date=None, state=None, type=None,
                           duration=None, genie_job_id="", genie_job_url="", try_number="", external_conf=None,task_type=None,is_kyuubi_job=False
                           ,spark_ui=None,template_code=None,granularity=None,owner=None,backfill_label=None):
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
               "task_type":TaskinstanceService.schedule_type_format(ti_type=task_type),
               "is_kyuubi_job": is_kyuubi_job,
               "spark_ui": spark_ui,
               "template_code":template_code,
               "granularity": Granularity.formatGranularity(granularity) if granularity else None,
               "owner":owner,
               "backfill_label": "" if not backfill_label else backfill_label,
               }
        if external_conf and isinstance(external_conf,dict):
            for k in external_conf:
                res[k] = external_conf[k]
        return res

    @classmethod
    def get_format_taskinstance_data(cls, taskInstance, ds_task_name=None,template_code=None,granularity=None,appurl_dict=None,owner=None,backfill_label=None):
        # 毫秒时间戳
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
        if state in [State.RUNNING,State.FAILED,State.SUCCESS,State.TASK_UP_FOR_RETRY]:
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
                                      external_conf=external_conf,spark_ui=spark_ui,template_code=template_code,
                                      granularity=granularity,owner=owner,backfill_label=backfill_label)

    @staticmethod
    def schedule_type_format(trigger_type=None,ti_type=None):
        if trigger_type:
            if trigger_type in (TRI_PIPELINE,TRI_CRON,TRI_FORCE) :
                return TRI_PIPELINE
            else :
                return trigger_type
        if ti_type:
            if ti_type in (PIPELINE,FIRST):
                return PIPELINE
            else:
                return ti_type