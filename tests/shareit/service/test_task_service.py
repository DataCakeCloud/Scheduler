# -*- coding: utf-8 -*-
"""
author: zhangtao
Copyright (c) 2021/6/30 Shareit.com Co., Ltd. All Rights Reserved.
"""
import json
import time

import unittest
from datetime import datetime

from croniter import croniter

from airflow import macros
# from airflow.shareit.grpc.service.task_grpc_service import TaskServiceGrpcService


from airflow.shareit.jobs.unified_sensor_job import UnifiedSensorJob
from airflow.shareit.operators.script_operator import ScriptOperator
from airflow.shareit.service.diagnosis_service import DiagnosisService
from airflow.shareit.utils import task_manager
from airflow.shareit.utils.task_util import TaskUtil
from airflow.shareit.www import pipeline
from airflow.utils import timezone

import pendulum

from airflow.shareit.check.check_dependency_helper import CheckDependecyHelper
from airflow.shareit.models.sensor_info import SensorInfo
from airflow.shareit.service.dataset_service import DatasetService
from airflow.shareit.service.task_service import TaskService
from airflow.shareit.trigger.trigger_helper import TriggerHelper
from airflow.shareit.utils.granularity import Granularity
from airflow.shareit.utils.state import State
from airflow.utils.timezone import beijing, my_make_naive, utcnow
from airflow.utils.db import provide_session
from airflow.models.dag import DagModel
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.shareit.models.dag_dataset_relation import DagDatasetRelation
from airflow.shareit.models.trigger import Trigger
from airflow.shareit.models.task_desc import TaskDesc


class TaskServiceTest(unittest.TestCase):
    @staticmethod
    def _my_print():
        print("Test start...")

    @provide_session
    def test_generate_trigger(self, session=None):
        from airflow.shareit.service.task_service import TaskService
        TriggerHelper.generate_trigger("test_time_scheduler_daily_0",
                                     execution_date=datetime(2022, 2, 27, 0, 0, 0, tzinfo=beijing), session=session)


    @provide_session
    def test_update_name(self, session=None):
        old_name = 'update_name11111'
        new_name = 'update_name1'
        from airflow.shareit.service.task_service import TaskService
        try:
            TaskService.update_task_name(old_name, new_name)
        except Exception as e:
            print e.message

    def test_clear(self):
        name = "32_58005"
        execution_date = "2024-03-27 00:00:00"
        count = 0
        parse_dt=[]
        try:
            if name and execution_date:
                execution_dates = execution_date.split(',')
                for dt in execution_dates:
                    dt = pendulum.parse(dt)
                    parse_dt.append(dt)
                TaskService.clear_instance(name,parse_dt)
                print count
            else:
                print '111'
        except Exception as e:
            print(e.message)

    def test_get_specified_task_instance_info(self):
        # self._my_print()
        info = TaskService.get_specified_task_instance_info(name='1_22560', page=1, size=3)
        print json.dumps(info)
        # a=[]
        # print type(a)
        from airflow.utils import timezone
        # dt_utc = datetime.utcnow()
        # dt_bj = dt_utc.replace(tzinfo=timezone.beijing)
        # print dt_utc.strftime('%Y-%m-%d %H:%M:%S')
        # print dt_bj.strftime('%Y-%m-%d %H:%M:%S')

    def test_set_failed(self):
        # self._my_print()
        TaskService.set_failed(dag_id="task-for-history-import-002", execution_date=pendulum.parse("2022/12/09 12:06"))

    def test_instance_relation(self):
        self._my_print()
        # res = TaskService.instance_relation_shell(core_task="test_time_scheduler_daily_3",
        #                                           execution_date=datetime(2022, 5, 10, 14, 10, 0, tzinfo=beijing),
        #                                           level=2,
        #                                           is_downstream=False)
        level = 3
        execution_date = '2022-09-07 15:52:35'
        execution_date = pendulum.parse(execution_date).replace(tzinfo=beijing)
        res = TaskService.instance_relation_shell(core_task='task66', execution_date=execution_date, level=level-1, is_downstream=True)
        print(res)

    def test_backfill_with_relation(self):
        self._my_print()
        res = TaskService.backfill_with_relation(task_ids=["yyy-2"],
                                                 core_task_name="yyy-2",
                                                 start_date=datetime(2022, 12, 02, 0, 0, 0, tzinfo=beijing),
                                                 end_date=datetime(2022, 12, 02, 23, 59, 59, tzinfo=beijing),
                                                 is_send_notify=False,
                                                 is_check_dependency=True,
                                                 operator=None)
        print(res)

    def test_success(self):
        TaskService.set_success(dag_id="test_echoecho", execution_date=pendulum.parse("2022/12/29 00:00:00"))


    def test_update_task(self):
        task_id = 'test_pushpush1'
        task_code = '''{
    "depend_types":"",
    "email_on_failure":true,
    "email_on_start":false,
    "email_on_success":false,
    "emails":"wuhaorui@ushareit.com",
    "end_date":"",
    "event_depends":[

    ],
    "extra_params":{
        "dingAlert":"true"
    },
    "filterField":{
        "excludes":[
            "execution_timeout"
        ],
        "includes":[

        ],
        "maxLevel":0
    },
    "input_datasets":[
        {
            "check_path":"",
            "date_calculation_param":{

            },
            "granularity":"daily",
            "guid":"",
            "id":"",
            "metadata":{
                "db":"",
                "region":"ue1",
                "source":"",
                "table":"",
                "type":"hive"
            },
            "offset":0,
            "ready_time":"0 0 * * *",
            "unitOffset":"-1",
            "use_date_calcu_param":false
        }
    ],
    "max_active_runs":1,
    "name":"test_pushpush1",
    "output_datasets":[
        {
            "id":"PythonShell_test_pushpush1",
            "offset":-1,
            "placeholder":true
        }
    ],
    "owner":"wuhaorui",
    "retries":1,
    "start_date":"",
    "task_items":[
        {
            "image":"848318613114.dkr.ecr.ap-southeast-1.amazonaws.com/bdp/workflow:script_task",
            "image_pull_policy":"IfNotPresent",
            "namespace":"bdp",
            "files":[

            ],
            "task_id":"test_pushpush1",
            "cluster_tags":"type:k8s,region:ue1,rbac.cluster:bdp-prod,sla:normal",
            "scripts":[
                "echo 1"
            ],
            "task_type":"ScriptOperator"
        }
    ],
    "trigger_param":{
        "crontab":"00 00 * * *",
        "crontab_param":{
            "endTime":"23:59",
            "fixedTime":"00:00",
            "interval":5,
            "range":[

            ],
            "startTime":"00:00"
        },
        "output_granularity":"daily",
        "type":"cron"
    },
    "version":2
}
'''

        TaskService.update_task(task_id=task_id, task_code=task_code,is_temp=False)

        # json.loads(task_code.strip())
        # TaskService.update_task(task_id,task_code)

    def test_backfill(self):
        task_ids = '57972,57973,57974,57975,57976'
        task_ids = task_ids.split(',')
        task_ids = ['{}_{}'.format(str(32), str(id)) for id in task_ids]
        start_date = '2024-01-13 00:00:00'
        end_date = '2024-01-15 00:00:00'
        # task_ids = ['32_57972','32_57973','32_57974','32_57975','32_57976']
        core_task = '32_57972'
        start_date = pendulum.parse(start_date)
        end_date = pendulum.parse(end_date)
        TaskService.backfill_with_relation(task_ids=task_ids,core_task_name=core_task,start_date=start_date, end_date=end_date,ignore_check=True)


    def test_get_all_running_dag_id(self):
        res = TaskService.get_all_running_dag_id()
        print(res)

    def test_get_relation(self):
        # exe_date = datetime(2022,6,27,2,0,0)
        # result = TaskService.instance_relation("new_6",execution_date=exe_date,is_downstream=True)
        # for i in  result[0]:
        #     print
        # dag_id = "test_merge_external2"
        # trigger = Trigger.get_last_trigger_data(dag_id=dag_id)
        # print trigger.dag_id, trigger.execution_date
        # res = CheckDependecyHelper.check_dependency(trigger.dag_id, trigger.execution_date, False)
        # print res

        loop_start_time = time.time()
        res = SensorInfo.get_checking_info()
        unique_res = UnifiedSensorJob._merge_redundant_sensor_info(si_list=res)
        check_si = []
        for si in unique_res:
            try:
                total_cost = (
                        timezone.make_naive(timezone.utcnow()) - si.first_check).total_seconds() if isinstance(
                    si.first_check, datetime) else 0
                last_cost = (timezone.make_naive(timezone.utcnow()) - si.last_check).total_seconds() if isinstance(
                    si.last_check, datetime) else 0
                if si.last_check is None:
                    check_si.append(si)
                elif total_cost > si.timeout_seconds:
                    si.update_state(State.FAILED)
                    # 检查超时失败发送钉钉报警
                    # UnifiedSensorJob.send_check_failed_notify(check_path=si.check_path,
                    #                               dag_id=si.dag_id,
                    #                               execution_date=si.execution_date)
                    dataset_time = Granularity.getPreTime(baseDate=si.execution_date, gra=si.granularity,
                                                          offset=si.offset) if si.detailed_execution_date is None else si.detailed_execution_date
                    # 检测超时后，生成下游任务终止状态的trigger，已有的trigger则更新为终止状态
                    DatasetService.trigger_downstream_task_by_dataset_info(metadata_id=si.metadata_id,
                                                                           execution_date=dataset_time,
                                                                           granularity=si.granularity,
                                                                           state=State.TASK_STOP_SCHEDULER)
                elif last_cost > si.poke_interval:
                    check_si.append(si)
                    if total_cost > 86400:
                        new_poke = int(si.poke_interval * 1.1)
                        new_poke = new_poke if new_poke < 86400 else 86400
                        si.update_poke(new_poke_interval=new_poke)
                else:
                    print ("[DataStudio Sensor] {metadata} {time} not in poke_interval ".format(
                        metadata=str(si.metadata_id),
                        time=str(si.execution_date)))
            except Exception:
                print ("[DataStudio Sensor] {metadata} {time} excute sensor Failed!".format(
                    metadata=str(si.metadata_id),
                    time=str(si.execution_date)))
        for si in check_si:
            print "-----------------"
            print si.dag_id

    def test_scheduler(self):
        datasets = DagDatasetRelation.get_all_outer_dataset()
        datasets = TaskUtil._merger_dataset_check_info(datasets=datasets)
        for ddr in datasets:
            if ddr.metadata_id == "python.test_merge_external@ue1":
                print ddr.dag_id,ddr.metadata_id,ddr.check_path,ddr.offset,ddr.granularity
    def test_merge(self):
        res = SensorInfo.get_checking_info()
        unique_res = UnifiedSensorJob._merge_redundant_sensor_info(si_list=res)
        print len(unique_res)
        for re in unique_res:
            if re.metadata_id =="python.test_merge_external@ue1":
                print re.metadata_id,re.dag_id,re.check_path


    def test_task_duration(self):
        dag = task_manager.get_dag("test_sg2_echo")
        ti = dag.get_task_instances()[0]
        ti_end_date = ti.end_date
        start_date = ti.start_date
        # start_date = datetime(2022, 7, 11, 14, 0, 5)
        # ti_end_date = datetime.utcnow()
        t_sec = (ti_end_date.replace(tzinfo=beijing) - start_date.replace(tzinfo=beijing)).total_seconds()
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
        print duration

    def test_diagnose_success(self):
        task_id = "new_external_test_2"
        execute_date = "2022/07/06 14:10"
        from airflow.shareit.service.diagnosis_service import DiagnosisService
        ds = DiagnosisService()
        start_date = pendulum.parse(execute_date)
        res = ds.diagnose_success(task_id=task_id,exe_date=start_date)
        print res


    def test_pod_Resource(self):
        from airflow.contrib.kubernetes.pod import Resources
        resources = """{
            "request_memory":"1g",
            "request_cpu":"1",
            "limit_memory":"800Mi",
            "limit_cpu":"1",
            "limit_gpu":"1"
        } """
        resources = json.loads(resources)
        print type(resources)
        resources = resources if isinstance(resources, dict) else {}
        input_resource = Resources()
        if resources:
            for item in resources.keys():
                print item
                setattr(input_resource, item, resources[item])
        print input_resource.__str__()

    def test_yaml_generate(self):
       resources = """{
                   "request_memory":"1g",
                   "request_cpu":"1",
                   "limit_memory":"800Mi",
                   "limit_cpu":"1",
                   "limit_gpu":"1"
               } """
       so = ScriptOperator(image="mysq:12",scripts="echo hello",workdir="/work",resources=json.loads(resources))
       so.dry_run()
       print so.executor_config
       print so.resources

    def test_analisys(self):
        dag = task_manager.get_dag(dag_id="test_python_shell_optimize")
        task = dag.get_task(task_id="test_python_shell_optimize")
        res = task.get_task_instances
        print res[0].get_template_context()
        # task.execute(context=context)

    def test_success_file(self):
        from airflow.shareit.service.task_service import TaskService
        ts = TaskService()
        context={}
        execution_date = pendulum.parse('2022-07-31 00:00:00')
        context["execution_date"] = execution_date
        context["macros"] = macros
        file_name = '''{{ (execution_date - macros.timedelta(days=1,minutes=1,hours=1,seconds=-1,weeks=1)).strftime("%Y%m%d_%H%M%S") }}/'''
        file_name = ts.render_api(file_name,context)
        print file_name

    def test_get_date_preview(self):
        task_gra='daily'
        task_crontab = '00 00 * * *'
        data_depend = '''
        {
    "dependId": "hive2shareStoreTest",
    "dependGra": "",
    "dateCalculationParam": {},
    "unitOffset": 0,
    "useDateCalcuParam": false,
    "granularity": "daily",
    "taskId": 35,
    "metadataId": ".sdasdasdas@ue1"
}
        '''
        task_depend = '''
        {"date_calculation_param":{"month":{"range":[],"type":"offset","unitOffset":"-1"},"day":{"range":["1","3","5","6","L+0"],"type":"range","unitOffset":"0"}},"dependGra":"","depend_id":"test_get_preview_date","granularity":"daily","taskId":"12860","type":0,"unitOffset":"-1","use_date_calcu_param":true}
        '''
        res = TaskService.get_date_preview(task_gra=task_gra,task_crontab=task_crontab,data_depend=data_depend)
        # res = TaskService.get_date_preview(task_gra=task_gra, task_crontab=task_crontab, task_depend=task_depend)
        print json.dumps(res)

    def test_task_kill(self):
        execution_date = "2022/12/29 00:00:00"
        execution_dates = execution_date.split(',')
        for dt in execution_dates:
            dt = pendulum.parse(dt)
            TaskService.set_failed(dag_id="test_echoecho", execution_date=dt)
        # from airflow.hooks.genie_hook import GenieHook
        # gh = GenieHook()
        # gh.check_job_exists("334c1a76-fac0-11ec-ac82-3256589a8389")

    def test_task_lastest7(self):
        # start_time = datetime.now()
        # names = ["echo_1"]
        # name_tuple_list = TaskServiceGrpcService.get_task_name_tuple_list(1, names)
        # # names1=["test_email_success"]
        # data = TaskService.get_result_last_7_instance(name_tuple_list)
        # print json.dumps(data)
        return

    def test_task_sensor(self):
        from airflow.shareit.models.sensor_info import SensorInfo
        si = SensorInfo()
        task_info = si.find(dag_id="1_23643",execution_date=datetime(2023,7,14))
        for si in task_info:
            if si.state == State.FAILED:
                # 检查超时失败发送钉钉报警
                # dataset_time = Granularity.getPreTime(baseDate=si.execution_date, gra=si.granularity,
                #                                       offset=si.offset) if si.detailed_execution_date is None else si.detailed_execution_date
                # 检测超时后，生成下游任务终止状态的trigger，已有的trigger则更新为终止状态
                print si.execution_date
                Trigger.sync_trigger_to_db(dag_id=si.dag_id, execution_date=si.execution_date,
                                           state=State.TASK_STOP_SCHEDULER,
                                           trigger_type="pipeline", editor="DataPipeline")


    def test_get_dag(self):
        import airflow.shareit.utils.task_manager
        dag = task_manager.get_dag("test_start_notify")
        task = dag.get_task("test_start_notify")
        print task.email_on_start

    def test_get_prev(self):
        crontab = "00 00-23/1 * * *"
        cron = croniter(crontab, utcnow())
        cron.tzinfo = beijing
        print cron.get_prev(datetime)

    def test_get_task_info(self):
        sensorInfors = SensorInfo.get_checking_info(dag_id="test_stop_sensor")
        execs = set(si.execution_date for si in sensorInfors)
        for execution_date in execs:
            SensorInfo.update_state_by_dag_id(dag_id=sensorInfors[0].dag_id,execution_date=execution_date,state=State.TASK_STOP_SCHEDULER)

    def test_task(self):
        name = "hive2sharestore"
        execution_date = pendulum.parse("2022/08/02 00:00")
        log_url = TaskService.get_dag_run_log(dag_id=name, execution_date=execution_date)
        print log_url

    def test_task_execute(self):
        task_name = 'test_update_and_execute'
        execution_date = pendulum.parse('2022-10-11 00:00:00').replace(tzinfo=beijing)
        external_conf = json.dumps({'callback_url':'http://ds-pipeline-test.ushareit.org/pipeline/test/callback','callback_id':'111'})
        TaskService.task_execute(task_name=task_name,execution_date=execution_date,external_conf=external_conf)


    def test_set_paused(self):
        TaskService.set_paused(task_name='1_23996',is_paused=True)

    def test_get_child_dependencie(self):
        task_id = 'shareit_workflow_n6_task1'
        result = TaskService.get_child_dependencie(task_id,set())
        print json.dumps(result)

    def test_new_backfill(self):
        core_task_name = '32_58005'
        sub_task_name_list = []
        start_date = pendulum.parse('2024-03-26 00:00:00')
        end_date = pendulum.parse('2024-03-26 23:59:59')
        ignore_check = True
        operator = 'wuhaorui'
        label = "ceshi"
        TaskService.backfill(core_task_name=core_task_name,sub_task_name_list=sub_task_name_list,
                             start_date=start_date,end_date=end_date,ignore_check=ignore_check,
                             operator=operator,label=label)