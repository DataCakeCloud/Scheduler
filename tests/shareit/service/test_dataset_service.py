# -*- coding: utf-8 -*-
"""
author: zhangtao
Copyright (c) 2021/6/30 Shareit.com Co., Ltd. All Rights Reserved.
"""
import json
import threading
import time

import unittest
from datetime import datetime, timedelta

import pendulum
from airflow import dag
from airflow.models import TaskInstance
from airflow.shareit.check.check_dependency_helper import CheckDependecyHelper
from airflow.shareit.jobs.alert_job import AlertJob
from airflow.shareit.models.alert_detail import AlertDetail
from airflow.shareit.models.regular_alert import RegularAlert
from airflow.shareit.scheduler.external_helper import ExternalManager
from airflow.shareit.scheduler.sensor_job_helper import ReadyTimeSensenJob, sensor_job_readytime_processor
from airflow.shareit.utils.alarm_adapter import build_import_sharestore_state_notify, send_ding_notify, \
    get_receiver_list, send_notify_by_alert_type, send_telephone_by_notifyAPI
from airflow.shareit.utils import task_manager, alarm_adapter
from airflow.shareit.utils.date_calculation_util import DateCalculationUtil
from airflow.shareit.utils.date_util import DateUtil
from airflow.shareit.utils.granularity import Granularity
from airflow.shareit.utils.spark_util import SparkUtil
from airflow.shareit.utils.state import State
from airflow.shareit.utils.task_cron_util import TaskCronUtil

from airflow.shareit.utils.task_util import TaskUtil
from airflow.utils import timezone
from croniter import croniter, croniter_range

from airflow.shareit.models.sensor_info import SensorInfo
from airflow.shareit.service.task_service import TaskService
from airflow.utils.timezone import beijing, utcnow, make_naive
from airflow.utils.db import provide_session
from airflow.shareit.models.dag_dataset_relation import DagDatasetRelation
from airflow.shareit.models.trigger import Trigger
from airflow.shareit.models.task_desc import TaskDesc
from airflow.shareit.service.dataset_service import DatasetService
from airflow.shareit.utils import task_manager

class DatasetServiceTest(unittest.TestCase):
    @provide_session
    def test_trigger_downstream_task_by_dataset_info(self, session=None):
        DatasetService.trigger_downstream_task_by_dataset_info(metadata_id="test.external_0@ue1",
                                                               execution_date=datetime(2022, 6, 28, 16, 0, 0,
                                                                                       tzinfo=beijing))

    def test_is_output_exist(self):
        res = DatasetService.is_output_exist(dag_id="df-my-ctr-deepfm2-v1",metadata_id="XXXXXX")
        print (res)


    def test_trigger_get(self):
        res = TaskDesc.get_task(task_name="test_update_task")
        print json.loads(res._input_datasets)[0]["check_path1"]
        print json.loads(res._input_datasets)[0].has_key("check_path1") and json.loads(res._input_datasets)[0]["check_path1"] == ""
        # from airflow.shareit.models.trigger import Trigger
        # tris = Trigger.get_untreated_triggers(dag_id="test_update_task")
        # execution_dates = [tri.execution_date for tri in tris]
        # for exe_date in execution_dates:
        #     SensorInfo.delete_by_execution_date(task_name="test_update_task", execution_date=exe_date)

    def test_update_task(self):
        task_id = 'test_no_check_path'
        task_code = '''{
    "depend_types":"dataset",
    "email_on_failure":true,
    "email_on_start":false,
    "email_on_success":false,
    "emails":"luhongyu@ushareit.com",
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
            "check_path":"s3://sprs-qz-dw-prod-ue1/datastudio/check_signal/test_no_check_path/{{execution_date.strftime('%Y%m%d_%H')}}/",
            "date_calculation_param":{

            },
            "detailed_gra":"",
            "granularity":"daily",
            "guid":"",
            "id":"ads_algo.check_path@ue1",
            "metadata":{
                "db":"ads_algo",
                "region":"ue1",
                "source":"",
                "table":"check_path",
                "type":"hive"
            },
            "offset":0,
            "unitOffset":"-1",
            "use_date_calcu_param":false
        },
        {
            "check_path":"",
            "date_calculation_param":{

            },
            "granularity":"daily",
            "guid":"",
            "id":"test.ue1_lhy_hivetomysql_1@ue1",
            "metadata":{
                "db":"test",
                "region":"ue1",
                "source":"",
                "table":"ue1_lhy_hivetomysql_1",
                "type":"hive"
            },
            "offset":0,
            "ready_time":"0 0 * * *",
            "unitOffset":"-1",
            "use_date_calcu_param":false
        }
    ],
    "max_active_runs":1,
    "name":"test_no_check_path",
    "output_datasets":[
        {
            "id":"ab_sys_dev.test_no_check_path@ue1",
            "metadata":{
                "db":"ab_sys_dev",
                "region":"ue1",
                "source":"",
                "table":"test_no_check_path",
                "type":"hive"
            },
            "offset":-1
        }
    ],
    "owner":"luhongyu",
    "retries":1,
    "start_date":"",
    "task_items":[
        {
            "image":"848318613114.dkr.ecr.ap-southeast-1.amazonaws.com/bdp/workflow:script_task",
            "image_pull_policy":"IfNotPresent",
            "namespace":"bdp",
            "files":[

            ],
            "task_id":"test_no_check_path",
            "cluster_tags":"type:k8s,region:ue1,rbac.cluster:bdp-prod,sla:normal",
            "scripts":[
                "echo hello"
            ],
            "task_type":"ScriptOperator"
        }
    ],
    "trigger_param":{
        "crontab":"00 00 * * *",
        "crontab_param":{

        },
        "output_granularity":"daily",
        "type":"data"
    },
    "version":10
}
    '''
        task_id1 = "test_hive2mysql_auto_create_table"

        task_code1 = """{
    "invokingStatus":true,
    "content":"JTJGKiUyMCUzRCUzRCUzRCUzRCUzRERFTU8lM0QlM0QlM0QlM0QlM0QlMjAqJTJGJTBBLS0lRTYlQjMlQTglRTYlODQlOEYlRTYlOTclQjYlRTklOTclQjQlRTUlOEYlOTglRTklODclOEYlRTUlOTIlOENhcyVFOCVCRCVBQyVFNiU4RCVBMiUyQyVFNSU4NSVCNyVFNCVCRCU5MyVFNSU4RiVBRiVFNiU5RiVBNSVFNyU5QyU4QiUyMiVFNiU4RiU5MCVFNyVBNCVCQSUyMiUwQVNFTEVDVCUwQSUyMCUyMG5hbWUlMkMlMEElMjAlMjBhZ2UlMkMlMEElMjAlMjBzY29yZSUyQyUwQUZST00lMEElMjAlMjBkZWZhdWx0LnN0dWRlbnQlMEFXSEVSRSUwQSUyMCUyMGR0JTIwJTNEJTIwJyU3QiU3QiUyMHllc3RlcmRheV9kc19ub2Rhc2glMjAlN0QlN0Qn",
    "templateCode":"Hive2Mysql",
    "runtimeConfig":{
        "targetSource":"11",
        "targetDB":"airflow",
        "targetTable":"student",
        "partitions":[
            {
                "name":"datepart",
                "value":"{{ yesterday_ds }}"
            }
        ],
        "owner":"luhongyu",
        "collaborators":[

        ],
        "dsGroups":[

        ],
        "group":"",
        "resourceLevel":"standard",
        "executionTimeout":0,
        "retries":1,
        "maxActiveRuns":1,
        "emails":"",
        "clusterSla":"normal",
        "alertMethod":[
            "dingTalk"
        ],
        "alertType":[
            2
        ],
        "acrossCloud":"common",
        "startDate":"",
        "endDate":"",
        "batchParams":"",
        "lifecycle":"Ec2spot",
        "columns":[
            {
                "columnType":"string",
                "name":"name",
                "data_type":"string",
                "columnComment":"",
                "isAdd":true,
                "columnName":"name"
            },
            {
                "columnType":"int",
                "name":"age",
                "data_type":"int",
                "columnComment":"",
                "isAdd":true,
                "columnName":"age"
            },
            {
                "columnType":"int",
                "name":"score",
                "data_type":"int",
                "columnComment":"",
                "isAdd":true,
                "columnName":"score"
            }
        ],
        "source":"task",
        "dbUser":"root",
        "connectionUrl":"jdbc:mysql://172.33.1.52:3306/?useUnicode=true&characterEncoding=utf8&useSSL=false",
        "sourceRegion":"ue1",
        "sourceColumns":[
            {
                "columnType":"VARCHAR(255)",
                "name":"name",
                "data_type":"string",
                "columnComment":"",
                "columnName":"name"
            },
            {
                "columnType":"VARCHAR(255)",
                "name":"age",
                "data_type":"int",
                "columnComment":"",
                "columnName":"age"
            },
            {
                "columnType":"VARCHAR(255)",
                "name":"score",
                "data_type":"int",
                "columnComment":"",
                "columnName":"score"
            }
        ],
        "dbPassword":"1rf2fguu9ZigSe49f8PkeByjo6ipWD5M",
        "cost":[
            {
                "value":100,
                "key":"shareitX"
            }
        ]
    },
    "name":"test_hive2mysql_auto_create_table",
    "description":"",
    "dependTypes":[

    ],
    "triggerParam":{
        "type":"cron",
        "outputGranularity":"daily",
        "crontabParam":{
            "endTime":"23:59",
            "fixedTime":"00:00",
            "interval":5,
            "range":[

            ],
            "startTime":"00:00"
        },
        "crontab":""
    },
    "eventDepends":[

    ],
    "inputDataset":[
        {
            "check_path":"",
            "dateCalculationParam":{

            },
            "granularity":"daily",
            "guid":"",
            "id":"",
            "metadata":{
                "db":"default",
                "region":"ue1",
                "source":"",
                "table":"student",
                "type":"hive"
            },
            "offset":0,
            "ready_time":"0 0 * * *",
            "unitOffset":"-1",
            "useDateCalcuParam":false
        }
    ],
    "outputDataset":[
        {
            "id":"Hive2Mysql_test_hive2mysql_auto_create_table",
            "offset":-1,
            "placeholder":true
        }
    ],
    "online":0,
    "id":16604
}"""

        TaskService.update_task(task_id=task_id, task_code=task_code)

        # json.loads(task_code.strip())
        # TaskService.update_task(task_id,task_code)

    def test_check_outDataset(self):
        # res = DagDatasetRelation.check_output(dag_id="test_up_task",metadata_id="python_shell.task2222_output@ue1")
        # print len(res)
        # if len(res) == 0:
        #     print "外部数据"
        # else:
        #     print "内部数据"
        tris = Trigger.get_untreated_triggers(dag_id="test_up_task")

        execution_dates = [tri.execution_date for tri in tris if tri.execution_date < datetime.utcnow()]
        print execution_dates


    def test_fix_time(self):
        from airflow.shareit.utils.date_util import DateUtil
        tris = Trigger.get_untreated_triggers(dag_id="test_no_check_path")
        print tris
        execution_dates = [tri.execution_date for tri in tris if DateUtil.date_compare(timezone.utcnow().replace(tzinfo=beijing),tri.execution_date)]

        print execution_dates

        # for exe_date in execution_dates:
        #     render_res = TaskUtil.render_datatime(content=content,
        #                                           execution_date=exe_date,
        #                                           dag_id="test_no_check_path",
        #                                           date_calculation_param=sensor[0].detailed_execution_date,
        #                                           gra=case1["detailed_gra"])
        #     print render_res
            # for render_re in render_res:
            #     SensorInfo.register_sensor(dag_id=task_name, metadata_id=input_data['id'],
            #                                execution_date=exe_date,
            #                                detailed_execution_date=render_re["date"],
            #                                granularity=granularity, check_path=render_re["path"],
            #                                cluster_tags=task_items[0]["cluster_tags"])
        # print execution_dates


    def test_sensor_info(self):
        # si = SensorInfo.get_checking_info(dag_id="test_sensor")[0]
        # print si.dag_id,si.execution_date
        # input_datasets = DagDatasetRelation.get_inputs(dag_id="test_sensor")
        # input_datasets = DateCalculationUtil.generate_ddr_date_calculation_param(input_datasets)
        # print input_datasets[0].date_calculation_param
        # data_dates = DateCalculationUtil.get_upstream_all_data_dates(execution_date=si.execution_date,
        #                                                              calculation_param=input_datasets[0].date_calculation_param,
        #                                                              gra=si.granularity)
        # print data_dates

        # res = SensorInfo.get_latest_sensor_data(metadata_id="test.store_sales@ue1")
        # print res.metadata_id,res.execution_date
        datasets = DagDatasetRelation.get_outer_dataset(dag_id="1_23179")
        merge_datasets = TaskUtil._merger_dataset_check_info(datasets=datasets)
        merge_datasets_dict = merge_datasets[1]
        divide_datasets = ExternalManager.divide_datasets(merge_datasets_dict, 1)
        print divide_datasets
        datasets = DateCalculationUtil.generate_ddr_date_calculation_param(divide_datasets[0])
        print datasets
        tl = TaskUtil()
        tl.external_data_processing(datasets)


    def test_check_depency_data(self):
         # CheckDependecyHelper.list_data_dependency_result_tuple(dag_id="test_sensor_ready_time",execution_date=datetime(2022, 10, 28, 17, 0, 0, tzinfo=beijing))
         res = CheckDependecyHelper.list_data_dependency_result_tuple(dag_id="populate_trigger_2",execution_date=datetime(2022, 10, 19, 2, 0, 0, tzinfo=beijing))
         print res
    def test_sensor_check(self):
        res = ReadyTimeSensenJob()
        res.start()

    def test_date(self):
        dag_id = "test_ready_time_5"
        crontab = TaskCronUtil.get_task_cron(dag_id)
        # cron = croniter("10 5 * * *")
        # cron.tzinfo = beijing
        # dt = cron.get_next(datetime)
        # dt = datetime(2022,11,7,15)
        # tl = TaskUtil()
        # dts = tl._get_indicate_datatime(cron_expression="0 * * * *", start_date=dt,
        #                                   offset=0)
        # print dts
        import croniter
        import datetime
        now = datetime.datetime(2022,11,7,16) - datetime.timedelta(seconds=1)
        cron = croniter.croniter("25 * * * *", now)

        print cron.get_next(datetime.datetime)

    def test_backfill_with_relation(self):
        res = TaskService.backfill_with_relation(task_ids=["test_ready_time_3"],
                                                 core_task_name="test_ready_time_3",
                                                 start_date=datetime(2022, 11, 7, 2, 0, 0, tzinfo=beijing),
                                                 end_date=datetime(2022, 11, 7, 3, 0, 0, tzinfo=beijing),
                                                 is_send_notify=False,
                                                 is_check_dependency=True,
                                                 operator=None)

    def test_merge(self):
        datasets = DagDatasetRelation.get_active_downstream_dags(metadata_id="test.test@ue1")
        res = TaskUtil._merger_dataset_check_info(datasets=datasets)
        ddr = None
        for dataset in res[0]:
            if dataset.dag_id == "test_reafy_time_lhy":
                ddr = dataset
        if ddr:
            print ddr.check_path
            if ddr.check_path is not None and not ddr.check_path.isspace() and ddr.check_path is not "":
                print "有路径"
            else:
                print "没有路径"

    def test_get_date(self):
        res = TaskCronUtil.get_all_date_in_one_cycle(task_name="test_date_path",execution_date=datetime(2022,11,11,12),cron="1 * * * *")
        print res[0]

    def test_external_state(self):
        from airflow.shareit.models.sensor_info import SensorInfo
        dag_id = 'test_check_path_state'
        metadata_id = 'asd.asdas@ue1'
        detailed_execution_date = datetime(2022, 11, 14, 23)
        res = SensorInfo().get_external_state(dag_id, metadata_id, detailed_execution_date)
        print(res)


    def test_check_event_dependency(self):
        res = CheckDependecyHelper.list_event_dependency_result_tuple(dag_id="test_check_taskinstance",execution_date=datetime(2022,11,18,0))
        # TaskCronUtil.get_all_date_in_one_cycle(task_name=, execution_date=dt)
        print res


    def test_get_version(self):
        res = TaskDesc.get_version(dag_id="test_check_taskinstance")
        print res

    def test_get_date_preview(self):
        task_gra='daily'
        task_crontab = '00 00 * * *'
        data_depend = '''{task_crontab=00 00 * * *, task_gra=daily, task_depend={"date_calculation_param":{"month":{"range":[],"type":"offset","unitOffset":"0"},"day":{"range":[],"type":"offset","unitOffset":"-2~-1"}},"depend_gra":"","depend_id":"testbyzt","granularity":"daily","isDelete":false,"metadataId":"test_db.test_create_iceberg3@ue1","taskId":"24","type":0,"unitOffset":"0","use_date_calcu_param":true}}
        '''
        task_depend = '''
{"date_calculation_param":{"month":{"range":[],"type":"offset","unitOffset":"0"},"day":{"range":[],"type":"offset","unitOffset":"-2~-1"}},"depend_gra":"","depend_id":"testbyzt","granularity":"daily","isDelete":false,"metadataId":"test_db.test_create_iceberg3@ue1","taskId":"24","type":0,"unitOffset":"0","use_date_calcu_param":true}'''
        # res = TaskService.get_date_preview(task_gra=task_gra,task_crontab=task_crontab,data_depend=data_depend)
        res = TaskService.get_date_preview(task_gra=task_gra, task_crontab=task_crontab, task_depend=task_depend)
        print json.dumps(res)

    def is_send_notify(self, state, notify_type):
        cur_task_param = TaskDesc.get_task(task_name="test_alert_dev").extra_param
        print cur_task_param
        if cur_task_param.has_key(state):
            types = json.loads(cur_task_param[state])["alertType"]
            print types
            if notify_type in types:
                return True
        else:
            if notify_type == "dingTalk":
                print cur_task_param.has_key("dingAlert")
                if cur_task_param.has_key("dingAlert"):
                    return True
            if notify_type == "phone":
                if cur_task_param.has_key("phoneAlert"):
                    return True
        return False

    def test_is_send_notify(self):
        from airflow.utils.state import State
        print self.is_send_notify(State.RETRY,"dingTalk")


    def test_dingding(self):
        dag = task_manager.get_dag(dag_id="test_alert_dev")
        task = dag.get_task(task_id="test_alert_dev")

        from airflow.utils.state import State
        from airflow.shareit.utils.alarm_adapter import send_telephone_by_notifyAPI, get_receiver_list
        cur_task_param = TaskDesc.get_task(task_name=task.task_id).extra_param
        print cur_task_param
        notifyCollaborator = json.loads(TaskDesc.get_task(task_name=task.task_id).extra_param[State.RETRY])[
            "notifyCollaborator"]
        if cur_task_param.has_key(State.RETRY):
            if notifyCollaborator:
                if task.notified_owner:
                    receivers = get_receiver_list(receiver=task.notified_owner)
                else:
                    receivers = get_receiver_list(receiver=task.owner)
            else:
                receivers = get_receiver_list(receiver=task.owner)
        else:
            receivers = get_receiver_list(receiver=task.owner)
        send_telephone_by_notifyAPI(receivers=receivers)
        
    def test_python_shell(self):
        dag = task_manager.get_dag(dag_id="1_21547")
        print dag
        task = dag.get_task(task_id="1_21547")
        print task
        context = {}
        task.execute(context=context)

    def check_is_notify(self,task_id,execution_date,try_number):
        ads = AlertDetail.get_alerts(task_name=task_id,execution_date=execution_date,try_number=try_number)
        return False if ads else True


    def test_dataset(self):
        owner = TaskDesc.get_owner_by_ds_task_name(ds_task_name="test_miration_file_ue12ue1")
        print get_receiver_list(owner)

        # tis = TaskInstance.get_all_import_sharestore_tasks()
        # ti = tis[0]
        # if ti:
        #     if self.check_is_notify(task_id=ti.task_id, execution_date=ti.execution_date, try_number=ti.try_number):
        #         print (timezone.utcnow().replace(tzinfo=beijing) - ti.update_date.replace(tzinfo=beijing)).total_seconds()
        #         dag_id = ti.task_id[:-18]
        #         print dag_id
        #         owner = TaskDesc.get_owner_by_ds_task_name(ds_task_name=dag_id)
        #         print owner
        #         content = build_import_sharestore_state_notify(dag_id=dag_id, task_name=ti.task_id,
        #                                                        execution_date=ti.execution_date)
        #         send_ding_notify(title="DataStudio 提示信息", content=content, receiver=get_receiver_list(owner))
        #         alert_detail = AlertDetail()
        #         alert_detail.sync_alert_detail_to_db(task_name=ti.task_id,
        #                                          execution_date = ti.execution_date,
        #                                          owner = owner,
        #                                          try_number=ti.try_number
        #                                      )
    def test_time_out_notify(self):
        dag = task_manager.get_dag(dag_id="1_23142")
        task = dag.get_task(task_id="1_23142")
        res = TaskInstance.get_taskinstance_durations_by_task_name(dag_id=task.dag_id,task_id=task.task_id)
        # durations = [task.duration for task in res]
        # print durations
        # print res[0].get_time_out_seconds()


    def test_spark_utils(self):
        command1 = "--deploy-mode cluster --conf spark.hadoop.lakecat.client.projectId=shareit --conf spark.hadoop.client.projectId=shareit --conf spark.kubernetes.driverEnv.HADOOP_USER_NAME=shareit#longxb --conf spark.hadoop.lakecat.client.userName=longxb  --driver-memory 5G --conf spark.rpc.message.maxSize=512 --conf spark.driver.maxResultSize=15g --conf spark.yarn.maxAppAttempts=1 --conf spark.dynamicAllocation.enabled=false --executor-memory 30G --executor-cores 4 --num-executors 100 --conf spark.kubernetes.container.image=swr.ap-southeast-3.myhuaweicloud.com/shareit-bdp/spark:2.4.3.14-rc4-hadoop-3.1.1-mrs-10 --jars \"obs://shareit-algo-data-sg/sharezone/jars/common/spark-tensorflow-connector_2.11-1.15.0.jar,obs://shareit-algo-data-sg/sharezone/jars/common/fastjson-1.2.75.jar,obs://bdp-deploy-sg/SPRS/push/psranking_2.11-0.1.0-SNAPSHOT.py\"  --conf spark.kubernetes.driverEnv.HADOOP_USER_NAME=longxb --conf spark.app.name=click_weightv4_tfrecord_hourly_noappcate --conf spark.app.taskId=18146 --conf spark.app.workflowId=0 --conf spark.kubernetes.driver.label.owner=longxb --conf spark.kubernetes.executor.label.owner=longxb --conf spark.kubernetes.driver.label.entry=DataStudio --conf spark.kubernetes.executor.label.entry=DataStudio --conf spark.kubernetes.driver.label.tenantId=1 --conf spark.kubernetes.executor.label.tenantId=1 --conf spark.kubernetes.driver.label.groupId=322022 --conf spark.kubernetes.executor.label.groupId=322022  obs://bdp-deploy-sg/SPRS/push/psranking_2.11-0.1.0-SNAPSHOT.py --resultPath obs://shareit-algo-data-sg/sharezone_model_server/ps_model/push_dnn_click_weightv4hourly_noappcate/shareit/     \\\n--date "
        print SparkUtil.spark_param_parsed(command=command1)

    def test_spark_util(self):

        command = '''
                        --deploy-mode cluster --conf spark.app.name=app_install_use_daily --conf spark.app.taskId=23420 --conf spark.app.workflowId=0 --conf spark.kubernetes.driver.label.owner=caifuli --conf spark.kubernetes.executor.label.owner=caifuli --conf spark.kubernetes.driver.label.entry=DataStudio --conf spark.kubernetes.executor.label.entry=DataStudio --conf spark.kubernetes.driver.label.tenantId=1 --conf spark.kubernetes.executor.label.tenantId=1 --conf spark.kubernetes.driver.label.groupId=0 --conf spark.kubernetes.executor.label.groupId=0 --conf spark.kubernetes.driverEnv.HADOOP_USER_NAME=shareit#caifuli --conf spark.hadoop.client.projectId=shareit --conf spark.hadoop.lakecat.client.projectId=shareit --conf spark.hadoop.lakecat.client.userName=caifuli s3://ecom-common-ue1/forever/caifuli/data_extract/app_install_use_daily_raw_extract.py --dt '''
        command1 = '''
                       --deploy-mode cluster --conf spark.app.name=backup_dwm_ecom_user_app_postback_and_event_sum_all_backup --conf spark.app.taskId=24428 --conf spark.app.workflowId=0 --conf spark.kubernetes.driver.label.owner=liulu --conf spark.kubernetes.executor.label.owner=liulu --conf spark.kubernetes.driver.label.entry=DataStudio --conf spark.kubernetes.executor.label.entry=DataStudio --conf spark.kubernetes.driver.label.tenantId=1 --conf spark.kubernetes.executor.label.tenantId=1 --conf spark.kubernetes.driver.label.groupId=0 --conf spark.kubernetes.executor.label.groupId=0 --conf spark.kubernetes.driverEnv.HADOOP_USER_NAME=shareit#liulu --conf spark.hadoop.client.projectId=shareit --conf spark.hadoop.lakecat.client.projectId=shareit --conf spark.hadoop.lakecat.client.userName=liulu --conf spark.kubernetes.container.image=848318613114.dkr.ecr.ap-southeast-1.amazonaws.com/bdp/spark:2.4.3.14-hadoop-2.8.5-amzn-5 --class com.ushareit.data.migration.DistCpMigration s3://shareit.deploy.ap-southeast-1/BDP/BDP-migration/ushareit-bigdata-migration-2.0-SNAPSHOT.jar --acrossCloud False'''

        lass_name, jar_py_path, spark_conf_dict, spark_param_list = SparkUtil.spark_param_parsed(command=command)
        print (lass_name, jar_py_path, spark_conf_dict, spark_param_list)

    def sync_data_to_regular_alert(self):
        def get_value_from_dict(dict_obj, key):
            if isinstance(dict_obj, unicode):
                dict_obj = json.loads(dict_obj)
            if not isinstance(dict_obj, dict):
                return None
            return dict_obj[key] if key in dict_obj.keys() else None

        from croniter import croniter, croniter_range
        tasks = TaskDesc.get_regular_alert_all_task()
        for task in tasks:
            if not DagModel.is_active_and_on(dag_id=task.task_name):
                continue
            regular_alert = get_value_from_dict(task.extra_param, "regularAlert")
            print regular_alert
            if regular_alert:
                triggerCondition = get_value_from_dict(regular_alert, "triggerCondition")
                notifyCollaborator = get_value_from_dict(regular_alert, "notifyCollaborator")
                graularity = get_value_from_dict(regular_alert, "graularity")
                offset = get_value_from_dict(regular_alert, "offset")
                alertType = ",".join(get_value_from_dict(regular_alert, "alertType"))
                checkTime = get_value_from_dict(regular_alert, "checkTime")

                print triggerCondition
                print notifyCollaborator
                print graularity
                print offset
                print alertType
                print type(alertType)
                print checkTime

            nowDate = timezone.utcnow()
            cron = croniter(task.crontab, nowDate)
            cron.tzinfo = beijing
            trigger_execution_date = cron.get_prev(datetime)
            print trigger_execution_date
            check_time = DateUtil.get_check_time(trigger_execution_date, graularity, checkTime, offset)
            print check_time
            # ra = RegularAlert()
            # ra.sync_regular_alert_to_db(
            #     task_name = task.task_name,
            #     execution_date = trigger_execution_date,
            #     alert_type = alertType,
            #     granularity = graularity,
            #     check_time = check_time,
            #     state = State.CHECKING,
            #     trigger_condition = triggerCondition,
            #     is_notify_collaborator = notifyCollaborator
            # )

    def check_regualer_alert(self):
        ras = RegularAlert.get_regular_alert_all_task()
        notStart = [State.TASK_UP_FOR_RETRY, State.FAILED, State.SUCCESS]
        for ra in ras:
            nowDate = timezone.utcnow().replace(tzinfo=beijing)
            if DateUtil.date_compare(nowDate, ra.check_time):
                try:
                    tis = TaskInstance.find_by_execution_date(dag_id=ra.task_name, task_id=ra.task_name,
                                                              execution_date=ra.execution_date.replace(tzinfo=beijing))
                    ti = tis[0] if tis else None
                    owners = TaskDesc.get_owner_by_ds_task_name(
                        ra.task_name) if ra.is_notify_collaborator else TaskDesc.get_owner(task_name=ra.task_name)
                    if ra.trigger_condition == 'notStart':
                        if not ti or (ti.state not in notStart):
                            send_notify_by_alert_type(ra.task_name, ra.execution_date, ra.alert_type, owners,
                                                      "任务未开始运行")
                    elif ra.trigger_condition == 'notSuccess':
                        if not ti or ti.state != 'notSuccess':
                            send_notify_by_alert_type(ra.dag_id, ra.execution_date, ra.alert_type, owners,
                                                      "任务未运行成功")
                    else:
                        print "没有当前告警类型{trigger_condition}".format(trigger_condition=ra.trigger_condition)
                    RegularAlert.update_ra_state(task_name=ra.task_name, execution_date=ra.execution_date,
                                                 state=State.SUCCESS, trigger_condition=ra.trigger_condition)
                except Exception as e:
                    print e.message
                    RegularAlert.update_ra_state(task_name=ra.task_name, execution_date=ra.execution_date,
                                                 state=State.FAILED, trigger_condition=ra.trigger_condition)

    def test_regular(self):
        aj = AlertJob()

        aj.send_phone_alert()
        # self.sync_data_to_regular_alert()
        # ti = TaskInstance.find_by_execution_date(dag_id="1_23550")[0]
        # notifyCollaborator = False
        # cur_task_param = TaskDesc.get_task(task_name=ti.task_id).extra_param
        # if cur_task_param.has_key(State.SUCCESS):
        #     notifyCollaborator = json.loads(TaskDesc.get_task(task_name=ti.task_id).extra_param[State.SUCCESS])[
        #         "notifyCollaborator"]
        # from airflow.shareit.models.regular_alert import RegularAlert
        # ra = RegularAlert()
        # ra.sync_regular_alert_to_db(
        #     task_name=ti.dag_id,
        #     execution_date=make_naive(ti.execution_date),
        #     alert_type='phone',
        #     state=State.CHECKING,
        #     trigger_condition=State.SUCCESS,
        #     is_notify_collaborator=notifyCollaborator,
        #     is_regular=False
        # )

    def test_sysnc_phone_alert(self):
        ti = TaskInstance.find_with_date_range(dag_id="1_25162")[0]
        notifyCollaborator = False
        cur_task_param = TaskDesc.get_task(task_name=ti.task_id).extra_param
        if cur_task_param.has_key(State.FAILED):
            notifyCollaborator = json.loads(TaskDesc.get_task(task_name=ti.dag_id).extra_param[State.FAILED])[
                "notifyCollaborator"]
        from airflow.shareit.models.regular_alert import RegularAlert
        ra = RegularAlert()
        ra.sync_regular_alert_to_db(task_name=ti.dag_id,execution_date=make_naive(ti.execution_date),alert_type='phone',state=State.CHECKING,trigger_condition=State.FAILED,is_notify_collaborator=notifyCollaborator,is_regular=False)

    def test_bash_operator(self):
        dag = task_manager.get_dag(dag_id="1_23613")
        task = dag.get_task(task_id="1_23613")
        context = {}
        task.execute(context=context)

    def test_json_transform(self):
        json_Str = """{
    "job": {
        "setting": {
            "speed": {
                "channel":1
            },
            "errorLimit": {
                "record": 0,
                "percentage": 0.02
            }
        },
        "content": [
            {
                "reader": {
                    "name": "streamreader",
                    "parameter": {
                        "column" : [
                            {
                                "value": "DataX",
                                "type": "string"
                            },
                            {
                                "value": 19890604,
                                "type": "long"
                            },
                            {
                                "value": "1989-06-04 00:00:00",
                                "type": "date"
                            },
                            {
                                "value": true,
                                "type": "bool"
                            },
                            {
                                "value": "test",
                                "type": "bytes"
                            }
                        ],
                        "sliceRecordCount": 100000
                    }
                },
                "writer": {
                    "name": "streamwriter",
                    "parameter": {
                        "print": false,
                        "encoding": "UTF-8"
                    }
                }
            }
        ]
    }
}"""

        import base64
        encoded_bytes = base64.b64encode(json_Str.encode('utf-8'))

        # json_one_line = json.dumps(json.loads(json_Str), separators=(',', ':'))

        print encoded_bytes


