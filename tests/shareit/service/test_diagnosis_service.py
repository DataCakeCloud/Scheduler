# -*- coding: utf-8 -*-
import json
import time
import base64
import random
import unittest
from datetime import datetime

import pendulum

from airflow.models.cpu_memory import CpuMemory
from airflow.shareit.models.dataset_partation_status import DatasetPartitionStatus
from airflow.utils.timezone import beijing
from airflow.utils.db import provide_session


class TaskServiceTest(unittest.TestCase):
    @provide_session
    def test_generate_trigger(self, session=None):
        from airflow.shareit.service.diagnosis_service import DiagnosisService
        # dt = datetime.strptime('2021-10-25 00:00:00','%Y-%m-%d %H:%M:%S')
        dt = '2022/05/31 00:00'
        execution_date = pendulum.parse(dt)

        resp = DiagnosisService().diagnose('new_test_depend1',execution_date,'waiting_queue')
        print json.dumps(resp,ensure_ascii=False)

    @provide_session
    def test_insert_datapartition(self,session=None):
        dataset = DatasetPartitionStatus()
        # dataset = session.query(DatasetPartitionStatus).filter(DatasetPartitionStatus.metadata_id == 'python_shell.test_add_success_out@ue1').filter(DatasetPartitionStatus.dataset_partition_sign == datetime(year=2022, month=5,day=29)).one_or_none()
        dataset.state='failed'
        dataset.dataset_partition_sign=datetime(year=2022, month=5,day=19)
        dataset.metadata_id = 'python_shell.test_add_success_out@ue1'
        dataset.run_id = 'External_data_generator_backCalculation'
        # dataset.state = 'ccc'
        session.merge(dataset)
        session.commit()

    @provide_session
    def test_diagnoise(self, session=None):
        # 测试检查上游
        from airflow.shareit.service.diagnosis_service import DiagnosisService
        # name = '1_17397'
        # execution_date = '2023/02/14 00:0success0:00.000'
        name = '32_57970'
        ds_task_name = 'depend_E'
        execution_date = '2024/01/15 00:00:00'
        state = 'waiting'
        execution_date = pendulum.parse(execution_date).replace(tzinfo=beijing)
        result = DiagnosisService().diagnose_new(name, execution_date, state, ds_task_name=ds_task_name)
        # result = DiagnosisService().diagnose(name, execution_date, state)
        print(json.dumps(result))

    @provide_session
    def test_dataset_info(self, session=None):
        from airflow.shareit.service.dataset_service import DatasetService
        # dev 12935
        # name = 'bd_dwm_dev.watchit_daily_new_devices@ue1'
        # name = 'python_shell.check_path1@ue1'
        # name = 'python_shell.tx_path1@ue1'
        name = 'access_log.test_first_pipeline2@ue1'
        data = DatasetService.get_dataset_info(name)
        print(data)

    @provide_session
    def test_dagid_count(self, session=None):
        # 测试某一任务的实例个数
        from airflow.shareit.service.diagnosis_service import DiagnosisService
        task_id = 'repeat_lineage_root'
        result = DiagnosisService.dagid_count(task_id)
        print(result)

    @provide_session
    def test_task_instance_date(self, session=None):
        # 测试某一任务的实例日期
        from airflow.shareit.service.diagnosis_service import DiagnosisService
        # task_id = 'repeat_lineage_root'
        task_id = 'test_online_no'  # 新建未上线的任务
        # execution_date = '2022/08/16 00:00'
        execution_date = ''
        if not execution_date:
            print('----not execution_date')
            from datetime import datetime
            # execution_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # '2022-10-27 18:32:43'
            # execution_date = pendulum.parse(execution_date).replace(tzinfo=beijing)  # 2022-10-27T18:32:43+08:00

            # 方式2：获取北京时间
            import pytz
            execution_date = datetime.fromtimestamp(int(time.time()), pytz.timezone('Asia/Shanghai')).strftime(
                '%Y-%m-%d %H:%M:%S %Z%z')  # '2022-10-27 18:30:48 CST+0800'
            execution_date = pendulum.parse(execution_date).replace(tzinfo=beijing)  # 2022-10-27T18:30:48+08:00

            # 方式3：获取北京时间
            # from airflow.utils import timezone
            # execution_date = timezone.utcnow().replace(tzinfo=beijing)  # 2022-10-27 18:27:06.313763+08:00
        result = DiagnosisService.task_dates(task_id, execution_date)
        print(result)

    @provide_session
    def test_data_lineage(self, session=None):
        # 测试数据血缘
        from airflow.shareit.service.diagnosis_service import DiagnosisService
        # name = 'python_shell.task1111_input@ue1'  # 外部数据
        # name = 'python_shell.task333@ue1'  # 外部数据
        # name = 'depend_replace_day'  # dev
        name = '32_53060'
        # name = 'task'
        # execution_date = '2022/09/13 00:00'
        # execution_date = '2022/08/31 00:00'  # todo 再次测试
        # name = 'diagnose_daily'  # 有自依赖
        # execution_date = '2022-09-17 22:00:00'
        execution_date = '2023-11-08 00:00:00'

        # name = 'task666'  # 下游
        # execution_date = '2022-09-07 15:52:35'
        depth = 3
        upDown = 1  # 1检查上游， 2检查下游
        execution_date = pendulum.parse(execution_date).replace(tzinfo=beijing)
        result = DiagnosisService().data_lineage(name, execution_date, depth, upDown,tenant_id=32, ds_task_id=53060)
        print(json.dumps(result))

    @provide_session
    def test_data_link_analysis(self, session=None):
        # 测试链路分析页面
        from airflow.shareit.service.diagnosis_service import DiagnosisService
        # name = 'task'
        # name = 'diagnose_daily'
        # name = 'repeat_lineage_root'  # dev 链路分析天级依赖天级任务去重
        # name = 'diagnose_hour'  # dev 链路分析小时级依赖小时级任务去重
        name = '1_23183'
        num = 10
        gantt = 0  # 柱状图
        # execution_date = '2022/09/13 00:00'
        execution_date = '2023-04-11 00:00:00'
        ds_task_name = 'opt_check_5'

        execution_date = pendulum.parse(execution_date).replace(tzinfo=beijing)
        result = DiagnosisService().data_link_analysis(name, execution_date, num, gantt,ds_task_name=ds_task_name)
        print json.dumps(result)

    @provide_session
    def test_data_link_analysis_gantt(self, session=None):
        # 测试链路分析页面
        from airflow.shareit.service.diagnosis_service import DiagnosisService
        # name = 'task1111'
        name = '32_148'
        # name = 'diagnose_daily'
        num = 6
        gantt = 0  # 甘特图
        execution_date = '2023-08-08 00:00:00'
        execution_date = pendulum.parse(execution_date).replace(tzinfo=beijing)
        result = DiagnosisService().data_link_analysis(name, execution_date, num, gantt)
        print json.dumps(result)

    def test_work_resource(self):
        from airflow.configuration import conf
        # cpu_memory = CpuMemory().get_resource()
        host_pool = 'c3NocGFzcyAtcCAndytleEdpMlhzOGF1X2olSkRBPTNoZEJWUGs2Wk9yJyBzc2ggLW8gc3RyaWN0aG9zdGtleWNoZWNraW5nPW5vIHJvb3RAMTcyLjE3LjUzLjEyMg=='
        resources = base64.b64decode(host_pool).decode('utf-8').split(',')
        # resources = base64.b64decode(conf.get("core", "host_pool")).decode('utf-8').split(",")
        # if len(resources) == 1:
        #     return resources[0]
        cpu = float(conf.get('resource', 'cpu_threshold'))
        memory = float(conf.get('resource', 'memory_threshold'))
        sleep = int(conf.get('resource', 'sleep'))
        result = ''
        flag = True
        while flag:
            cpu_memory = CpuMemory().get_resource()
            random.shuffle(cpu_memory)
            for item in cpu_memory:
                for resource in resources:
                    if item[1] in resource and (item[2] < cpu and item[3] < memory):
                        result = resource
            if not result:
                # self.log.info("There are no qualified machines for scheduling")
                print "--------"
                time.sleep(sleep)
            else:
                print("===" + result)
                flag = False
        return result

    @staticmethod
    def test_id_spark():
        from airflow.shareit.grpc.client.greeter_client import GreeterClient
        name = 'dws_user_push_detail_di_rt'
        response = GreeterClient().getTaskInfoByID([{'taskId':17289}],tenant_id=1)
        print response.message
        # print response.code
        # print response.message
        # print response.info.id
        # print response.templateCode
        # print(result['code'])
        # print(result['message'])
        # print(result['info']['id'])
        # print(result['info']['isSparkTask'])

    @provide_session
    def test_dependency(self, session=None):
        # 测试链路分析页面
        from airflow.shareit.service.diagnosis_service import DiagnosisService
        name = 'task'
        # name = 'diagnose_daily'
        execution_date = '2022-09-17 22:00:00'

        execution_date = pendulum.parse(execution_date).replace(tzinfo=beijing)
        result = DiagnosisService().get_upstream(name, execution_date)
        print(result)
