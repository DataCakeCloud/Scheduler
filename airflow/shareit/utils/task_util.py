# -*- coding: utf-8 -*-
"""
author: zhangtao
Copyright (c) 2021/6/18 Shareit.com Co., Ltd. All Rights Reserved.
"""
import traceback
from datetime import datetime, timedelta

import jinja2
import pendulum
import six
from croniter import croniter, croniter_range

from airflow.shareit.models.dag_dataset_relation import DagDatasetRelation
from airflow.shareit.models.dataset_partation_status import DatasetPartitionStatus
from airflow.shareit.models.sensor_info import SensorInfo
from airflow.shareit.models.task_desc import TaskDesc
from airflow.shareit.models.trigger import Trigger
from airflow.shareit.utils.date_util import DateUtil
from airflow.shareit.utils.granularity import Granularity
from airflow.shareit.utils.state import State
from airflow.shareit.service.dataset_service import DatasetService
from airflow.shareit.utils.task_cron_util import TaskCronUtil
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.timezone import beijing, utcnow
from airflow.shareit.utils.date_calculation_util import DateCalculationUtil

class TaskUtil(LoggingMixin):
    def external_data_processing(self,datasets):
        from airflow.models.dag import DagModel
        try:
            datasets = DateCalculationUtil.generate_ddr_date_calculation_param(datasets)
        except Exception:
            self.log.error(traceback.format_exc())
            raise
        '''
        # 因为DatasetPartitionStatus不区分dag_id, 当在获取最近一次时间latest_date时,只能通过metadata_id去获取,一旦被更新,下一个相同的metadata_id获取时,latest_date就改变了
        # 这不是我们想要的结果,每次循环我们要保证所有相同的metadata_id都有相同的latest_date
        '''
        dataset_status_dict={}

        for dataset in datasets:
            try:
                dag_id = dataset.dag_id
                if not DagModel.is_active_and_on(dag_id=dag_id):
                    continue
                # 获取依赖这个上游数据集的最近一次execution_date
                # execution_date = TaskCronUtil.get_pre_execution_date(dag_id, utcnow())
                crontab = TaskCronUtil.get_task_cron(dag_id)
                # execution_date = Granularity.get_execution_date(base_time=utcnow(), gra=granularity)
                self.log.info('dag_id:{},dataset:{}'.format(dataset.dag_id, dataset.metadata_id))
                offset = -1
                if dataset.offset is not None :
                    if dataset.offset == 0 and dataset.date_calculation_param is not None:
                        offset = -1
                    else :
                        offset = dataset.offset
                # offset = dataset.offset if dataset.offset is not None
                date_calculation_param = dataset.date_calculation_param
                metadata_id = dataset.metadata_id
                ready_time = dataset.ready_time
                # offset = dataset.offset if dataset.offset is not None else -1
                granularity = dataset.granularity
                check_path = dataset.check_path
                cluster_tags = self._get_cluster_tags(dag_id=dag_id)
                # 获取到当前execution_date依赖的所有data_dates
                # data_dates = DateCalculationUtil.get_upstream_all_data_dates(execution_date=execution_date,
                #                                                              calculation_param=date_calculation_param,
                #                                                              gra=granularity)

                latest = SensorInfo.get_latest_sensor_info(dag_id=dag_id, metadata_id=metadata_id)
                execution_dates = []
                if latest:
                    now_date = utcnow().replace(tzinfo=beijing)
                    latest_date = latest.execution_date.replace(tzinfo=beijing)
                    # 只生成当前任务本次上线之后的实例，历史实例跳过(本次上线实际时间取上一粒度的开始时间，防止当天任务无法启动)
                    ## 只catchup一天内的
                    latest_date = now_date - timedelta(days=1) if (now_date - latest_date).total_seconds() > 86400 else latest_date + timedelta(seconds=1)
                    # 与任务最后一次上线时间做比较,只生成最后一次上线时间之后的sensor
                    td = TaskDesc.get_task(task_name=dag_id)
                    if td and td.last_update:
                        if DateUtil.date_compare(td.last_update - timedelta(hours=1),latest_date):
                            latest_date = td.last_update.replace(tzinfo=beijing)
                    for dt in croniter_range(latest_date, now_date, crontab):
                        execution_dates.append(dt)
                else:
                    cron = croniter(crontab, utcnow())
                    cron.tzinfo = beijing
                    execution_dates.append(cron.get_prev(datetime))

                if check_path and check_path.strip():
                    for exe_date in execution_dates:
                        render_res = TaskUtil.render_datatime(content=check_path, execution_date=exe_date,
                                                              dag_id=dag_id,
                                                              date_calculation_param=date_calculation_param,
                                                              gra=granularity)

                        for render_re in render_res:
                            SensorInfo.register_sensor(dag_id=dag_id, metadata_id=metadata_id,
                                                       execution_date=exe_date,
                                                       detailed_execution_date=render_re["date"],
                                                       granularity=granularity, check_path=render_re["path"],
                                                       cluster_tags=cluster_tags)
                            self.log.info("[DataStudio Pipeline] {dag_id} {metadata_id} {dt} register to sensor..".format(dag_id=dag_id,
                                metadata_id=metadata_id,
                                dt=str(exe_date)))

                else:
                    for exe_date in execution_dates:
                        data_dates = DateCalculationUtil.get_upstream_all_data_dates(execution_date=exe_date,
                                                                                     calculation_param=dataset.date_calculation_param,
                                                                                     gra=dataset.granularity)
                        if ready_time and len(ready_time) > 5:
                            dt = TaskCronUtil.get_all_date_in_one_cycle(task_name=dag_id,
                                                                   execution_date=exe_date,
                                                                   cron=ready_time,
                                                                   gra=int(granularity))
                            completion_time = dt[0]
                        else:
                            completion_time = DateUtil.date_format_ready_time_copy(exe_date, ready_time,granularity)

                        SensorInfo.register_sensor(dag_id=dag_id, metadata_id=metadata_id,
                                                       execution_date=exe_date,
                                                       detailed_execution_date=data_dates[0],
                                                       granularity=granularity,
                                                       cluster_tags=cluster_tags,
                                                       completion_time=completion_time)
            except Exception as e:
                s = traceback.format_exc()
                self.log.error(s)
                self.log.error("external data process filed: " + str(dataset.dag_id), exc_info=True)
                continue
    def get_next_completion_date(self,cron_expression,cur_date):
        import croniter
        import datetime
        cur_date = cur_date - datetime.timedelta(seconds=1)
        cron = croniter.croniter(cron_expression, cur_date)
        return cron.get_next(datetime.datetime)



    def _get_cluster_tags(self, dag_id=None):
        from airflow.shareit.models.task_desc import TaskDesc

        task_desc_source = TaskDesc.get_source_json(task_name=dag_id)
        # task_desc_source["task_items"][0]["cluster_tags"]
        task_items = task_desc_source["task_items"] if "task_items" in task_desc_source.keys() else []
        for task_item in task_items:
            task_id = task_item["task_id"] if "task_id" in task_item.keys() else dag_id
            if task_id.endswith('import_sharestore'):
                continue
            cluster_tags = task_item["cluster_tags"] if "cluster_tags" in task_item.keys() else ""
            return cluster_tags

    def _get_indiacte_split_by_gra(self, gra=None, start_date=None, offset=None):
        # 计算获取该粒度从start_date开始到utcnow - offset 的所有的execution_date
        res = []
        if gra is None or offset is None or start_date is None:
            return res
        base_time = Granularity.get_execution_date(base_time=utcnow(), gra=gra)
        if isinstance(start_date, datetime):
            start_date = start_date.replace(tzinfo=beijing)
            task_st = Granularity.getNextTime(baseDate=start_date, gra=gra, offset=offset)
            while base_time > task_st:
                res.append(base_time)
                base_time = Granularity.getPreTime(baseDate=base_time, gra=gra, offset=-1)
        else:
            res.append(base_time)
        return res

    # def check_outer_dataset(self):
    #     datasets = DagDatasetRelation.get_all_outer_dataset()
    #     for dataset in datasets:
    #         # TODO maybe to check whether the granularity matches the ready time
    #         metadata_id = dataset.metadata_id
    #         ready_time = dataset.ready_time
    #         latest = DatasetPartitionStatus.get_latest_external_data(metadata_id=metadata_id)
    #         latest_date = latest.dataset_partition_sign if latest else None
    #         # 先前多补offset的数据
    #         tmp_date = Granularity.getPreTime(utcnow(), dataset.granularity, dataset.offset)
    #         backfill_dts = self._get_indicate_datatime(cron_expression=ready_time, start_date=tmp_date)
    #         for dt in backfill_dts:
    #             DatasetPartitionStatus.sync_state(metadata_id=metadata_id, dataset_partition_sign=dt,
    #                                               run_id="External_data_generator", state=State.SUCCESS)
    #         dts = self._get_indicate_datatime(cron_expression=ready_time, start_date=latest_date)
    #         for dt in dts:
    #             DatasetPartitionStatus.sync_state(metadata_id=metadata_id, dataset_partition_sign=dt,
    #                                               run_id="External_data_generator", state=State.SUCCESS)
    #             # trigger downstream task
    #             # TODO generate only this ddr task, not all downstream task
    #             self._generate_trigger(metadata_id=metadata_id, granularity=dataset.granularity, execution_date=dt)
    #         else:
    #             # TODO the outer_dataset don`t have a ready_time
    #             pass

    def _get_indicate_datatime(self, cron_expression=None, start_date=None, offset=None):
        res = []
        if offset is None or cron_expression is None:
            return res
        try:
            cron = croniter(cron_expression)
            cron.tzinfo = beijing
            dt = cron.get_prev(datetime)
            if offset > 0:
                return [dt]
            else:
                idx = 0
                while idx != offset:
                    dt = cron.get_prev(datetime)
                    idx -= 1
                if isinstance(start_date, datetime):
                    start_date = start_date.replace(tzinfo=beijing)
                else:
                    return [dt]
                while dt > start_date:
                    res.append(dt)
                    dt = cron.get_prev(datetime)
        except Exception as e:
            self.log.error("Error parsing cron expression", exc_info=True)
        return res

    # def _generate_trigger(self, metadata_id, granularity, execution_date):
    #     ddrs = DagDatasetRelation.get_downstream_dags(metadata_id=metadata_id)
    #     for ddr in ddrs:
    #         if ddr.granularity == granularity:
    #             exe_time = Granularity.getNextTime(execution_date, ddr.granularity, ddr.offset)
    #             # 计算 trigger
    #             Trigger.sync_trigger_to_db(dag_id=ddr.dag_id, execution_date=exe_time, state=State.WAITING,
    #                                        trigger_type="pipeline", editor="DataPipeline")

    def trigger_self_reliance_task(self):
        """
        trigger self-reliance task
        :return:
        """
        from airflow.models.dag import DagModel
        # 从DDR表获取自依赖的dataset
        ddrs = DagDatasetRelation.get_self_reliance_dataset()
        for ddr in ddrs:
            if not DagModel.is_active_and_on(ddr.dag_id):
                continue
            res = DatasetPartitionStatus.get_state(metadata_id=ddr.metadata_id)
            if res is None:
                # 检查该数据在无记录
                exe_time = Granularity.get_execution_date(utcnow(), ddr.granularity)
                tmp_date = Granularity.getPreTime(exe_time, ddr.granularity, ddr.offset)
                backfill_dts = Granularity.splitTimeByGranularity(tmp_date, utcnow(), ddr.granularity)
                for dt in backfill_dts:
                    # 生产offset的数据
                    DatasetPartitionStatus.sync_state(metadata_id=ddr.metadata_id, dataset_partition_sign=dt,
                                                      run_id="External_data_generator", state=State.SUCCESS)

                # trigger一个当前粒度的任务
                Trigger.sync_trigger_to_db(dag_id=ddr.dag_id, execution_date=exe_time, state=State.WAITING, priority=5,
                                           trigger_type="pipeline", editor="DataPipeline")

    @staticmethod
    def _merger_dataset_check_info(datasets):
        """
        临时方案，
        如果同时有check路径和时间，以路径为准
        如果有多个不同路径，暂时不处理
        如果有多个时间，以最晚时间为准
        {"last_ready_time":"0 23 * * *",
        "check_path":"s3://XXXX/XXX",
        "ddr_list":[XX,XXX,XXX,...]}
        """
        res = []
        groups = []
        res_dict = {} # key: metadata_id
        for dataset in datasets:
            metadata_id = dataset.metadata_id
            ready_time = dataset.ready_time
            offset = dataset.offset if dataset.offset is not None else -1
            granularity = dataset.granularity
            check_path = dataset.check_path
            is_grouped = False
            for group in groups:
                if group["ddr_list"][0].metadata_id == metadata_id and \
                        group["ddr_list"][0].offset == offset and \
                        group["ddr_list"][0].granularity == granularity:
                    group["ddr_list"].append(dataset)
                    # 更新check_path或者ready_time
                    has_check_path = False
                    if group["check_path"] is None:
                        if check_path is not None:
                            group["check_path"] = check_path
                            has_check_path = True
                    else:
                        if check_path is not None and len(check_path) > len(group["check_path"]):
                            group["check_path"] = check_path
                            has_check_path = True
                    if not has_check_path:
                        # 获取较后面时间的 ready_time
                        if ready_time is not None and group["last_ready_time"] is not None:
                            if TaskUtil._compare_cron(ready_time, group["last_ready_time"]):
                                group["last_ready_time"] = ready_time
                    is_grouped = True
                    break
            if not is_grouped:
                groups.append({"last_ready_time": ready_time,
                               "check_path": check_path,
                               "ddr_list": [dataset]})
        for group in groups:
            for ddr in group["ddr_list"]:
                # TODO
                if ddr.check_path is None or len(ddr.check_path.strip()) == 0:
                    ddr.check_path = group["check_path"]
                ddr.ready_time = group["last_ready_time"]
                if ddr.metadata_id not in res_dict.keys():
                    res_dict[ddr.metadata_id] = [ddr]
                else:
                    res_dict[ddr.metadata_id].append(ddr)
                res.append(ddr)

        return (res,res_dict)

    @staticmethod
    def _compare_cron(cron_expression1, cron_expression2):
        str_time_now = datetime(year=2021, month=1, day=1)
        cron1 = croniter(cron_expression1, str_time_now)
        cron2 = croniter(cron_expression2, str_time_now)
        aa = cron1.get_prev(datetime)
        bb = cron2.get_prev(datetime)
        return aa > bb

    @staticmethod
    def render_datatime(content, execution_date, dag_id,date_calculation_param,gra):
        if not isinstance(content, six.string_types):
            raise ValueError("Content should be string_types! ")
        res = []
        context = {}
        from airflow import macros

        data_dates = DateCalculationUtil.get_upstream_all_data_dates(execution_date=execution_date,calculation_param=date_calculation_param,gra=gra)
        if "[[" in content and "]]" in content:
            for d_date in data_dates:
                res_conten_list = []
                head = content.split("[[")
                res_conten_list.append(head[0])
                for i in range(1, len(head)):
                    pu = head[i].split("]]")
                    res_conten_list.append(d_date.strftime(pu[0]))
                    res_conten_list.append(pu[1])
                res.append({"path": "".join(res_conten_list),
                            "date": d_date})
        else:
            for d_date in data_dates:
                res.append({"path": content,
                            "date": d_date})

        context["execution_date"] = execution_date
        context["macros"] = macros
        context = TaskUtil.get_template_context(dag_id=dag_id, execution_date=execution_date, context=context)
        jinja_env = jinja2.Environment(cache_size=0)
        for i in range(0, len(res)):
            res[i]["path"] = jinja_env.from_string(res[i]["path"]).render(**context)
        return res

    @staticmethod
    def render_datatime_one_path(content,execution_date,data_date,crontab):
        if not isinstance(content, six.string_types):
            raise ValueError("Content should be string_types! ")
        res = ''
        context = {}
        from airflow import macros
        if "[[" in content and "]]" in content:
            res_conten_list = []
            head = content.split("[[")
            res_conten_list.append(head[0])
            for i in range(1, len(head)):
                pu = head[i].split("]]")
                res_conten_list.append(data_date.strftime(pu[0]))
                res_conten_list.append(pu[1])
            res = "".join(res_conten_list)
        else:
            res = content

        context["execution_date"] = execution_date
        context["macros"] = macros
        context = TaskUtil.get_template_context_by_crontab(crontab=crontab, execution_date=execution_date, context=context)
        jinja_env = jinja2.Environment(cache_size=0)
        for i in range(0, len(res)):
            res = jinja_env.from_string(res).render(**context)
        return res


    @staticmethod
    def is_new_release_task(name, execution_date):
        '''
       判断某个任务是不是首次上线的任务,或者是下线一段时间后再次上线的任务
       '''
        task = TaskDesc.get_task(task_name=name)
        if task.create_date.strftime('%Y-%m-%d %H:%M:%S') >= execution_date.strftime('%Y-%m-%d %H:%M:%S'):
            return True

        # todo 目前没办法精确判断任务是否为下线一段时间后再次上线的任务 怎么搞呢？
        return False

    @staticmethod
    def get_template_context(dag_id, execution_date, context):
        from airflow.shareit.utils.task_cron_util import TaskCronUtil

        ds = execution_date.strftime('%Y-%m-%d')
        ts = execution_date.isoformat()
        yesterday_ds = (execution_date - timedelta(1)).strftime('%Y-%m-%d')
        tomorrow_ds = (execution_date + timedelta(1)).strftime('%Y-%m-%d')
        lastmonth_date = DateCalculationUtil.datetime_month_caculation(execution_date, -1).strftime('%Y-%m')

        prev_execution_date = TaskCronUtil.get_pre_execution_date(dag_id, execution_date)
        prev_2_execution_date = TaskCronUtil.get_pre_execution_date(dag_id, prev_execution_date)
        next_execution_date = TaskCronUtil.get_next_execution_date(dag_id, execution_date)

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

        context['ds'] = ds
        context['next_ds'] = next_ds
        context['next_ds_nodash'] = next_ds_nodash
        context['prev_ds'] = prev_ds
        context['prev_ds_nodash'] = prev_ds_nodash
        context['ds_nodash'] = ds_nodash
        context['ts'] = ts
        context['ts_nodash'] = ts_nodash
        context['ts_nodash_with_tz'] = ts_nodash_with_tz
        context['yesterday_ds'] = yesterday_ds
        context['yesterday_ds_nodash'] = yesterday_ds_nodash
        context['tomorrow_ds'] = tomorrow_ds
        context['tomorrow_ds_nodash'] = tomorrow_ds_nodash
        context['lastmonth_date'] = lastmonth_date
        context['lastmonth_date_ds_nodash'] = lastmonth_date_ds_nodash
        context['execution_date'] = pendulum.instance(execution_date)
        context['prev_execution_date'] = prev_execution_date
        context['prev_2_execution_date'] = prev_2_execution_date
        context['next_execution_date'] = next_execution_date
        context = TaskUtil.set_utc0_template_context(context)
        return context

    @staticmethod
    def get_template_context_by_crontab(crontab, execution_date, context):
        from airflow.shareit.utils.task_cron_util import TaskCronUtil

        ds = execution_date.strftime('%Y-%m-%d')
        ts = execution_date.isoformat()
        yesterday_ds = (execution_date - timedelta(1)).strftime('%Y-%m-%d')
        tomorrow_ds = (execution_date + timedelta(1)).strftime('%Y-%m-%d')

        prev_execution_date = TaskCronUtil.get_pre_execution_date_by_crontab(crontab, execution_date)
        prev_2_execution_date = TaskCronUtil.get_pre_execution_date_by_crontab(crontab, prev_execution_date)
        next_execution_date = TaskCronUtil.get_pre_execution_date_by_crontab(crontab, execution_date)

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

        context['ds'] = ds
        context['next_ds'] = next_ds
        context['next_ds_nodash'] = next_ds_nodash
        context['prev_ds'] = prev_ds
        context['prev_ds_nodash'] = prev_ds_nodash
        context['ds_nodash'] = ds_nodash
        context['ts'] = ts
        context['ts_nodash'] = ts_nodash
        context['ts_nodash_with_tz'] = ts_nodash_with_tz
        context['yesterday_ds'] = yesterday_ds
        context['yesterday_ds_nodash'] = yesterday_ds_nodash
        context['tomorrow_ds'] = tomorrow_ds
        context['tomorrow_ds_nodash'] = tomorrow_ds_nodash
        context['execution_date'] = pendulum.instance(execution_date)
        context['prev_execution_date'] = prev_execution_date
        context['prev_2_execution_date'] = prev_2_execution_date
        context['next_execution_date'] = next_execution_date
        context = TaskUtil.set_utc0_template_context(context)
        return context

    @staticmethod
    def set_utc0_template_context(context):
        execution_date = context['execution_date'] - timedelta(hours=8)
        ds = execution_date.strftime('%Y-%m-%d')
        ts = execution_date.isoformat()
        yesterday_ds = (execution_date - timedelta(1)).strftime('%Y-%m-%d')
        tomorrow_ds = (execution_date + timedelta(1)).strftime('%Y-%m-%d')
        lastmonth_date = DateCalculationUtil.datetime_month_caculation(execution_date, -1).strftime('%Y-%m')

        prev_execution_date = context['prev_execution_date'] - timedelta(hours=8)
        prev_2_execution_date = context['prev_2_execution_date'] - timedelta(hours=8)
        next_execution_date = context['next_execution_date'] - timedelta(hours=8)

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
        lastmonth_date_ds_nodash = lastmonth_date.replace('-', '')

        context['ds_utc0'] = ds
        context['next_ds_utc0'] = next_ds
        context['next_ds_nodash_utc0'] = next_ds_nodash
        context['prev_ds_utc0'] = prev_ds
        context['prev_ds_nodash_utc0'] = prev_ds_nodash
        context['ds_nodash_utc0'] = ds_nodash
        context['ts_utc0'] = ts
        context['ts_nodash_utc0'] = ts_nodash
        context['ts_nodash_with_tz_utc0'] = ts_nodash_with_tz
        context['yesterday_ds_utc0'] = yesterday_ds
        context['yesterday_ds_nodash_utc0'] = yesterday_ds_nodash
        context['tomorrow_ds_utc0'] = tomorrow_ds
        context['tomorrow_ds_nodash_utc0'] = tomorrow_ds_nodash
        context['lastmonth_date_utc0'] = lastmonth_date
        context['lastmonth_date_ds_nodash_utc0'] = lastmonth_date_ds_nodash
        context['execution_date_utc0'] = pendulum.instance(execution_date)
        context['prev_execution_date_utc0'] = prev_execution_date
        context['prev_2_execution_date_utc0'] = prev_2_execution_date
        context['next_execution_date_utc0'] = next_execution_date
        return context