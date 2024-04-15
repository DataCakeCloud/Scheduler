# -*- coding: utf-8 -*-
"""
author: zhangtao
Copyright (c) 2021/5/24 Shareit.com Co., Ltd. All Rights Reserved.
"""
import time
import traceback

import unittest
from datetime import datetime, timedelta

from airflow import LoggingMixin
from airflow.shareit.models.dataset_partation_status import DatasetPartitionStatus
from airflow.shareit.scheduler.external_helper import ExternalManager
from airflow.shareit.service.dataset_service import DatasetService
from airflow.shareit.utils.state import State
from airflow.utils.timezone import utcnow
import pendulum

from airflow.shareit.models.dag_dataset_relation import DagDatasetRelation
from airflow.shareit.service.task_service import TaskService
from airflow.shareit.utils.granularity import Granularity
from airflow.shareit.utils.task_util import TaskUtil


class TaskUtilTest(unittest.TestCase):

    def test_generate_extra_dataset(self):
        # from airflow.shareit.models.dag_dataset_relation import DagDatasetRelation
        # res = DagDatasetRelation.get_all_outer_dataset()
        # print (res)
        # from airflow.shareit.utils.task_util import TaskUtil
        # tu = TaskUtil()
        # tu.external_data_processing()
        pass

    def test_merger_dataset_check_info(self):
        ddrs = DagDatasetRelation.get_all_outer_dataset()
        res = TaskUtil._merger_dataset_check_info(datasets=ddrs)
        print (len(res))

    def test_render_datatime(self):
        case1 = {
            "content": 's3://test/path/XXXXXX/{{ (execution_date + macros.timedelta(hours=8)).strftime("%Y%m%d") }}/SUCCESS',
            "execution_date": datetime(year=2021, month=9, day=15, hour=0, minute=0, second=0),
            "depend_gra": "monthly",
            "offset": -1,
            "detailed_dependency": "1,3,5,L",
            "detailed_gra": "daily",
        }
        res1 = TaskUtil.render_datatime(case1["content"], case1["execution_date"],
                                         case1["depend_gra"], case1["offset"],
                                         case1["detailed_dependency"], case1["detailed_gra"])
        self.assertEqual(len(res1), 1)

        case2 = {
            "content": 's3://test/path/XXXXXX/[[%Y-%m-%d]]/SUCCESS',
            "execution_date": datetime(year=2021, month=9, day=15, hour=0, minute=0, second=0),
            "depend_gra": "monthly",
            "offset": -2,
            "detailed_dependency": None,  # "1,3,5,L" "1,3,5,ALL"
            "detailed_gra":None ,# "daily"
        }
        res2 = TaskUtil.render_datatime(case2["content"], case2["execution_date"],
                                         case2["depend_gra"], case2["offset"],
                                         case2["detailed_dependency"], case2["detailed_gra"])
        self.assertEqual(len(res2), 1)

    def test_backfill(self):
        # start_date = pendulum.parse("2022-02-13 00:00:00")
        # end_date = pendulum.parse("2022-02-15 00:00:00")

        # start_date = None
        end_date = None

        # TaskService.backfill(["flink-batch-demo2"], start_date, end_date, False, False)
        # date = Granularity.get_latest_execution_date(utcnow(), 'monthly')
        # print  date
        info = TaskService.get_specified_task_instance_info(name="1_23996",page=1, size=7)
        print info

    def test_set_success(self):
        # TaskService.set_failed("test_clear_trigger",
        #                        "2022-01-11 00:00:00,2022-01-12 00:00:00,2022-01-13 00:00:00,2022-01-14 00:00:00")

        name = '32_58005'
        execution_date = "2024-03-27 00:00:00"

        if name and execution_date:
            execution_dates = execution_date.split(',')
            for dt in execution_dates:
                dt = pendulum.parse(dt)
                TaskService.set_success(dag_id=name, execution_date=dt)

    def test_render(self):
        import six
        import json
        from airflow import macros
        execution_date = pendulum.parse('2022-08-22 00:00:00')
        content='''
add jar  s3://shareit.deploy.us-east-1/BDP/BDP-lbs/lbs-udf/test/ads_base-1.0.0.jar;
create temporary function ipnationudf as 'com.ushareit.ads.base.hive.udf.GetNationFromIPUDF';
insert overwrite table bd_dws.dws_beyla_device_active_inc_daily_back
partition (
    dt= '{{ (execution_date - macros.timedelta(hours=696)).strftime("%Y%m%d") }}',
        -- dt= '99990101',

    app_token
)
select t1.beyla_id,t1.nation,t1.os_name,t1.os_ver,
t1.device_band,t1.device_model,t1.app_ver_code,t1.resolution,t1.time_zone,
if(t2.beyla_id is not null, datediff('{{ (execution_date - macros.timedelta(hours=696)).strftime("%Y-%m-%d") }}', concat(substr(t2.first_dt, 1, 4), '-', substr(t2.first_dt,5,2),'-',substr(t2.first_dt,7,2))), 0) as day_first,
if(t2.beyla_id is not null, datediff('{{ (execution_date - macros.timedelta(hours=696)).strftime("%Y-%m-%d") }}', concat(substr(t2.active_dt, 1, 4), '-', substr(t2.active_dt, 5, 2), '-', substr(t2.active_dt, 7, 2))), 0) as day_before,
if(t2.beyla_id is null, 'new', 'active') as user_type,
if(t2.beyla_id is null, 
   array(null,case when t3.beyla_id is null then 'none_af_mes' 
                   when t3.beyla_id is not null and media_source is null then 'organic' 
                   else media_source end, 
        af_channel, campaign, adset, adgroup, campaign_id),
   t2.channel_install) as channel_install,if(t4.up_times is not null, t4.up_times, 0),t1.gaid,
  t1.province,
  t1.city,
  t1.country,
  t1.language,
  case  when t5.region_en is null then 'OTHER' else  t5.region_en end,
  case  when t5.region_cn is null then '其他所有国家' else t5.region_cn end,
  t5.country_cn,
  t1.event_network,
t1.app_token from ( 
    SELECT beyla_id,gaid, nation, os_name, os_ver
       ,device_band, device_model, app_ver_code, resolution, time_zone, app_token, 
          province,
          city,
          country,
          language,
          event_network
FROM (
    SELECT case when (app_token='PLAYIT_A' or app_token='SHAREIT_W') then user_id else beyla_id end as beyla_id, 
         first_value(case when (gaid='unknown' or gaid='') then null else gaid end, true) over(partition by case when (app_token='PLAYIT_A' or app_token='SHAREIT_W') then user_id else beyla_id end) as gaid,
         ipnationudf(ip) nation, os_name, os_ver
        , lower(manufacture) AS device_band, device_model, app_ver_code
        , resolution, time_zone, app_token, event_time,
          province,
          city,
          country,
          language,
          event_network,
          row_number() OVER (PARTITION BY case when (app_token='PLAYIT_A' or app_token='SHAREIT_W') then user_id else beyla_id end, app_token ORDER BY event_time desc) AS rn
    FROM iceberg.bd_dwd.beyla_cmd_events
    WHERE server_date_ymd between '{{ (execution_date - macros.timedelta(hours=696)).strftime("%Y%m%d") }}' and '{{ (execution_date - macros.timedelta(days=1)).strftime("%Y%m%d") }}'
        AND client_date_ymd = '{{ (execution_date - macros.timedelta(hours=696)).strftime("%Y%m%d") }}'
        AND (   
               (    
                    ((event_name IN ('in_page', 'out_page', 'UF_PortalInfo')) or (event_type IN ('1','2')))
                    and app_token not in ('SHAREIT_A', 'SHAREIT_I', 'SHAREK','SLITE_A','FUNU', 'LAKI', 'WATCHIT', 'LIKEIT', 'BUZZNEWS', 'STAR_LIVE', 'LIKEIT_L', 'GAMESHARE_A', 'MAXPLAYER_A', 'PLAYIT_A', 'SHAREIT_W', 'GAMESHARE_B', 'MAXFILE_A','WALL_PAP','NEWS_BEES')
                    AND beyla_id IS NOT NULL
                    AND beyla_id != ''
               )
                
            or
               (
                    (
                        event_type in ('1','2')
                        or event_name in ('Content_Show', 'Video_IjkPlayerResultSimple', 'Video_YtbPlayerResultSimple', 'Video_ExoPlayerResultSimple', 'Video_YuppPlayerResultSimple')
                        or (event_name = 'CMD_ReportCompleted' and event_params['cmd_id'] like 'cmd_inf%push%' and event_params['cmd_id'] not like 'test%')
                        or (event_name in ('UF_ScreenCardClick','Music_WidgetAction')) 
                        or (event_name = 'Content_Click' and event_params['pve_cur'] like '/ScreenLock%')
                    )
                    and app_token in ('FUNU', 'LAKI', 'WATCHIT', 'LIKEIT')
                    AND beyla_id IS NOT NULL
                    AND beyla_id != ''
                )
            or
                (
                    app_token ='GAMESHARE_A'
                    and (event_name = 'UF_PortalInfo'
                         or (event_name='game_common_event'
                                and  event_params['type'] <> 'push'
                                and  event_params['content_type'] ='new_make_money'
                                and  event_params['event'] ='event_show' 
                             )
                        )
                    and  event_network in('WIFI_CONNECT','WIFI','MOBILE_3G_CONNECT','MOBILE_3G','MOBILE_4G_CONNECT','MOBILE_4G')
                    AND beyla_id IS NOT NULL
                    AND beyla_id != ''
                )
            or
                (
                    app_token ='GAMESHARE_B'
                    and event_name='game_common_event'
                    and event_params['type'] <> 'push'
                    and event_params['content_type'] ='new_make_money'
                    and event_params['event'] ='event_show' 
                    and event_network not like '%OFFLINE'
                    AND beyla_id IS NOT NULL
                    AND beyla_id != ''
                )
            or
                (
                    app_token in ('BUZZNEWS', 'STAR_LIVE', 'LIKEIT_L', 'MAXPLAYER_A', 'MAXFILE_A')
                    and event_name in ('UF_PortalInfo', 'in_page', 'out_page', 'launch')
                    AND beyla_id IS NOT NULL
                    AND beyla_id != ''
                )
            or
              (
            app_token in ('SHAREIT_I', 'SHAREK', 'SLITE_A','SHAREIT_A') 
          AND (event_group='page' 
     or (event_name IN ('in_page', 'out_page', 'UF_PortalInfo')) 
     or (event_type IN ('1', '2')) 
     or event_name='Music_WidgetAction' 
     or event_name='UF_ScreenCardClick' 
     or ( event_name='CMD_ReportCompleted' AND substr(event_params['cmd_id'], 1, 11) in('cmd_inf_man', 'cmd_inf_rec') 
       and (
                    event_params ['cmd_id'] != 'cmd_inf_man_vi_pushtest2_20181107_ta'
                    and event_params ['cmd_id'] != 'cmd_inf_man_vi_pushtest1_20181107_ta'
                    and event_params ['cmd_id'] != 'cmd_inf_man_vi_pushtest2_20181107_ta'
                    and event_params ['cmd_id'] != 'cmd_inf_man_vi_pushtest2_20181107_te'
                    and event_params ['cmd_id'] != 'cmd_inf_man_vi_pushtest1_20181107_te'
                    and event_params ['cmd_id'] != 'cmd_inf_man_vi_pushtest1_20181107_mh'
                    and event_params ['cmd_id'] != 'cmd_inf_man_vi_pushtest2_20181107_mh'
                    and event_params ['cmd_id'] != 'cmd_inf_man_vi_pushtest2_20181107_hi'
                    and event_params ['cmd_id'] != 'cmd_inf_man_vi_pushtest1_20181107_hi'
                    and event_params ['cmd_id'] != 'cmd_inf_man_vi_push_20190426_none_hi'
                    and event_params ['cmd_id'] != 'cmd_inf_man_vi_push_20190426_onlyone_hi'
                  )
                  
                )
                or (
                  event_name = 'Content_Click'
                  and event_params ['pve_cur'] like '/ScreenLock%'
                )
                or  
                 (  
                   event_name = 'Local_UnreadNotifyClick'  
                   and event_params['portal_from'] = 'push_local_tool'  
                 )  
              )
              AND beyla_id IS NOT NULL
              AND beyla_id != ''
            )
            or(   app_token = 'PLAYIT_A' 
                  AND ((event_name IN ('in_page', 'out_page', 'UF_PortalInfo')) or (event_type IN ('1','2')))
                  AND user_id IS NOT NULL
                  AND user_id != '' 
              ) 
            or(
                  app_token = 'SHAREIT_W'
                  AND user_id IS NOT NULL
                  AND user_id != '' 
              ) 
              or(
                  app_token IN ('WALL_PAP','NEWS_BEES')
                  AND event_name IN ('in_page', 'page_in')
              )  
            )   
        AND app_token IS NOT NULL
        AND app_token != ''
) a
WHERE rn = 1) t1
left join (select beyla_id, app_token, first_dt, before_dt, active_dt,channel_install 
 from bd_dws.dws_beyla_device_active_all_daily_back
    where dt = '{{ (execution_date - macros.timedelta(hours=720)).strftime("%Y%m%d") }}') t2
on t1.beyla_id = t2.beyla_id and t1.app_token = t2.app_token 
left join (
     select
        app_token
        ,beyla_id
        ,media_source
        ,af_channel
        ,campaign
        ,campaign_id
        ,adgroup
        ,adset
    from
     (select 
        app_token
        ,beyla_id
        ,event_params['media_source'] as media_source
        ,event_params['af_channel'] as af_channel
        ,event_params['campaign'] as campaign 
        ,get_json_object(event_params['extras'],'$.campaign_id') as campaign_id
        ,get_json_object(event_params['extras'],'$.adgroup') as adgroup 
        ,get_json_object(event_params['extras'],'$.adset') as adset 
        ,row_number() over (partition by app_token,beyla_id order by event_time asc) as row_index      
        from iceberg.bd_dwd.beyla_cmd_events      
        where server_date_ymd between '{{ (execution_date - macros.timedelta(hours=696)).strftime("%Y%m%d") }}' and '{{ (execution_date - macros.timedelta(days=1)).strftime("%Y%m%d") }}'
        AND client_date_ymd = '{{ (execution_date - macros.timedelta(hours=696)).strftime("%Y%m%d") }}'
        and event_name = 'AppsFlyer_Launch'
        and event_params['is_first_launch'] = 'true') a 
    where row_index = 1
) t3
on t1.beyla_id = t3.beyla_id and t1.app_token = t3.app_token
left join (select beyla_id,app_token, count(beyla_id) as up_times
from iceberg.bd_dwd.beyla_cmd_events      
where  server_date_ymd between '{{ (execution_date - macros.timedelta(hours=696)).strftime("%Y%m%d") }}' and '{{ (execution_date - macros.timedelta(days=1)).strftime("%Y%m%d") }}'
        AND client_date_ymd = '{{ (execution_date - macros.timedelta(hours=696)).strftime("%Y%m%d") }}'
    and event_name = 'UF_PortalInfo'
group by beyla_id,app_token) t4
on t1.beyla_id = t4.beyla_id and t1.app_token = t4.app_token
  left join (
      select 
        region_en,
        region_cn,
        country_cn,
        country_en
      from  qz_cdm.dim_country_region
  )t5 on t1.nation = t5.country_en ;
        '''
        context={}
        context["execution_date"] = execution_date
        context["macros"] = macros
        res = TaskService.render_api(content=content, context=context,task_name='dws_beyla_device_active_inc_daily_back')
        print res

    def test_clear_instance(self):
        name='test_external'
        is_check_upstream=False
        execution_date="2022-11-06 21:00:00"

        parse_dates = []
        execution_dates = execution_date.split(',')
        for dt in execution_dates:
            dt = pendulum.parse(dt)
            parse_dates.append(dt)

        TaskService.clear_instance(dag_id=name, execution_dates=parse_dates,
                                           is_check_upstream=is_check_upstream)

    def test_get_dataset_info(self):
        info = DatasetService.get_dataset_info("test.test_time_scheduler_minute_0@ue1")
        print info

    def test_(self):
        # log = LoggingMixin().logger
        # for i in range(5):
        #     try:
        #         raise Exception("abasda")
        #     except Exception as e:
        #         s = traceback.format_exc()
        #         log.error("check dependecy eror!")
        #         log.error(s)
        #         print i
        #         continue
        #         print 1111111
        pass_date = [State.SUCCESS, State.BACKFILLDP]
        a = datetime(2022,1,2,3)
        res = DatasetPartitionStatus.check_dataset_status_single_date(metadata_id='aaa',
                                                                      dataset_partition_sign=a , state=pass_date)
        if res is None or len(res) == 0:
            print False

    def test_11(self):
        a_dict = {}
        a_dict['1']=[1]
        a_dict['1'].append(2)
        a_dict['2']=[2]
        print a_dict['1']
        print hash('aaa')
        print hash('bbb')
        for key, value in a_dict.iteritems():
            print key
            print value

    def test_divide_datasets(self):
        a_dict = {}
        for i in range(1,100):
            a_dict[i] = [i]
        result = ExternalManager.divide_datasets(a_dict, 7)
        for i in  result:
            print i

    def test_external(self):
        ExternalManager().main_scheduler_process(7)

    def test_mark_success(self):
        dag_id = "test_check_path_2"
        dt = pendulum.parse("2022/12/06 03:00:00.000")
        TaskService.set_success(dag_id=dag_id, execution_date=dt)
