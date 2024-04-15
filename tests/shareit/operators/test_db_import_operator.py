# -*- coding: utf-8 -*-
"""
author: zhangtao
Copyright (c) 2020/12/21 Shareit.com Co., Ltd. All Rights Reserved.
"""

import unittest
from datetime import datetime, timedelta

from airflow.shareit.operators.db_import_operator import DBImportOperator
from airflow.utils import timezone

DEFAULT_DATE = datetime(2016, 1, 1, tzinfo=timezone.utc)
END_DATE = datetime(2016, 1, 2, tzinfo=timezone.utc)
INTERVAL = timedelta(hours=12)


class DBImportOperatorTest(unittest.TestCase):

    def test_tasks(self):
        commands = """--deploy-mode cluster --conf spark.kubernetes.driver.volumes.configMap.jdbc-configmap.mount.path=/mnt/configmap/jdbc/ --conf spark.kubernetes.driver.volumes.configMap.jdbc-configmap.options.configMapName=aws-bdp1-do-dwd-ch --name DBImporter --class com.ushareit.data.etl.jdbc.DBImporter db-etl-1.1.0-SNAPSHOT.jar --table dm_ads_funnel_revenue --input s3://da.results.prod.us-east-1/caowei/monetization/dm_ads_funnel_revenue/DWA_result/dt=20201216 --inputtype parquet --batchsize 10000 --delete "set_source" --sql "select c_dt ,s_dt ,if(nation is null or nation = '', 'other', nation) as nation ,app_token ,network ,pid ,ad_id ,creative_id ,source ,pkg_name ,if(app_ver_code is null or app_ver_code = '', -333, app_ver_code) as app_ver_code ,inv ,load_suc ,show ,click ,adshow ,adclick ,lp_show ,lp_click ,dl_click ,dl_add_list ,dl_start ,dl_suc ,install ,re_installs ,re_revenue ,'2020-12-16' as dt from analyst.dm_ads_funnel_revenue where dt = '20201216'" --preSql "alter table dm_ads_funnel_revenue drop partition '2020-12-16'" --header true --configFile /mnt/configmap/jdbc/config.properties"""
        test_task = DBImportOperator(
            task_id='dwd_log_live_sc_event_inc_daily',
            genie_conn_id='da_genie_prod_default',
            cluster_tags='region:us-east-1,provider:aws,sla:normal,type:k8s',
            table='dm_ads_funnel_revenue',
            input_path='s3://da.results.prod.us-east-1/caowei/monetization/dm_ads_funnel_revenue/DWA_result/dt=20201216',
            input_type='parquet',
            batchsize=10000,
            driver='',
            truncate='',
            add_columns="""""",
            delete_columns='set_source',
            query_sql="""select c_dt ,s_dt ,if(nation is null or nation = "", 'other', nation) as nation ,app_token ,network ,pid ,ad_id ,creative_id ,source ,pkg_name ,if(app_ver_code is null or app_ver_code = '', -333, app_ver_code) as app_ver_code ,inv ,load_suc ,show ,click ,adshow ,adclick ,lp_show ,lp_click ,dl_click ,dl_add_list ,dl_start ,dl_suc ,install ,re_installs ,re_revenue ,'2020-12-16' as dt from analyst.dm_ads_funnel_revenue where dt = '20201216'""",
            configmap='aws-bdp1-do-dwd-ch',
            configmap_path='/mnt/configmap/jdbc/',
            configmap_file='config.properties',
            spark_conf='',
            preSql="""alter table dm_ads_funnel_revenue drop partition '2020-12-16'""",
            postSql="""""",
            header='true',
            schema=''
        )

        print test_task.command
        print test_task.genie_conn_id
