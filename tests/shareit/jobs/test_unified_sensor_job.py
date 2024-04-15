# -*- coding: utf-8 -*-
"""
Author: Tao Zhang
Date: 2022/4/21
If you are doing your best,you will not have to worry about failure.
"""
import unittest
from airflow.shareit.jobs.unified_sensor_job import UnifiedSensorJob


class SensorJobTest(unittest.TestCase):

    def test_check_poke_by_sdk(self):
        d = "https://bdp-noah-prod-sg2.obs.ap-southeast-3.myhuaweicloud.com/video/62318033e4b0c252174c7d0b.mp4"
        e = "https://bdp-druid-test.obs.ap-southeast-3.myhuaweicloud.com:443/yuanhui.tar.gz?AWSAccessKeyId=8jhN9nc2kYKhu6GMSBPG&Expires=1647575096&response-content-disposition=inline&Signature=8WNTC6ClDtZ1tuqn4eXo%2B02CNx0%3D"
        f = "obs://bdp-druid-test/sdasf/sdfas.tt"
        # g = "s3://ecom-common-sg1/forever/common/dw/table/ecom_dwm/dwm_ads_user_app_action_all/dt=20221027/hour=23/_SUCCESS"
        # g = "s3://shareit-algo-data-ue1/push/user_active_new/app=shareit/dt=20221027/_SUCCESS"
        # f = "https://www.googleapis.com/storage/v1/b/bdp-ds-test-sg3/o/hebe/dev/jars/4200037824cf4349bbab6547bf5d0cb8/spark-submit-sql-1.0-SNAPSHOT.jar"
        f = "gs://bdp-poc-test/pipeline/112.txt"
        usj = UnifiedSensorJob()
        res = usj.check_poke_by_sdk(check_path=f)
        print (res)

    def test_send_check_failed_notify(self):
        usj = UnifiedSensorJob()
        usj.send_check_failed_notify(check_path="gshgrshrtf",dag_id="test_zhangtao",execution_date="2022-01-02 00:00:00")
        print ("success")

    def test_check_obs_file_or_dir(self):
        path = 'obs://shareit-algo-data-sg/push/activity_query_testp9p10/app=shareit/http_request_new/dt=20220914/_SUCCESS'
        print UnifiedSensorJob().check_obs_file_or_dir(path)