# -*- coding: utf-8 -*-
"""
author: zhangtao
Copyright (c) 2021/5/24 Shareit.com Co., Ltd. All Rights Reserved.
"""
import time

from airflow.shareit.utils.task_manager import analysis_task, analysis_dag, format_dag_params
import unittest
from datetime import datetime, timedelta
from airflow.shareit.utils.granularity import Granularity


class GranularityTest(unittest.TestCase):

    def test_format(self):
        aa = Granularity.formatGranularity("6")
        bb = Granularity.formatGranularity("hourly")
        cc = Granularity.formatGranularity("HOURLY")
        dd = Granularity.formatGranularity(6)
        self.assertEqual(aa, bb)
        self.assertEqual(aa, cc)
        self.assertEqual(aa, dd)

    def test_getTimeRange(self):
        td = datetime(year=2022, month=5, day=12, hour=0, minute=0, second=0)
        ytd = datetime(year=2022, month=5, day=12, hour=3, minute=0, second=0)
        sd, ed = Granularity.getTimeRange(baseDate=td, gra=7, offset=-1)
        print (sd, ed)
        # self.assertEqual(sd, ytd)
        # self.assertEqual(ed, td)

    def test_splitTimeByGranularity(self):
        td = datetime(year=2022, month=5, day=12, hour=0, minute=0, second=0)
        ytd = datetime(year=2022, month=5, day=12, hour=3, minute=0, second=0)
        res = Granularity.splitTimeByGranularity(td,ytd, 7)
        print (res)

    """
        def get_detail_time_range(base_gra=None, depend_gra=None, detailed_gra=None, detailed_dependency=None,
                              base_date=None):
        
        base_gra int 数据产出粒度,
        detailed_gra int 详细依赖的粒度,
        depend_gra int 依赖的粒度,
        detailed_dependency string or list 依赖的具体时间,
        base_data datatime 基础时间
        gra_priority_map = {
        "hourly": 6,
        "daily": 5,
        "weekly": 4,
        "monthly": 3,
        "yearly": 2,
        "other": 1
    }
    """

    def test_get_detail_time_range(self):
        base_gra = 5
        depend_gra = 3
        detailed_gra = 4
        st,et = Granularity.getTimeRange(baseDate=datetime(year=2021, month=9, day=15, hour=0, minute=0, second=0), gra=depend_gra, offset=-1)
        res = Granularity.get_detail_time_range(base_gra=base_gra,
                                                depend_gra=depend_gra,
                                                detailed_gra=detailed_gra,
                                                detailed_dependency="1,L,L-1",
                                                base_date=st)
        print (len(res))
        print(res)

    def test_get_prev_next_date(self):
        td = datetime(year=2022, month=5, day=12, hour=0, minute=0, second=0)
        print Granularity.getNextTime(td, 7, -1)
        print Granularity.getPreTime(td, 7, -1)
