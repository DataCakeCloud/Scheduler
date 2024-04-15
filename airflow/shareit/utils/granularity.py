# -*- coding: utf-8 -*-
"""
author: zhangtao
Copyright (c) 2021/5/12 Shareit.com Co., Ltd. All Rights Reserved.
"""
from datetime import datetime, timedelta

import six

from airflow.utils.timezone import beijing


class Granularity:
    granularity_enum = ["minutely","hourly", "daily", "weekly", "monthly", "yearly", "other"]
    gra_priority_map = {
        "minutely": 7,
        "hourly": 6,
        "daily": 5,
        "weekly": 4,
        "monthly": 3,
        "yearly": 2,
        "other": 1
    }
    priority_gra_map = {
        7: "minutely",
        6: "hourly",
        5: "daily",
        4: "weekly",
        3: "monthly",
        2: "yearly",
        1: "other"
    }

    def __init__(self):
        pass

    @staticmethod
    def getIntGra(gra):
        if isinstance(gra, str) or isinstance(gra, unicode):
            _gra = gra.lower()
            if _gra in Granularity.granularity_enum:
                return Granularity.gra_priority_map[_gra]
        try:
            gra = int(gra)
            if gra in Granularity.priority_gra_map:
                return gra
        except Exception:
            pass
        return False

    @staticmethod
    def formatGranularity(gra):
        if isinstance(gra, str) or isinstance(gra, unicode):
            _gra = gra.lower()
            if _gra in Granularity.granularity_enum:
                return _gra
        try:
            gra = int(gra)
        except Exception:
            pass
        if isinstance(gra, int):
            if gra in Granularity.priority_gra_map.keys():
                return Granularity.priority_gra_map[gra]
        return False

    @staticmethod
    def getNextTime(baseDate, gra, offset):
        # calculate upstream to downstream
        if offset > 0:
            return Granularity.getPreTime(baseDate, gra, -offset)
        else:
            offset = -offset
        if not isinstance(baseDate, datetime):
            return
        gra = Granularity.formatGranularity(gra)
        if gra == "other":
            pass
        elif gra == "minutely":
            tmp = baseDate + timedelta(minutes=offset)
            return datetime(year=tmp.year, month=tmp.month, day=tmp.day, hour=tmp.hour, minute=tmp.minute,
                            tzinfo=tmp.tzinfo)
        elif gra == "hourly":
            tmp = baseDate + timedelta(hours=offset)
            return datetime(year=tmp.year, month=tmp.month, day=tmp.day, hour=tmp.hour, tzinfo=tmp.tzinfo)
        elif gra == "daily":
            tmp = baseDate + timedelta(days=offset)
            return datetime(year=tmp.year, month=tmp.month, day=tmp.day, tzinfo=tmp.tzinfo)
        elif gra == "weekly":
            tmp = baseDate + timedelta(weeks=offset)
            return datetime(year=tmp.year, month=tmp.month, day=tmp.day, tzinfo=tmp.tzinfo)
        elif gra == "monthly":
            year = baseDate.year
            month = baseDate.month + offset
            if month >= 13:
                year += month // 12
                month = month % 12
            if month == 0:
                month = 12
                year -= 1
            return datetime(year=year, month=month, day=1, tzinfo=baseDate.tzinfo)
        elif gra == "yearly":
            year = baseDate.year + offset
            return datetime(year=year, month=1, day=1, tzinfo=baseDate.tzinfo)

    @staticmethod
    def getPreTime(baseDate, gra, offset):
        # calculate downstream to upstream
        if offset > 0:
            return Granularity.getNextTime(baseDate, gra, -offset)
        else:
            offset = -offset
        if not isinstance(baseDate, datetime):
            return
        gra = Granularity.formatGranularity(gra)
        if gra == "other":
            pass
        elif gra == "minutely":
            tmp = baseDate - timedelta(minutes=offset)
            return datetime(year=tmp.year, month=tmp.month, day=tmp.day, hour=tmp.hour, minute=tmp.minute,
                            tzinfo=tmp.tzinfo)
        elif gra == "hourly":
            tmp = baseDate - timedelta(hours=offset)
            return datetime(year=tmp.year, month=tmp.month, day=tmp.day, hour=tmp.hour, tzinfo=tmp.tzinfo)
        elif gra == "daily":
            tmp = baseDate - timedelta(days=offset)
            return datetime(year=tmp.year, month=tmp.month, day=tmp.day, tzinfo=tmp.tzinfo)
        elif gra == "weekly":
            tmp = baseDate - timedelta(weeks=offset)
            return datetime(year=tmp.year, month=tmp.month, day=tmp.day, tzinfo=tmp.tzinfo)
        elif gra == "monthly":
            year = baseDate.year
            month = baseDate.month - offset
            if month < 0:
                year = year + month // 12
                month = month % 12
            if month == 0:
                month = 12
                year -= 1
            return datetime(year=year, month=month, day=1, tzinfo=baseDate.tzinfo)
        elif gra == "yearly":
            year = baseDate.year - offset
            return datetime(year=year, month=1, day=1, tzinfo=baseDate.tzinfo)

    @staticmethod
    def getTimeRange(baseDate, gra, offset):
        """

        :param baseDate:
        :param gra:
        :param offset:  should be more than 0
        :return:
        """
        start_time = None
        end_time = None
        if not isinstance(baseDate, datetime):
            return
        gra = Granularity.formatGranularity(gra)
        if gra == "other":
            pass
        elif gra == "minutely":
            tmps = baseDate + timedelta(minutes=offset)
            tmpe = baseDate + timedelta(minutes=(offset + 1))
            start_time = datetime(year=tmps.year, month=tmps.month, day=tmps.day, hour=tmps.hour, minute=tmps.minute)
            end_time = datetime(year=tmpe.year, month=tmpe.month, day=tmpe.day, hour=tmpe.hour, minute=tmpe.minute)
        elif gra == "hourly":
            tmps = baseDate + timedelta(hours=offset)
            tmpe = baseDate + timedelta(hours=(offset + 1))
            start_time = datetime(year=tmps.year, month=tmps.month, day=tmps.day, hour=tmps.hour)
            end_time = datetime(year=tmpe.year, month=tmpe.month, day=tmpe.day, hour=tmpe.hour)
        elif gra == "daily":
            tmps = baseDate + timedelta(days=offset)
            tmpe = baseDate + timedelta(days=(offset + 1))
            start_time = datetime(year=tmps.year, month=tmps.month, day=tmps.day)
            end_time = datetime(year=tmpe.year, month=tmpe.month, day=tmpe.day)
        elif gra == "weekly":
            tmpStartDate = baseDate + timedelta(weeks=offset)
            tmpEndDate = tmpStartDate + timedelta(weeks=1)
            start_time = datetime(year=tmpStartDate.year, month=tmpStartDate.month, day=tmpStartDate.day)
            end_time = datetime(year=tmpEndDate.year, month=tmpEndDate.month, day=tmpEndDate.day)
        elif gra == "monthly":
            year = baseDate.year
            month = baseDate.month + offset
            if month >= 12 or month <= 0:
                year += month // 12
                month = month % 12
            if month == 0:
                month = 12
                year -= 1
            start_time = datetime(year=year, month=month, day=1)
            tmpMonth = month + 1
            tmpYear = year
            if tmpMonth == 13:
                tmpMonth = 1
                tmpYear += 1
            end_time = datetime(year=tmpYear, month=tmpMonth, day=1)
        elif gra == "yearly":
            start_time = datetime(year=baseDate.year + offset, month=baseDate.month, day=baseDate.day)
            end_time = datetime(year=baseDate.year + offset + 1, month=baseDate.month, day=baseDate.day)
        start_time = start_time.replace(tzinfo=beijing)
        end_time = end_time.replace(tzinfo=beijing)
        return start_time, end_time

    @staticmethod
    def splitTimeByGranularity(start_time, end_time, gra):
        if not isinstance(start_time, datetime) or not isinstance(end_time, datetime):
            return
        gra = Granularity.formatGranularity(gra)
        start_time = Granularity.get_next_execution_date(start_time, gra)
        start_time = start_time.replace(tzinfo=beijing)
        end_time = end_time.replace(tzinfo=beijing)
        res = []
        if gra == "other":
            pass
        elif gra == "minutely":
            tmpTime = start_time
            while tmpTime < end_time:
                res.append(tmpTime)
                tmpTime = tmpTime + timedelta(minutes=1)
        elif gra == "hourly":
            tmpTime = start_time
            while tmpTime < end_time:
                res.append(tmpTime)
                tmpTime = tmpTime + timedelta(hours=1)
        elif gra == "daily":
            tmpTime = start_time
            while tmpTime < end_time:
                res.append(tmpTime)
                tmpTime = tmpTime + timedelta(days=1)
        elif gra == "weekly":
            tmpTime = start_time
            while tmpTime < end_time:
                res.append(tmpTime)
                tmpTime = tmpTime + timedelta(weeks=1)
        elif gra == "monthly":
            tmpTime = start_time
            while tmpTime < end_time:
                res.append(tmpTime)
                year = tmpTime.year
                month = tmpTime.month + 1
                if month == 13:
                    month = 1
                    year += 1
                tmpTime = datetime(year=year, month=month, day=1, tzinfo=tmpTime.tzinfo)
        elif gra == "yearly":
            tmpTime = start_time
            while tmpTime < end_time:
                res.append(tmpTime)
                tmpTime = datetime(year=tmpTime.year + 1, month=1, day=1, tzinfo=tmpTime.tzinfo)
        return res

    @staticmethod
    def get_latest_execution_date(base_time, gra):
        """
        Generate latest execution_date by base_time
        example: daily,  2021-11-01 01:00:00  ->  2021-11-01 00:00:00
                 weekly, 2021-11-03 01:00:00  ->  2021-11-01 00:00:00
                 monthly,2021-11-23 01:00:00  ->  2021-11-01 00:00:00
                 yearly, 2021-11-23 01:00:00  ->  2021-01-01 00:00:00
        :param base_time:
        :param gra:
        :return:
        """
        if not isinstance(base_time, datetime):
            return
        gra = Granularity.formatGranularity(gra)
        if gra == "minutely":
            tmpTime = datetime(year=base_time.year, month=base_time.month,
                               day=base_time.day, hour=base_time.hour, minute=base_time.minute, tzinfo=base_time.tzinfo)
        elif gra == "hourly":
            tmpTime = datetime(year=base_time.year, month=base_time.month,
                               day=base_time.day, hour=base_time.hour, tzinfo=base_time.tzinfo)
        elif gra == "daily":
            tmpTime = datetime(year=base_time.year, month=base_time.month,
                               day=base_time.day, tzinfo=base_time.tzinfo)
        elif gra == "weekly":
            if base_time.weekday() == 0:
                tmpTime = datetime(year=base_time.year, month=base_time.month,
                                   day=base_time.day, tzinfo=base_time.tzinfo)
            else:
                tmpTime = datetime(year=base_time.year, month=base_time.month,
                                   day=base_time.day, tzinfo=base_time.tzinfo) \
                          - timedelta(days=base_time.weekday())
        elif gra == "monthly":
            tmpTime = datetime(year=base_time.year, month=base_time.month, day=1, tzinfo=base_time.tzinfo)
        elif gra == "yearly":
            tmpTime = datetime(year=base_time.year, month=1, day=1, tzinfo=base_time.tzinfo)
        elif gra == "other":
            tmpTime = datetime(year=base_time.year, month=base_time.month,
                               day=base_time.day, tzinfo=base_time.tzinfo)
        else:
            tmpTime = datetime(year=base_time.year, month=base_time.month,
                               day=base_time.day, tzinfo=base_time.tzinfo)
        tmpTime = tmpTime.replace(tzinfo=beijing)
        return tmpTime

    @staticmethod
    def get_next_execution_date(base_time, gra):
        """
        Generate execution_date  which range contain base_time+gra
        :param base_time:
        :param gra:
        :return:
        """
        if not isinstance(base_time, datetime):
            return
        gra = Granularity.formatGranularity(gra)

        if gra == "minutely":
            tmp = datetime(year=base_time.year, month=base_time.month,
                           day=base_time.day, hour=base_time.hour, minute=base_time.minute,tzinfo=base_time.tzinfo)
            if base_time == tmp:
                tmpTime = tmp
            else:
                tmpTime = tmp + timedelta(minutes=1)
        elif gra == "hourly":
            tmp = datetime(year=base_time.year, month=base_time.month,
                           day=base_time.day, hour=base_time.hour, tzinfo=base_time.tzinfo)
            if base_time == tmp:
                tmpTime = tmp
            else:
                tmpTime = tmp + timedelta(hours=1)
        elif gra == "daily":
            tmp = datetime(year=base_time.year, month=base_time.month,
                           day=base_time.day, tzinfo=base_time.tzinfo)
            if base_time == tmp:
                tmpTime = tmp
            else:
                tmpTime = tmp + timedelta(days=1)
        elif gra == "weekly":
            if base_time.weekday() == 0:
                tmpTime = datetime(year=base_time.year, month=base_time.month,
                                   day=base_time.day, tzinfo=base_time.tzinfo)
            else:
                tmpTime = datetime(year=base_time.year, month=base_time.month,
                                   day=base_time.day, tzinfo=base_time.tzinfo) \
                          + timedelta(days=7 - base_time.weekday())
        elif gra == "monthly":
            tmp = datetime(year=base_time.year, month=base_time.month, day=1, tzinfo=base_time.tzinfo)
            if tmp == base_time:
                tmpTime = tmp
            else:
                year = tmp.year
                month = tmp.month + 1
                if month == 13:
                    month = 1
                    year += 1
                tmpTime = datetime(year=year, month=month, day=1, tzinfo=base_time.tzinfo)
        elif gra == "yearly":
            tmp = datetime(year=base_time.year, month=1, day=1, tzinfo=base_time.tzinfo)
            if tmp == base_time:
                tmpTime = tmp
            else:
                tmpTime = datetime(year=base_time.year + 1, month=1, day=1, tzinfo=base_time.tzinfo)
        elif gra == "other":
            pass
        tmpTime = tmpTime.replace(tzinfo=beijing)
        return tmpTime

    @staticmethod
    def get_execution_date(base_time, gra):
        """
        Generate execution_date  which range contain base_time
        :param gra:
        :param base_time:
        :return: execution_date :datetime
        """
        if not isinstance(base_time, datetime):
            return
        gra = Granularity.formatGranularity(gra)
        if gra == "minutely":
            tmpTime = datetime(year=base_time.year, month=base_time.month,
                               day=base_time.day, hour=base_time.hour, minute=base_time.minute)
        elif gra == "hourly":
            tmpTime = datetime(year=base_time.year, month=base_time.month,
                               day=base_time.day, hour=base_time.hour)
        elif gra == "daily":
            tmpTime = datetime(year=base_time.year, month=base_time.month,
                               day=base_time.day)
        elif gra == "weekly":
            tmpTime = datetime(year=base_time.year, month=base_time.month, day=base_time.day) \
                      - timedelta(base_time.weekday())
        elif gra == "monthly":
            tmpTime = datetime(year=base_time.year, month=base_time.month, day=1)

        elif gra == "yearly":
            tmpTime = datetime(year=base_time.year, month=1, day=1)
        elif gra == "other":
            pass
        tmpTime = tmpTime.replace(tzinfo=beijing)
        return tmpTime

    @staticmethod
    def get_last_time_of_different_gra(execution_date, offset, source_gra, target_gra):
        source_gra = Granularity.formatGranularity(source_gra)
        target_gra = Granularity.formatGranularity(target_gra)
        if source_gra < target_gra:
            return
        execution_date + timedelta(XXX=offset)
        pass

    @staticmethod
    def get_detail_time_range(base_gra=None, depend_gra=None, detailed_gra=None, detailed_dependency=None,
                              base_date=None):
        """
        base_gra int 数据产出粒度,
        detailed_gra int 详细依赖的粒度,
        depend_gra int 依赖的粒度,
        detailed_dependency string or list 依赖的具体时间,
        base_data datatime 基础时间
        """
        # TODO 可以将detailed_gra，detailed_dependency 放入这个方法里，(base_date的计算放入该方法里可能也会方便一点)
        # ["hourly", "daily", "weekly", "monthly", "yearly", "other"]
        res = []
        if Granularity.getIntGra(depend_gra) >= Granularity.getIntGra(detailed_gra) > Granularity.getIntGra(base_gra):
            raise ValueError("[DataCake] 粒度配置错误")
        detailed_gra_str = Granularity.formatGranularity(detailed_gra)
        depend_gra_str = Granularity.formatGranularity(depend_gra)
        base_date = Granularity.get_execution_date(base_date, depend_gra)
        if isinstance(detailed_dependency, six.string_types):
            detailed_dependency = detailed_dependency.split(',')
        if "ALL" in detailed_dependency:
            end_time = Granularity.getNextTime(baseDate=base_date,gra=depend_gra,offset=-1)
            res = Granularity.splitTimeByGranularity(start_time=base_date,
                                                     end_time=end_time,
                                                     gra=detailed_gra)
            return res
        if base_gra < detailed_gra:
            return ValueError("detailed_gra is small than base_gra")
        if depend_gra_str == "daily" and detailed_gra_str == "hourly":
            for detail_date in detailed_dependency:
                if "L" in detail_date:
                    new_str = detail_date.replace(' ', '')
                    if len(new_str) == 1:
                        res.append(base_date + timedelta(hours=23))
                    elif len(new_str) >= 3:
                        op = new_str[1]
                        num = int(new_str[2:])
                        if op == "-":
                            res.append(base_date + timedelta(hours=(23 - num)))
                        elif op == "+":
                            # 正常情况应该没有
                            res.append(base_date + timedelta(hours=(23 + num)))
                else:
                    res.append(base_date.replace(hour=int(detail_date)))
        elif depend_gra_str == "weekly" and detailed_gra_str == "daily":
            monday = base_date - timedelta(days=base_date.weekday())
            for detail_date in detailed_dependency:
                if base_gra == detailed_gra:
                    if "L" in detail_date:
                        new_str = detail_date.replace(' ', '')
                        if len(new_str) == 1:
                            res.append(monday + timedelta(days=6))
                        elif len(new_str) >= 3:
                            op = new_str[1]
                            num = int(new_str[2:])
                            if op == "-":
                                res.append(monday + timedelta(days=(6 - num)))
                            elif op == "+":
                                # 正常情况应该没有
                                res.append(monday + timedelta(days=(6 + num)))
                    else:
                        res.append(monday+timedelta(days=int(detail_date)-1))
                elif base_gra > detailed_gra:
                    if "L" in detail_date:
                        new_str = detail_date.replace(' ', '')
                        if len(new_str) == 1:
                            res.extend(Granularity.splitTimeByGranularity(start_time=base_date,
                                                                          end_time=base_date + timedelta(days=1),
                                                                          gra=base_gra))
                        elif len(new_str) >= 3:
                            op = new_str[1]
                            num = int(new_str[2:])
                            if op == "-":
                                res.extend(
                                    Granularity.splitTimeByGranularity(start_time=base_date + timedelta(days=(6 - num)),
                                                                       end_time=base_date + timedelta(
                                                                           days=(6 - num)) + timedelta(days=1),
                                                                       gra=base_gra))
                            elif op == "+":
                                # 正常情况应该没有
                                res.extend(
                                    Granularity.splitTimeByGranularity(start_time=base_date + timedelta(days=(6 + num)),
                                                                       end_time=base_date + timedelta(days=(7 + num)),
                                                                       gra=base_gra))
                    else:
                        res.extend(
                            Granularity.splitTimeByGranularity(start_time=base_date.replace(hour=int(detail_date)),
                                                               end_time=base_date.replace(
                                                                   hour=int(detail_date)) + timedelta(days=1),
                                                               gra=base_gra))
        elif depend_gra_str == "monthly" and detailed_gra_str == "daily":
            for detail_date in detailed_dependency:
                if base_gra == detailed_gra:
                    if "L" in detail_date:
                        new_str = detail_date.replace(' ', '')
                        if len(new_str) == 1:
                            res.append(Granularity.getNextTime(baseDate=base_date, gra=depend_gra, offset=-1)
                                       - timedelta(days=1))
                        elif len(new_str) >= 3:
                            op = new_str[1]
                            num = int(new_str[2:])
                            if op == "-":
                                res.append(
                                    Granularity.getNextTime(baseDate=base_date, gra=depend_gra, offset=-1)
                                    - timedelta(days=num + 1))
                            elif op == "+":
                                # 正常情况应该没有
                                res.append(
                                    Granularity.getNextTime(baseDate=base_date, gra=depend_gra, offset=-1)
                                    + timedelta(days=num - 1))

                    else:
                        res.append(base_date.replace(day=int(detail_date)))
                elif base_gra > detailed_gra:
                    if "L" in detail_date:
                        new_str = detail_date.replace(' ', '')
                        if len(new_str) == 1:
                            res.extend(Granularity.splitTimeByGranularity(
                                start_time=Granularity.getNextTime(baseDate=base_date, gra=depend_gra,
                                                                   offset=-1) - timedelta(days=1),
                                end_time=Granularity.getNextTime(baseDate=base_date, gra=depend_gra, offset=-1),
                                gra=base_gra))
                        elif len(new_str) >= 3:
                            op = new_str[1]
                            num = int(new_str[2:])
                            if op == "-":
                                res.extend(
                                    Granularity.splitTimeByGranularity(
                                        start_time=Granularity.getNextTime(baseDate=base_date, gra=depend_gra,
                                                                           offset=-1) - timedelta(days=num + 1),
                                        end_time=Granularity.getNextTime(baseDate=base_date, gra=depend_gra,
                                                                         offset=-1) - timedelta(days=num),
                                        gra=base_gra))
                            elif op == "+":
                                # 正常情况应该没有
                                res.extend(
                                    Granularity.splitTimeByGranularity(
                                        start_time=Granularity.getNextTime(baseDate=base_date, gra=depend_gra,
                                                                           offset=-1) + timedelta(days=num - 1),
                                        end_time=Granularity.getNextTime(baseDate=base_date, gra=depend_gra,
                                                                         offset=-1) + timedelta(days=num),
                                        gra=base_gra))
                    else:
                        res.extend(
                            Granularity.splitTimeByGranularity(start_time=base_date.replace(day=int(detail_date)),
                                                               end_time=base_date.replace(
                                                                   day=int(detail_date)) + timedelta(days=1),
                                                               gra=base_gra))
        elif depend_gra_str == "monthly" and detailed_gra_str == "weekly":
            last_date = Granularity.getNextTime(baseDate=base_date, gra=depend_gra, offset=-1) - timedelta(days=1)
            last_week_date = last_date - timedelta(days=last_date.weekday())
            tmp = base_date - timedelta(days=base_date.weekday())
            if tmp == base_date:
                first_week_date = base_date
            else:
                first_week_date = tmp + timedelta(weeks=1)
            for detail_date in detailed_dependency:
                if base_gra == detailed_gra:
                    if "L" in detail_date:
                        new_str = detail_date.replace(' ', '')
                        if len(new_str) == 1:
                            res.append(last_week_date)
                        elif len(new_str) >= 3:
                            op = new_str[1]
                            num = int(new_str[2:])
                            if op == "-":
                                res.append(last_week_date - timedelta(weeks=num))
                            elif op == "+":
                                # 正常情况应该没有
                                res.append(last_week_date + timedelta(weeks=num))
                    else:
                        res.append(first_week_date + timedelta(weeks=int(detail_date) - 1))
                elif base_gra > detailed_gra:
                    if "L" in detail_date:
                        new_str = detail_date.replace(' ', '')
                        if len(new_str) == 1:
                            res.extend(Granularity.splitTimeByGranularity(
                                start_time=last_week_date,
                                end_time=last_week_date + timedelta(weeks=1),
                                gra=base_gra))
                        elif len(new_str) >= 3:
                            op = new_str[1]
                            num = int(new_str[2:])
                            if op == "-":
                                res.extend(
                                    Granularity.splitTimeByGranularity(
                                        start_time=last_week_date - timedelta(weeks=num),
                                        end_time=last_week_date - timedelta(weeks=num - 1),
                                        gra=base_gra))
                            elif op == "+":
                                # 正常情况应该没有
                                res.extend(
                                    Granularity.splitTimeByGranularity(
                                        start_time=last_week_date + timedelta(weeks=num),
                                        end_time=last_week_date + timedelta(weeks=num + 1),
                                        gra=base_gra))
                    else:
                        res.extend(
                            Granularity.splitTimeByGranularity(
                                start_time=first_week_date + timedelta(weeks=int(detail_date) - 1),
                                end_time=first_week_date + timedelta(weeks=int(detail_date)),
                                gra=base_gra))
        return res
