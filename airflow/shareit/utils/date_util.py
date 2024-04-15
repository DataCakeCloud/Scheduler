# -*- coding: utf-8 -*-
import copy

import six
import time
import datetime
import time

import pendulum

from airflow.shareit.utils.granularity import Granularity
from airflow.utils import timezone
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.timezone import beijing, utcnow


class DateUtil(LoggingMixin):
    @staticmethod
    def date_compare(date1,date2,eq=True):
        """
        比较两个日期之间的大小, date1 >= date2 返回true
        :param date1 (datetime)
        :param date2 (datetime)
        :param eq是否包含等于
        """
        fmt = '%Y-%m-%d %H:%M:%S'
        try:
            date1 = date1.replace(tzinfo=timezone.beijing)
            date2 = date2.replace(tzinfo=timezone.beijing)
            date1_str = date1.strftime(fmt)
            date2_str = date2.strftime(fmt)
        except:
            raise Exception('date_compare get error date type,date1:{},date2:{}'.format(str(type(date1)),str(type(date2))))
        if eq:
            return date1_str >= date2_str
        else :
            return date1_str > date2_str

    @staticmethod
    def date_equals(date1,date2,fmt='%Y-%m-%d %H:%M:%S'):
        """
        比较两个日期是否相等, 相等返回true
        :param date1 (datetime)
        :param date2 (datetime)
        """
        try:
            date1_str = date1.strftime(fmt)
            date2_str = date2.strftime(fmt)
        except:
            raise Exception('date_equals get error date type,date1:{},date2:{}'.format(str(type(date1)),str(type(date2))))
        return date1_str == date2_str

    @staticmethod
    def is_date_in_the_range(cur_date,start_date,end_date):
        """
        判断某个日期是否在范围内
        :param cur_date (datetime)
        :param start_date (datetime)
        :param end_date (datetime)
        """
        if start_date == None:
            pre_compare = True
        else:
            pre_compare = DateUtil.date_compare(cur_date,start_date)

        if end_date == None:
            after_compare = True
        else:
            after_compare = DateUtil.date_compare(end_date,cur_date)

        return pre_compare and after_compare

    @staticmethod
    def get_timestamp(cur_date):
        if cur_date is None:
            return 0
        cur_date = cur_date.replace(tzinfo=beijing)
        second = int((cur_date - datetime.datetime(1970, 1, 1, tzinfo=pendulum.timezone(
            'UTC'))).total_seconds())

        return int(second * 1000 + cur_date.microsecond / 1000)

    @staticmethod
    def to_timestamp(date):
        # if isinstance(date, six.string_types):  # 会因为系统时区不同，造成时区误差
        #     date = time.strptime(date, '%Y-%m-%d %H:%M:%S')
        # else:
        #     date = date.timmetuple()
        # return int(time.mktime(date)) * 1000
        if isinstance(date, six.string_types):
            date = datetime.datetime.strptime(date, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.beijing)
        else:
            date = date.timmetuple()
        second = int((date - datetime.datetime(1970, 1, 1, tzinfo=pendulum.timezone('UTC'))).total_seconds())
        return int(second * 1000 + date.microsecond / 1000)

    @staticmethod
    def get_gantt_internal(time_difference):
        if 0 <= time_difference < 60:
            internal = 1
        elif 60 <= time_difference < 900:
            internal = 60
        elif 900 <= time_difference < 3600:  # 1小时
            internal = 600
        elif 3600 <= time_difference < 86400:  # 24小时
            internal = 3600
        elif 86400 <= time_difference < 172800:  # 48小时
            internal = 7200
        elif 172800 <= time_difference < 604800:  # 1周
            internal = 86400
        elif 604800 <= time_difference < 1209600:  # 2周
            internal = 7200
        else:
            internal = 3600
        return internal

    @staticmethod
    def format_histogram_date(date, gra):
        if gra == '7':
            start = date.strftime('%Y-%m-%d %H:%M')
        elif gra == '6':
            start = date.strftime('%Y-%m-%d %H')
        elif gra == '5':
            start = date.strftime('%Y-%m-%d')
        elif gra == '4':
            start = date.strftime('%Y-%m-%d')
        elif gra == '3':
            start = date.strftime('%Y-%m')
        else:
            start = date.strftime('%Y-%m-%d')
        return start

    @staticmethod
    def generate_gra_date(gra, execution_date, root=0):
        date = ''
        if root == 0 and gra == '6':  # 小时级任务为非根节点
            return execution_date.strftime('%H:%M:%S')
        if gra == '7':
            date = execution_date.strftime('%M:%S')
        elif gra == '6':  # 小时级任务为根节点
            date = execution_date.strftime('%M:%S')
        elif gra == '5':
            date = execution_date.strftime('%H:%M:%S')
        # todo 周级、月级任务的execution_date特征
        priority_gra_map = {
            7: "minutely",
            6: "hourly",
            5: "daily",
            4: "weekly",
            3: "monthly",
            2: "yearly",
            1: "other"
        }
        return date

    @staticmethod
    def get_check_time(date, gra, delta_time, offset=0):
        from dateutil.relativedelta import relativedelta
        if gra == 'hourly':
            start = date + relativedelta(hours=offset)
            dt = datetime.datetime.strptime(delta_time, "%M")
            check_time = datetime.datetime(start.year, start.month, start.day, start.hour, dt.minute).strftime(
                '%Y-%m-%d %H:%M:%S')
        elif gra == 'daily':
            start = date + relativedelta(days=offset)
            dt = datetime.datetime.strptime(delta_time, "%H:%M")
            check_time = datetime.datetime(start.year, start.month, start.day, dt.hour, dt.minute).strftime(
                '%Y-%m-%d %H:%M:%S')
        elif gra == 'weekly':
            date_list = delta_time.split(' ')
            print offset
            print date.weekday()
            print (int(date_list[0]) - 1 - date.weekday())
            start = date + relativedelta(weeks=offset) + relativedelta(days=(int(date_list[0]) - 1 - date.weekday()))
            dt = datetime.datetime.strptime(date_list[1], "%H:%M")
            check_time = datetime.datetime(start.year, start.month, start.day, dt.hour, dt.minute).strftime(
                '%Y-%m-%d %H:%M:%S')
        elif gra == 'monthly':
            start = date + relativedelta(months=offset)
            dt = datetime.datetime.strptime(delta_time, "%d %H:%M")
            check_time = datetime.datetime(start.year, start.month, dt.day, dt.hour, dt.minute).strftime(
                '%Y-%m-%d %H:%M:%S')
        else:
            check_time = date.strftime('%Y-%m-%d')
        return check_time

    @staticmethod
    def timestamp_str(timestamp):
        if timestamp == 0 or timestamp is None:
            return ''
        return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp/1000))

    @staticmethod
    def second_str(date):
        duration = ''
        seconds = int(date % 60)
        days = int(date // 86400)
        hours = int((date - days * 86400) // 60 // 60)
        minutes = int((date - days * 86400) // 60 % 60)
        if days:
            duration += '{0} Days '.format(days)
        if hours:
            duration += '{0} Hours '.format(hours)
        if minutes:
            duration += '{0} Min '.format(minutes)
        if seconds:
            duration += '{0} Sec'.format(seconds)
        return duration

    @staticmethod
    def get_cost_time(start, end):
        if not start or not end:
            return ''
        start = datetime.datetime.strptime(start, '%Y-%m-%d %H:%M:%S')
        end = datetime.datetime.strptime(end, '%Y-%m-%d %H:%M:%S')
        total = int((end - start).total_seconds())
        return DateUtil.second_str(total)

    @staticmethod
    def format_millisecond(dt):
        # 精确到毫秒不是微秒
        new_microsend = (dt.microsecond / 1000) * 1000
        return dt.replace(microsecond = new_microsend)

    @staticmethod
    def format_millisecond_random(dt):
        import random
        import uuid
        seed = uuid.uuid1()
        random.seed(seed)
        # 精确到毫秒不是微秒
        new_microsend = (dt.microsecond / 1000) * 1000
        new_microsend = new_microsend + int(random.uniform(0, 999999))
        if new_microsend > 999999:
            new_microsend -= 1000000
        return dt.replace(microsecond = new_microsend)

    @staticmethod
    def get_time_median(columns):
        # 运维页面中链路分析的柱状图，展示中位数
        column_time = sorted(sum(columns, []))  # 二维数组转一维，且排序
        length = len(column_time)
        if length % 2 == 0:
            median = (column_time[length // 2 - 1] + column_time[length // 2]) // 2
        else:
            median = column_time[length // 2]
        return median

    @staticmethod
    def get_cost_time_by_date(start_date,end_date):
        now_date = utcnow()
        if not end_date:
            end_date = now_date
        if not start_date:
            start_date = now_date
        end_date = end_date.replace(tzinfo=timezone.beijing)
        start_date = start_date.replace(tzinfo=timezone.beijing)
        total = int((end_date - start_date).total_seconds())
        return total

    @staticmethod
    def timestamp_to_datestr(timestamp, format='%Y-%m-%d %H:%M:%S'):
        # 时间戳转字符串
        if timestamp and timestamp != 0:
            date = pendulum.from_timestamp(timestamp/1000).in_timezone(beijing)
            return date.strftime(format)
        else :
            return ''

    @staticmethod
    def date_format_ready_time_copy(date,ready_time,granularity):
        date_cp = copy.copy(date)
        if not ready_time:
            return date_cp
        gra = Granularity.getIntGra(granularity)
        if ':' in ready_time:
            arr = ready_time.split(':')
            minute = int(arr[1])
            if gra > 5:
                return date_cp.replace(minute=minute)
            else:
                hour = int(arr[0])
                return date_cp.replace(hour=hour,minute=minute)
        else :
            minute = int(ready_time)
            return date_cp.replace(minute=minute)

    @staticmethod
    def split_date_range(start_date,end_date,delta_day=30):
        '''
        将一个日期范围，按照指定天数切分为多个日期区间。如果最后一个日期区间*2仍小于指定天数，则划归到前一区间中去
        '''
        delta = datetime.timedelta(days=delta_day)
        result = []
        cur = start_date
        while cur < end_date:
            tail = cur + delta - datetime.timedelta(seconds=1)
            if (end_date - tail).days * 2 < delta_day:
                tail = end_date
            result.append((cur,tail))
            cur = tail + datetime.timedelta(seconds=1)
        return result



