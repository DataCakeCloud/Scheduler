# -*- coding: utf-8 -*-
import logging
import re
from datetime import datetime, timedelta

import pendulum
from croniter import croniter_range

from airflow.shareit.models.dag_dataset_relation import DagDatasetRelation
from airflow.shareit.utils.granularity import Granularity
from airflow.shareit.utils.task_cron_util import TaskCronUtil
from airflow.utils.log.logging_mixin import LoggingMixin
import json


log = LoggingMixin()
class DateCalculationUtil(LoggingMixin):
    '''
    结构:
    {
    "dateCalculationParam":{
            "month":{
                "offset":0,
                "unitOffset":"",
                "range":[],
                "type":"offset"
            },
            "day":{
                "offset":0,
                "unitOffset":"",
                "range":[],
                "type":"offset"
            },
            "hour":{
                "offset":-1,
                "unitOffset":"",
                "range":[],
                "type":"offset"
            },
            "minute":{
                "offset":0,
                "unitOffset":"",
                "range":[],
                "type":"range"
            }
        }
    }
    dateCalculationParam 也有可能是一个数组,存在多个
    '''

    def __init__(self):
        return



    @classmethod
    def get_upstream_all_execution_dates(cls,execution_date,calculation_param,upstream_task,upstream_gra=None,upstream_crontab=None):
        '''
        补充缺失的单位,生成具体的日期
        通过calculation_param计算的日期还不能定位到具体的实例日期.
        举例:
        1.假如上游产出周期是天, 每天9点45分执行, 通过calculation_param只能确认是具体的哪一天,无法拿到时/分的单位.
        2.上游产出周期是周, 周一,周二,周日每天的9点45分执行, 通过calculation_param只能确认是具体的哪一周,无法拿到天/时/分的单位
        '''
        result_date = []
        dates = cls.get_upstream_calculated_dates(execution_date=execution_date, calculation_param=calculation_param)
        # return dates
        # 通过上游任务的cron表达式,获取上游任务的执行周期
        for dt in dates:
            dts = TaskCronUtil.get_all_date_in_one_cycle(task_name=upstream_task,execution_date=dt,gra=upstream_gra,cron=upstream_crontab)
            result_date.extend(dts)

        return result_date

    # todo 待完善
    @classmethod
    def get_upstream_all_data_dates(cls,execution_date,calculation_param,gra):
        '''
        格式化日期, 数据日期会比较对比粒度低的时间单位做格式化
        比如:
        数据产出粒度为daily, 计算得出日期是2022/06/22 09:00:00, 数据日期其实是2022/06/22 00:00:00
        数据产出粒度为hourlu, 计算得出日期是2022/06/22 09:23:00, 数据日期其实是2022/06/22 09:00:00
        '''
        gra = Granularity.getIntGra(gra)
        result_date = []
        dates = cls.get_upstream_calculated_dates(execution_date=execution_date, calculation_param=calculation_param)
        for dt in dates:
            if gra == 6:
                # 小时周期格式化
                result_date.append(dt.replace(minute=0,second=0))
                continue
            if gra == 5:
                # 天周期格式化
                result_date.append(dt.replace(hour=0,minute=0,second=0))
                continue
            if gra == 4:
                # 周周期格式化
                # 获取周一的日期
                result_date.append((dt - timedelta(days=dt.weekday())).replace(hour=0,minute=0,second=0))
                continue
            if gra == 3:
                # 月周期格式化
                # 获取当月一号的日期
                result_date.append(dt.replace(day=1,hour=0,minute=0,second=0))
                continue

        return result_date


    @classmethod
    def get_upstream_calculated_dates(cls, execution_date, calculation_param):
        '''
        获取上游的日期,返回结果可能是多个
        '''
        if isinstance(calculation_param, dict):
            param_jsons = [calculation_param]
        elif  isinstance(calculation_param, list):
            param_jsons = calculation_param
        else:
            try:
                calculation_param_loads = json.loads(calculation_param)
                if isinstance(calculation_param_loads, list):
                    param_jsons = calculation_param_loads
                else :
                    param_jsons = [calculation_param_loads]

            except ValueError:
                log.logger.error()
                return []

        result_date_list = []

        for param_json in param_jsons:
            cur_execution_date = execution_date
            month_date_list = []
            week_date_list = []
            day_date_list = []
            hour_date_list = []
            mintue_date_list = []

            has_week = False

            # month
            # 月不支持range,只支持offset
            if "month" in param_json.keys():
                month_json = param_json['month']
                type = month_json['type']
                if type == 'offset':
                    offsets = str(month_json['offset']) if 'unitOffset' not in month_json.keys() else month_json['unitOffset']
                    offset_list = DateCalculationUtil.offset_range_split(offsets)
                    for offset in offset_list:
                        # cur_execution_date = cls.datetime_month_caculation(cur_execution_date, offset)
                        month_date_list.append(cls.datetime_month_caculation(cur_execution_date, offset))

            # week非必传项,可能不包含周
            if "week" in param_json.keys():
                has_week = True
                week_json = param_json['week']
                type = week_json['type']
                if type == 'offset' :
                    # 影响到了'天'单位的值,所以增加has_week标记true
                    offset = str(week_json['offset']) if 'unitOffset' not in week_json.keys() else week_json['unitOffset']
                    offset_list = DateCalculationUtil.offset_range_split(offset)
                    for offset in offset_list:
                        if len(month_date_list) > 0:
                            for m_date in month_date_list:
                                week_date_list.append(m_date + timedelta(weeks=offset))

            # day
            if "day" in param_json.keys():
                day_json = param_json['day']
                type = day_json['type']
                if type == 'offset':
                    offsets = str(day_json['offset']) if 'unitOffset' not in day_json.keys() else day_json['unitOffset']
                    offset_list = DateCalculationUtil.offset_range_split(offsets)
                    for offset in offset_list:
                        if has_week:
                            for w_date in week_date_list:
                                day_date_list.append(w_date + timedelta(days=offset))
                        elif len(month_date_list) > 0:
                            for m_date in month_date_list:
                                day_date_list.append(m_date + timedelta(days=offset))
                        else :
                            day_date_list.append(cur_execution_date + timedelta(days=offset))

                if type == 'range':
                    ranges = day_json['range']

                    # 分两种情况,周单位下的天和月单位下的天
                    # 此处是计算周单位下的天
                    if has_week :
                        if len(ranges) == 0:
                            day_date_list = week_date_list
                        for w_date in week_date_list:
                            #  获取当前execution_date日期,当周对应的周一的日期
                            # monday_exe_date = cur_execution_date - timedelta(days=cur_execution_date.weekday())
                            monday_exe_date = w_date - timedelta(days=w_date.weekday())
                            for value in ranges:
                                if 'L' in value.upper():
                                    day_date_list.append(
                                        cls.get_L_expression_result('week-day',monday_exe_date, value))
                                else:
                                    '''
                                    day_date_list中追加日期
                                    计算周单位对应日单位的范围时, 此处的value其实代表了取周几
                                    如果是周一,那么其实+0. 如果是周二,则+1 .. 以此类推
                                    '''
                                    day_date_list.append(monday_exe_date + timedelta(days=int(value)-1))

                    '''
                    此处是计算月单位下的天
                    '''
                    if not has_week and len(month_date_list) > 0:
                        if len(ranges) == 0:
                            day_date_list = month_date_list
                        for m_date in month_date_list:
                            # 由于不同月的L不同,所以这里根据年和月获取一下L
                            L = cls.get_month_L(m_date.year, m_date.month)

                            for value in ranges:
                                if 'L' in value.upper():
                                    day_date_list.append(
                                        cls.get_L_expression_result('day', m_date.replace(day=L), value))
                                else:
                                    # day_date_list中追加日期
                                    try :
                                        day_date_list.append(m_date.replace(day=int(value)))
                                    except ValueError:
                                        log.logger.warning("day unit more than max value,month:{},day:{}".format(str(m_date.month),str(int(value))))

            # hour
            if "hour" in param_json.keys():
                hour_json = param_json['hour']
                type = hour_json['type']
                if type == 'offset':
                    # offset = hour_json['offset']
                    offsets = str(hour_json['offset']) if 'unitOffset' not in hour_json.keys() else hour_json['unitOffset']
                    offset_list = DateCalculationUtil.offset_range_split(offsets)
                    for offset in offset_list:
                        for d_date in day_date_list:
                            # cur_execution_date = cur_execution_date + timedelta(hours=offset)
                            hour_date_list.append(d_date + timedelta(hours=offset))

                if type == 'range':
                    ranges = hour_json['range']
                    if len(ranges) == 0:
                        hour_date_list = day_date_list
                    L = 23
                    # day_date_list,说明是从day或month开始就已经是数值范围
                    if len(day_date_list) > 0:
                        for d_date in day_date_list:
                            for value in ranges:
                                if 'L' in value.upper():
                                    hour_date_list.append(
                                        cls.get_L_expression_result('hour', d_date.replace(hour=L), value))
                                else:
                                    # day_date_list中追加日期
                                    hour_date_list.append(d_date.replace(hour=int(value)))


            # minute
            if "minute" in param_json.keys():
                minute_json = param_json['minute']
                type = minute_json['type']
                if type == 'offset':
                    # offset = minute_json['offset']
                    offsets = str(minute_json['offset']) if 'unitOffset' not in minute_json.keys() else minute_json['unitOffset']
                    offset_list = DateCalculationUtil.offset_range_split(offsets)
                    for offset in offset_list:
                        for h_date in hour_date_list:
                            mintue_date_list.append(h_date + timedelta(minutes=offset))
                    # cur_execution_date = cur_execution_date + timedelta(minutes=offset)

                if type == 'range':
                    ranges = minute_json['range']
                    if len(ranges) == 0:
                        mintue_date_list = hour_date_list
                    L = 59
                    if len(hour_date_list) > 0:
                        for h_date in hour_date_list:
                            for value in ranges:
                                if 'L' in value.upper():
                                    mintue_date_list.append(
                                        cls.get_L_expression_result('minute', h_date.replace(minute=L), value))
                                else:
                                    # mintue_date_list追加日期
                                    mintue_date_list.append(h_date.replace(minute=int(value)))


            if len(mintue_date_list) > 0:
                result_date_list.extend(mintue_date_list)
                continue

            if len(hour_date_list) > 0:
                result_date_list.extend(hour_date_list)
                continue

            if len(day_date_list) > 0:
                result_date_list.extend(day_date_list)
                continue

            if len(week_date_list) > 0:
                result_date_list.extend(week_date_list)
                continue

            if len(month_date_list) > 0:
                result_date_list.extend(month_date_list)
                continue

            result_date_list.append(cur_execution_date)
        return result_date_list


    @classmethod
    def get_downstream_all_execution_dates(cls,execution_date,calculation_param,downstream_task,downstream_gra=None,downstream_crontab=None):
        '''
        获取下游所有的execution_date日期, 触发场景下是需要确定下游任务的具体execution_date是什么,然后做触发.

        补充缺失的单位,生成具体的日期
        通过calculation_param计算的日期还不能定位到具体的实例日期.
        举例:
        1.假如上游产出周期是天, 每天9点45分执行, 通过calculation_param只能确认是具体的哪一天,无法拿到时/分的单位.
        2.上游产出周期是周, 周一,周二,周日每天的9点45分执行, 通过calculation_param只能确认是具体的哪一周,无法拿到天/时/分的单位
        '''
        result_date = []
        dates = cls.get_downstream_calculated_date(execution_date=execution_date, calculation_param=calculation_param)

        for dt in dates:
            dts = TaskCronUtil.get_all_date_in_one_cycle(task_name=downstream_task,execution_date=dt[0],max_gra=dt[1],gra=downstream_gra,cron=downstream_crontab)
            result_date.extend(dts)
        return result_date

    @classmethod
    def get_downstream_all_data_dates(cls,execution_date,calculation_param,gra):
        '''
        补充缺失的单位,生成具体的日期
        通过calculation_param计算的日期还不能定位到具体的实例日期.
        举例:
        1.假如上游产出周期是天, 每天9点45分执行, 通过calculation_param只能确认是具体的哪一天,无法拿到时/分的单位.
        2.上游产出周期是周, 周一,周二,周日每天的9点45分执行, 通过calculation_param只能确认是具体的哪一周,无法拿到天/时/分的单位
        '''
        gra=Granularity.getIntGra(gra)
        result_date = []
        dates = cls.get_downstream_calculated_date(execution_date=execution_date, calculation_param=calculation_param)
        for dt in dates:
            # 如果允许被触发的最小粒度比数据本身的产出粒度还大,则无法触发
            if dt[1] < gra:
                continue
            if gra == 6:
                # 小时周期格式化
                result_date.append(dt[0].replace(minute=0, second=0))
            if gra == 5:
                # 天周期格式化
                result_date.append(dt[0].replace(hour=0, minute=0, second=0))
            if gra == 4:
                # 周周期格式化
                # 获取周一的日期
                result_date.append((dt[0] - timedelta(days=dt[0].weekday())).replace(hour=0, minute=0, second=0))
            if gra == 3:
                # 月周期格式化
                # 获取当月一号的日期
                result_date.append(dt[0].replace(day=1, hour=0, minute=0, second=0))
        return result_date

    @staticmethod
    def formatted_execution_date(execution_date,gra):
        gra = Granularity.getIntGra(gra)
        if gra == 6:
            return execution_date.replace(minute=0, second=0)
        if gra == 5:
            return execution_date.replace(hour=0, minute=0, second=0)
        if gra == 4:
            return (execution_date - timedelta(days=execution_date.weekday())).replace(hour=0, minute=0, second=0)
        if gra == 3:
            return execution_date.replace(day=1, hour=0, minute=0, second=0)
        return execution_date

    @classmethod
    def get_downstream_calculated_date(cls, execution_date,calculation_param):
        '''
        与upstream不同的是,此函数是当上游任务完成时, 用来找到下游的实例日期
        calculation_param: 下游配置依赖上游(也就是当前任务的计算参数)
        execution_date: 当前任务(也就是上游)的实例日期

        return: 返回值只有一个日期,因为下游实例依赖上游实例, 一定是下游对上游 -- 1对多的情况
                当上游日期不属于下游实例日期依赖的范围时,返回None

        有些情况我们是无法知道下游的具体日期的, 比如说上游任务本身天周期任务, 但是下游依赖他过去一个月的数据,

        '''

        if isinstance(calculation_param, dict):
            param_jsons = [calculation_param]
        elif isinstance(calculation_param, list):
            param_jsons = calculation_param
        else:
            try:
                calculation_param_loads = json.loads(calculation_param)
                if isinstance(calculation_param_loads, list):
                    param_jsons = calculation_param_loads
                else:
                    param_jsons = [calculation_param_loads]

            except ValueError:
                log.error()
                return []


        result_dates = []

        for param_json in param_jsons:
            allow_trigger_gra = None
            final_execution_date = None
            cur_execution_date = execution_date
            # 是否有周单位
            has_week = False


            if "month" in param_json.keys():
                month_json = param_json['month']
                type = month_json['type']
                if type == 'offset':
                    offsets = str(month_json['offset']) if 'unitOffset' not in month_json.keys() else month_json['unitOffset']
                    last_offset = DateCalculationUtil.get_last_offset(offsets)
                    # 因为是上游找下游,所以这里减去offset
                    cur_execution_date = cls.datetime_month_caculation(cur_execution_date, -int(last_offset))

            #
            if "week" in param_json.keys():
                has_week = True
                week_json = param_json['week']
                type = week_json['type']
                if type == 'offset':
                    offsets = str(week_json['offset']) if 'unitOffset' not in week_json.keys() else week_json['unitOffset']
                    last_offset = DateCalculationUtil.get_last_offset(offsets)
                    # 因为是上游找下游,所以这里减去offset
                    cur_execution_date = cur_execution_date + timedelta(weeks=-int(last_offset))


            if "day" in param_json.keys():
                day_json = param_json['day']
                type = day_json['type']
                if type == 'offset':
                    offsets = str(day_json['offset']) if 'unitOffset' not in day_json.keys() else day_json['unitOffset']
                    last_offset = DateCalculationUtil.get_last_offset(offsets)
                    # 因为是上游找下游,所以这里减去offset
                    cur_execution_date = cur_execution_date + timedelta(days=-int(last_offset))

                # 当type等于range时,
                if type == 'range':
                    # 出现range时,代表能算出的下游任务的最精确粒度日期已经确定,也就是上一个粒度偏移计算后的日期
                    final_execution_date = cur_execution_date
                    allow_trigger_gra = 4 if has_week else 3
                    ranges = day_json['range']
                    # 如果has_week=ture,说明下游是以周的周期去依赖上游的
                    if has_week:
                        # 当前execution_dae日期对应的是周几,  用来判断是否满足范围
                        week_number = execution_date.weekday() + 1
                        # week的比较特殊,获取的range其实是周一到周日
                        week_day_transfor_ranges = cls.week_day_ranges_transfomer(ranges)
                        if len(week_day_transfor_ranges) != 0 and week_number not in week_day_transfor_ranges:
                            continue
                        # else:
                        #     # 将周之下的单位天,置为周一的日期
                        #     cur_execution_date = cur_execution_date - timedelta(days=cur_execution_date.weekday())

                    # 如果has_week=false,说明下游是以月周期去依赖上游的, 这里只需要判断当前execution_date的天是否属于下游所依赖的范围内
                    else:
                        transfor_ranges = cls.ranges_transformer(execution_date, 'day', ranges)
                        if len(transfor_ranges) != 0 and execution_date.day not in transfor_ranges:
                            continue
                        # else:
                        #     cur_execution_date = cur_execution_date.replace(day=1)

            if "hour" in param_json.keys():
                hour_json = param_json['hour']
                type = hour_json['type']
                if type == 'offset':
                    offsets = str(hour_json['offset']) if 'unitOffset' not in hour_json.keys() else hour_json['unitOffset']
                    last_offset = DateCalculationUtil.get_last_offset(offsets)
                    # offset = hour_json['offset']
                    # 因为是上游找下游,所以这里减去offset
                    cur_execution_date = cur_execution_date + timedelta(hours=-int(last_offset))

                # 当type等于range时,说明下游是以日或月或年周期去依赖上游的, 这里只需要判断当前execution_date的小时是否属于下游所依赖的范围内
                if type == 'range':
                    # 出现range时,代表能算出的下游任务的最精确粒度日期已经确定,也就是上一个粒度偏移计算后的日期
                    final_execution_date = cur_execution_date if final_execution_date is None else final_execution_date
                    allow_trigger_gra = 5 if allow_trigger_gra is None else  allow_trigger_gra
                    ranges = hour_json['range']
                    transfor_ranges = cls.ranges_transformer(execution_date, 'hour', ranges)
                    if len(transfor_ranges) != 0 and execution_date.hour not in transfor_ranges:
                        continue
                    # else:
                    #     cur_execution_date = cur_execution_date.replace(hour=0)

            if "minute" in param_json.keys():
                minute_json = param_json['minute']
                type = minute_json['type']
                if type == 'offset':
                    offsets = str(minute_json['offset']) if 'unitOffset' not in minute_json.keys() else minute_json['unitOffset']
                    last_offset = DateCalculationUtil.get_last_offset(offsets)
                    # 因为是上游找下游,所以这里减去offset
                    cur_execution_date = cur_execution_date + timedelta(minutes=-int(last_offset))

                # 当type等于range时,说明下游是以小时或日或月或年周期去依赖上游的, 这里只需要判断当前execution_date的分钟是否属于下游所依赖的范围内
                if type == 'range':
                    # 出现range时,代表能算出的下游任务的最精确粒度日期已经确定,也就是上一个粒度偏移计算后的日期
                    final_execution_date = cur_execution_date if final_execution_date is None else final_execution_date
                    allow_trigger_gra = 6 if allow_trigger_gra is None else allow_trigger_gra
                    ranges = minute_json['range']
                    transfor_ranges = cls.ranges_transformer(execution_date, 'minute', ranges)
                    if len(transfor_ranges) != 0 and execution_date.minute not in transfor_ranges:
                        continue
                    # else:
                    #     cur_execution_date = cur_execution_date.replace(minute=0)
            final_execution_date = cur_execution_date if final_execution_date is None else final_execution_date
            allow_trigger_gra = 7 if allow_trigger_gra is None else allow_trigger_gra
            result_dates.append((final_execution_date,allow_trigger_gra))
        return result_dates

    @classmethod
    def ranges_transformer(cls, execution_date, unit, ranges):
        '''
        ranges中会包含一些L表达式的内容, 将其转换为对应execution_date的实际数字
        unit: 单位,一定为 month/day/hour/minute其中之一

        return: 返回的是传入单位对应的数值
        '''
        result_ranges = []
        temp_L_ranges = []

        for value in ranges:
            if 'L' in value.upper():
                # 问题来了,如果是L+ xxx的情况怎么判断啊?? 想了一下,涉及到进位的情况,从上游找下游属实太难, 直接过滤这种
                # 问题又来了,过滤掉还怎么触发下游? 这个简单... 在配置端限制,不允许只配置L+ xxx这种类型的range,嘻嘻~
                if '-' in value.upper():
                    temp_L_ranges.append(value)
            else:
                result_ranges.append(int(value))

        if unit == 'month':
            L = 12

        if unit == 'day':
            L = cls.get_month_L(execution_date.year, execution_date.month)

        if unit == 'hour':
            L = 24

        if unit == 'minute':
            L = 59

        for L_expression in temp_L_ranges:
            exec('result_ranges.append({})'.format(L_expression.replace('L', str(L))))

        return result_ranges

    @classmethod
    def week_day_ranges_transfomer(cls, ranges):
        '''
        此函数单独使用在 周单位对应下的天单位range范围的情况
        ranges中会包含一些L表达式的内容, 在此转化为对应的周几
        '''
        result_ranges = []

        for value in ranges:
            if 'L' in value.upper():
                # 问题来了,如果是L+ xxx的情况怎么判断啊?? 想了一下,涉及到进位的情况,从上游找下游属实太难, 直接过滤这种
                # 问题又来了,过滤掉还怎么触发下游? 这个简单... 在配置端限制,不允许只配置L+ xxx这种类型的range,嘻嘻~
                if '-' in value.upper():
                    # 对应周之下的天来说, L就等于星期日也就是7
                    exec('result_ranges.append({})'.format(value.upper().replace('L', '7')))
            else:
                result_ranges.append(int(value))
        return result_ranges


    @classmethod
    def get_month_L(cls, year, month):
        '''
        由于每个月的天数不一样,L代表每个月的最后一天,这里用来获取对应月的L数值
        '''
        if int(month) < 0:
            month = abs(int(month))
        if int(month) > 12:
            month = int(month) % 12
        if int(month) == 0:
            month = 12

        if int(month) == 1:
            return 31
        if int(month) == 2:
            return 29 if (int(year) % 4 == 0 and int(year) % 100 != 0) or (int(year) % 400 == 0) else 28  # 闰年29天,平年28天
        if int(month) == 3:
            return 31
        if int(month) == 4:
            return 30
        if int(month) == 5:
            return 31
        if int(month) == 6:
            return 30
        if int(month) == 7:
            return 31
        if int(month) == 8:
            return 31
        if int(month) == 9:
            return 30
        if int(month) == 10:
            return 31
        if int(month) == 11:
            return 30
        if int(month) == 12:
            return 31

    @classmethod
    def get_L_expression_result(cls, unit, cur_date, expression):
        '''
        用来将L表达式,计算并转换为日期
        unit: L的单位
        cur_date: 传入日期
        expression: L运算表达式
        '''
        match = re.match(r'L\s*((\+|-)+\s*[0-9]+)', expression, re.I)
        if match:
            # value一定是 +/-某个数值
            value = match.group(1)
        else:
            # 理论这里应该不会出现这种情况, 在web端已经作好校验了
            value = 0

        if (unit == 'month'):
            return cls.datetime_month_caculation(cur_date, int(value))

        if (unit == 'week-day'):
            '''
            因为当计算`周单位`下对应`日单位`的范围时,L代表周日,也就是最后一天.
            那么其实相对于周一来说, L就是+6天
            举例: L+1 = +6 +1  , L-1 = +6 -1
            '''
            return cur_date + timedelta(days=6+int(value))

        if (unit == 'day'):
            return cur_date + timedelta(days=int(value))

        if (unit == 'hour'):
            return cur_date + timedelta(hours=int(value))

        if (unit == 'minute'):
            return cur_date + timedelta(minutes=int(value))

    @classmethod
    def datetime_month_caculation(cls, cur_date, offset):
        '''
        不知道为啥timedelta不支持month的运算....
        这里自己简单实现一下
        '''
        new_month = cur_date.month + int(offset)
        if new_month > 0 and new_month <= 12:
            try :
                return cur_date.replace(month=new_month)
            except ValueError:
                return cls.datetime_month_caculation(cur_date - timedelta(days=1),offset)

        if new_month > 12:
            carry = int(new_month / 12)
            new_month = 12 if new_month % 12 == 0 else new_month % 12
            new_year = cur_date.year + carry
            try :
                return cur_date.replace(year=new_year, month=new_month)
            except ValueError:
                return cls.datetime_month_caculation(cur_date - timedelta(days=1),offset)

        if new_month <= 0:
            carry = int(abs(new_month) / 12) + 1
            new_month = 12 - abs(new_month) % 12
            new_year = cur_date.year - carry
            try :
                return cur_date.replace(year=new_year, month=new_month)
            except ValueError:
                return cls.datetime_month_caculation(cur_date - timedelta(days=1),offset)


    @classmethod
    def datetime_week_caculation(cls, cur_date, offset):
        '''
        timedelta虽然支持week的运算,但其实只是加了7天或者减了7天... 不是我们想要的效果
        我们希望获取到的每个周的日期都是从1号开始的
        '''
        cur_date = cur_date + timedelta(weeks=offset)

    @staticmethod
    def offset_range_split(offset_range):
        offset_list = []
        if '~' in offset_range:
            start_end = offset_range.split('~')
            if len(start_end) < 2:
                offset_list.append(int(start_end[0]))
                return offset_list
            cur = int(start_end[0])
            end = int(start_end[1])
            while cur <= end:
                offset_list.append(cur)
                cur = cur + 1
        else:
            offset_list.append(int(offset_range))
        return offset_list

    @staticmethod
    def get_last_offset(offset_range):
        offset_list = DateCalculationUtil.offset_range_split(offset_range)
        return offset_list[len(offset_list)-1]



    @staticmethod
    def generate_ddr_date_calculation_param(ddrs):
        '''
        将历史没有date_calculation_param的ddr,按照以前的格式转换成date_calculation_param
        '''
        for ddr in ddrs:
            if ddr.date_calculation_param is None or ddr.date_calculation_param == '':
                upstream_output_gra = DagDatasetRelation.get_dataset_granularity(ddr.metadata_id)
                if upstream_output_gra == -1:
                    upstream_output_gra = int(
                        ddr.detailed_gra) if ddr.detailed_gra is not None and ddr.detailed_gra != '' else int(
                        ddr.granularity)

                upstream_output_gra = Granularity.getIntGra(upstream_output_gra)
                if int(ddr.granularity) == upstream_output_gra:
                    ddr.date_calculation_param = DateCalculationUtil.offset_2_data_calculation_param(ddr.granularity,ddr.offset)

                # 依赖粒度比上游本身产出粒度大
                if int(ddr.granularity) < upstream_output_gra:
                    if ddr.detailed_dependency is not None and ddr.detailed_gra is not None:
                        if "ALL" in str.upper(ddr.detailed_dependency):
                            ddr.date_calculation_param = DateCalculationUtil.offset_2_data_calculation_param(ddr.granularity,
                                                                                          ddr.offset,is_all=True)
                        else:
                            ddr.date_calculation_param = DateCalculationUtil.qwewqewqeqwe(ddr.granularity, ddr.offset,
                                                                                          ddr.detailed_dependency)
                    else:
                        ddr.date_calculation_param = DateCalculationUtil.offset_2_data_calculation_param(ddr.granularity, ddr.offset,is_all=True)
                    ddr.granularity = upstream_output_gra
        return ddrs

    @staticmethod
    def offset_2_data_calculation_param(gra, offset,is_all=False):
        '''
        将旧的offset配置转换为新的时间计算参数
        '''
        gra = Granularity.getIntGra(gra)
        if is_all:
            hour_all = '"1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","0"'
            week_day_all = '"1","2","3","4","5","6","7"'
            month_day_all = '"1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31"'
        else :
            hour_all = ''
            week_day_all = ''
            month_day_all = ''

        if gra == 7:
            return '''
            {{"month": {{"range": [], "type": "offset", "unitOffset": "0"}}, "minute": {{"range": [], "type": "offset", "unitOffset": "{minute}"}}, "day": {{"range": [], "type": "offset", "unitOffset": "0"}}, "hour": {{"range": [], "type": "offset", "unitOffset": "0"}}}}
            '''.format(minute=offset)
        if gra == 6:
            return '''
            {{"month": {{"range": [], "type": "offset", "unitOffset": "0"}}, "day": {{"range": [], "type": "offset", "unitOffset": "0"}}, "hour": {{"range": [], "type": "offset", "unitOffset": "{hour}"}}}}
            '''.format(hour=offset)
        if gra == 5:
            return '''
            {{"month": {{"range": [], "type": "offset", "unitOffset": "0"}}, "day": {{"range": [], "type": "offset", "unitOffset": "{day}"}}, "hour": {{"range": [{hour}], "type": "range", "unitOffset": "0"}}}}
            '''.format(day=offset,hour=hour_all)
        if gra == 4 :
            return '''
            {{"week": {{"range": [], "type": "offset", "unitOffset": "{week}"}}, "month": {{"range": [], "type": "offset", "unitOffset": "0"}}, "day": {{"range": [{day}], "type": "range", "unitOffset": "0"}}}}
            '''.format(week=offset,day=week_day_all)
        if gra == 3 :
            return '''
            {{"month": {{"range": [], "type": "offset", "unitOffset": "{month}"}}, "day": {{"range": [{day}], "type": "range", "unitOffset": "0"}}}}
            '''.format(month=offset,day=month_day_all)
        return

    @staticmethod
    def qwewqewqeqwe(high_gra, offset,range):
        '''
         将旧的offset配置(特殊处理detailed_gra这种的配置任务) 转换为新的时间计算参数
         '''
        high_gra = Granularity.getIntGra(high_gra)
        m_range = map(lambda x: '"{}"'.format(x), range.split(','))
        format_range = reduce(lambda x,y: '{},{}'.format(x, y),m_range)
        if high_gra == 5:
            return '''
            {{"month": {{"range": [], "type": "offset", "unitOffset": "0"}}, "minute": {{"range": [], "type": "offset", "unitOffset": "0"}}, "day": {{"range": [], "type": "offset", "unitOffset": "{day}"}}, "hour": {{"type": "range", "range": [{hour}]}}}}
            '''.format(day=offset,hour=format_range)
        if high_gra == 4 :
            return '''
            {{"week": {{"range": [], "type": "offset", "unitOffset": "{week}"}}, "month": {{"range": [], "type": "offset", "unitOffset": "0"}},  "day": {{ "type": "range", "range": [{day}]}}}}
            '''.format(week=offset,day=format_range)
        if high_gra == 3:
            return '''
                {{"month": {{"range": [], "type": "offset", "unitOffset": "{month}"}}, "day": {{ "type": "range", "range": [{day}]}}}}
                '''.format(month=offset, day=format_range)

    @staticmethod
    def get_execution_date_in_date_range(task_name,start_date,end_date):
        '''
        获取某个任务在一个日期范围内的首个execution_date
        '''
        cron = TaskCronUtil.get_task_cron(task_name)
        if cron == '':
            return None

        for dt in croniter_range(start_date, end_date, cron):
            return dt

        return None

    @staticmethod
    def format_execution_date_by_gra(execution_date,gra):
        gra = Granularity.getIntGra(gra)
        # 如果允许被触发的最小粒度比数据本身的产出粒度还大,则无法触发
        if gra == 6:
            # 小时周期格式化
            return execution_date.replace(minute=0, second=0)
        if gra == 5:
            # 天周期格式化
            return execution_date.replace(hour=0, minute=0, second=0)
        if gra == 4:
            # 周周期格式化
            # 获取周一的日期
            return (execution_date - timedelta(days=execution_date.weekday())).replace(hour=0, minute=0, second=0)
        if gra == 3:
            # 月周期格式化
            # 获取当月一号的日期
            return execution_date.replace(day=1, hour=0, minute=0, second=0)
        return execution_date