import json
import re
import unittest
from datetime import datetime,timedelta

from airflow.shareit.utils.date_calculation_util import DateCalculationUtil
from airflow.utils import timezone
import pendulum
from croniter import croniter,croniter_range
import time
from airflow.shareit.service.diagnosis_service import DiagnosisService



class DateCalculationTest(unittest.TestCase):

    def test_get_upstream_calculated_dates(self):
        param='''
        {
        "year":{"type":"offset","offset":-1},
        "month":{"type":"range","range":["1","2","5","L-1"]},
        "day":{"type":"range","range":["10","L-1","L+1"]},
        "hour":{"type":"range","range":["3","L-1"]},
        "minute":{"type":"range","range":["1","L-1"]}
        }
        '''
        param1='''
        {
        "year":{"type":"offset","offset":-1},
        "month":{"type":"offset","offset":-25},
        "day":{"type":"offset","offset":-10},
        "hour":{"type":"offset","offset":-5},
        "minute":{"type":"offset","offset":9}
        }
        '''
        param2='''
        {
        "year":{"type":"offset","offset":-1},
        "month":{"type":"offset","offset":-14},
        "day":{"type":"offset","offset":-5},
        "hour":{"type":"range","range":["3","L-1"]},
        "minute":{"type":"range","range":["1","L-1"]}
        }
        '''

        execution_date =  datetime(year=2022,month=4,day=18)
        print execution_date - timedelta(days=execution_date.weekday())
        print execution_date.weekday()
        print execution_date + timedelta(weeks=1)
        # list = DateCalculationUtil().get_upstream_calculated_dates(execution_date, param2)
        # print list
        # for i in list :
        #     print i

    def test_get_downstream_calculated_date(self):
        param = '''
                {
        "year":{"type":"offset","offset":-1},
        "month":{"type":"range","range":["1","2","5","L-1"]},
        "day":{"type":"range","range":["10","L-1","L+1"]},
        "hour":{"type":"range","range":["0","3","L-1"]},
        "minute":{"type":"range","range":["0","1","L-1"]}
        }'''
        param1 = '''
                {
        "year":{"type":"offset","offset":-1},
        "month":{"type":"offset","offset":-25},
        "day":{"type":"offset","offset":-10},
        "hour":{"type":"offset","offset":-5},
        "minute":{"type":"offset","offset":9}
        }
                '''
        param2 = '''
                {
        "year":{"type":"offset","offset":-1},
        "month":{"type":"offset","offset":-14},
        "day":{"type":"offset","offset":-5},
        "hour":{"type":"range","range":["3","L-1"]},
        "minute":{"type":"range","range":["1","L-1"]}
        }
                '''

        param3 = '''{
        "year":{"type":"offset","offset":-1},
        "month":{"type":"offset","offset":-1},
        "week":{"type":"offset","offset":-2},
        "day":{"type":"range","range":["1","3","L-1","L+2"]},
        "hour":{"type":"range","range":[]},
        "minute":{"type":"range","range":[]}
        }
                '''

        param4 = '''{
        "month":{"type":"offset","unitOffset":"0"},
        "week":{"type":"offset","unitOffset":"-2~-1"},
        "day":{"type":"range","range":["2"]},
        "hour":{"type":"offset","unitOffset":"1"},
        "minute":{"type":"range","range":["23"]}
        }
                '''

        param5 = '''[{
        "month":{"type":"offset","unitOffset":"-1"},
        "day":{"type":"range","range":["20","L","L-1","L+1"]},
        "hour":{"type":"offset","unitOffset":"-1"},
        "minute":{"type":"range","range":["23"]}
        },{
        "month":{"type":"offset","unitOffset":"0"},
        "week":{"type":"offset","unitOffset":"-2~-1"},
        "day":{"type":"range","range":["1","L","L-1","L+1"]},
        "hour":{"type":"range","range":["1"]},
        "minute":{"type":"range","range":["23"]}
        }]
        '''
        execution_date = datetime(year=2022, month=6, day=20,hour=1,minute=23)
        result = DateCalculationUtil.get_upstream_calculated_dates(execution_date,param5)

        # result = DateCalculationUtil.get_upstream_all_data_dates(execution_date, param4,4)
        # for r in result:
        #     print r

        # print  DateCalculationUtil.get_downstream_calculated_date(param3,execution_date)
        #
        # result = DateCalculationUtil.get_downstream_calculated_date(execution_date, param5)
        result = DateCalculationUtil.get_downstream_all_data_dates(execution_date, param5,4)
        for r in result:
            print r

    def test_get_upstream_all_dates(self):
        # DateCalculationUtil.get_upstream_all_dates()
        range = '1,2'
        m_range = map(lambda x: '"{}"'.format(x), range.split(','))
        format_range = reduce(lambda x,y: '{},{}'.format(x, y),m_range)
        print  format_range

    def test_ssadsadassss(self):
        # for i in range(3,8):
        #     print json.loads(DateCalculationUtil.ssadsadassss(i,"-1~2"))

        for i in range(3,6):
            print DateCalculationUtil.qwewqewqeqwe(i,"-1~2",'L-23,L-22,L-21,L-20,L-19,L-18,L-17,L-16,L-15,L-14,L-13,L-12,L-11,L-10,L-9,L-8,L-7,L-6,L-5,L-4,L-3,L-2,L-1,L+0')

    def test_get_downstream_all_execution_dates(self):
        DateCalculationUtil.get_downstream_all_data_dates()