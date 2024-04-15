# -*- coding: utf-8 -*-
"""
Author: Tao Zhang
Date: 2021/12/21
                                                               
       ,--.                              ,--.                  
,-----.|  ,---.  ,--,--.,--,--,  ,---. ,-'  '-. ,--,--. ,---.  
`-.  / |  .-.  |' ,-.  ||      \| .-. |'-.  .-'' ,-.  || .-. | 
 /  `-.|  | |  |\ '-'  ||  ||  |' '-' '  |  |  \ '-'  |' '-' ' 
`-----'`--' `--' `--`--'`--''--'.`-  /   `--'   `--`--' `---'  
                                `---'                          
If you are doing your best,you will not have to worry about failure.
"""
import unittest
from airflow.shareit.operators.flink_batch_operator import FlinkBatchOperator,FlinkBatchHook
from airflow.models.connection import Connection
from airflow.utils.db import provide_session

context = ""
def get_long_str(length=5000):
    i = 0
    res = ""
    while True:
        res += str(i)
        i += 1
        if len(res) >= length:
            return res


class FlinkBatchOperatorTest(unittest.TestCase):
    @provide_session
    def test_conn(self, session=None):
        conn = Connection()
        conn.conn_id = "zhangtao_flink_test"
        conn.conn_type = "flink"
        conn.set_extra(value=context)
        session.merge(conn)
        session.commit()

    def test_hook(self):
        f_hook = FlinkBatchHook(conn_id="zhangtao_flink_test")
        conn = f_hook._conn
        if conn:
            print (conn.extra)

