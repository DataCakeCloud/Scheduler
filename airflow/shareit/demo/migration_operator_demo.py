# -*- coding: utf-8 -*-

#  Copyright (c) 2020 Shareit.com Co., Ltd. All Rights Reserved.

#!/usr/bin/env python

"""
@Project    :   Airflow of SHAREit
@File       :   migration_operator_demo.py
@Date       :   2020/7/8 5:12 下午
@Author     :   Yunqing Mo
@Concat     :   moyq@ushareit.com
@Company    :   SHAREit
@Desciption :   None
"""

from airflow.shareit.operators.migration_operator import MigrationOperator

operator = MigrationOperator(srcPath="sdfs", distPath="", s3Region="sfs2222", genie_conn_id="sdfsfs")
print(operator.command)

