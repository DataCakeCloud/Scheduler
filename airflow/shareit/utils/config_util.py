# -*- coding: utf-8 -*-
"""
@Time ： 2022/9/20 16:15
@Auth ： tianxu
@File ：config_util.py
"""

from airflow.configuration import conf


def get_genie_domain():
    try:
        genie_domain = conf.get("core", "default_genie_url")
    except Exception as e:
        genie_domain = ''
    genie_domain = genie_domain if genie_domain is '' or genie_domain.endswith('/') else genie_domain + '/'
    return genie_domain
