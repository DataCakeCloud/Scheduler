# -*- coding: utf-8 -*-
"""
Author: Tao Zhang
Date: 2022/4/20
If you are doing your best,you will not have to worry about failure.
"""
import requests


class Shareit_SCMP(object):
    def __init__(self,username=None,password=None,client_id="sgt-user"):
        self.username = username
        self.password = password
        self.client_id = client_id
        pass

