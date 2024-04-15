# -*- coding: utf-8 -*-
import base64
from functools import wraps

from google.protobuf import json_format

from airflow.shareit.grpc.TaskServiceApi.entity import UserInfo_pb2


def get_user_info(context):
    headers = context.invocation_metadata()
    header = {}
    for value in headers:
        header[value.key] = value.value
    if 'current_login_user' in header.keys():
        user_info = json_format.Parse(base64.b64decode(header['current_login_user']), UserInfo_pb2.UserInfo())
    else :
        user_info = UserInfo_pb2.UserInfo(tenantId=1)
    return user_info

def provide_userinfo(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        arg_user = 'user_info'
        context = args[2]
        user_info = get_user_info(context)
        # func_params = func.__code__.co_varnames
        # user_in_args = arg_user in func_params and \
        #     func_params.index(arg_user) < len(args)
        # user_in_kwargs = arg_user in kwargs

        # if user_in_kwargs or user_in_args:
        #     return func(*args, **kwargs)
        # else:
            # with create_session() as session:
            #     kwargs[arg_session] = session
        kwargs[arg_user] = user_info
        return func(*args, **kwargs)

    return wrapper