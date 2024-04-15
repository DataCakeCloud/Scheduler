# -*- coding: utf-8 -*-

#  Copyright (c) 2020 Shareit.com Co., Ltd. All Rights Reserved.

# !/usr/bin/env python

"""
@Project    :   Airflow of SHAREit
@File       :   email_operator.py
@Date       :   2020/7/16 5:19 下午
@Author     :   Yunqing Mo
@Concat     :   moyq@ushareit.com
@Company    :   SHAREit
@Desciption :   None
"""
import sys

reload(sys)
sys.setdefaultencoding('utf-8')
from airflow.operators.genie_pod_operator import GeniePodOperator
from airflow.utils import apply_defaults


def generate_pod_name():
    """
    生成pod name
    :return:
    """
    return "email-operator"


class EmailOperator(GeniePodOperator):
    """
    send email operator
    :param task_id: task id
    :param receivers: receivers
    :param subject: subject
    :param input_path: email content from
    :param subtype: email content type
    :param charset: email content charset
    """

    @apply_defaults
    def __init__(self,
                 image=None,
                 namespace=None,
                 task_id="Email-Send",
                 receivers=None,
                 subject=None,
                 input_path=None,
                 service_account_name="genie",
                 subtype="html",
                 charset="utf-8",
                 name=generate_pod_name(),
                 *args,
                 **kwargs):
        super(EmailOperator, self).__init__(image=image, namespace=namespace, task_id=task_id, name=name,
                                            service_account_name=service_account_name, *args, **kwargs)
        self.receivers = receivers
        self.subject = subject
        self.input_path = input_path
        self.subtype = subtype
        self.charset = charset
        self.is_delete_operator_pod = True
        self.build_cmds()

    def valid_params(self):
        """
        验证输入参数
        :return:
        """

        def do_valid(input_param, input_param_name):
            if input_param is None or input_param == "":
                raise Exception("param [%s] can not empty." % input_param_name)

        do_valid(self.receivers, "receivers")
        do_valid(self.subject, "subject")
        do_valid(self.input_path, "input_path")
        do_valid(self.genie_conn_id, "genie_conn_id")
        if self.cluster_tags is None:
            do_valid(self.cluster, "cluster")
            do_valid(self.cluster_type, "cluster_type")
        if self.input_path.startswith("obs://"):
            self.image = ""
        elif self.input_path.startswith("s3://") or self.input_path.startswith("s3a://"):
            self.image = ""
        else:
            raise Exception("Not support file system with path:%s" % self.input_path)

    def build_cmds(self):
        """
        build cmd contains cmd and argments
        :return:
        """
        self.valid_params()
        self.cmds = ["python", "/work/email_operator_helper.py"]
        self.arguments = [self.subject, self.receivers, self.input_path, self.subtype, self.charset]
