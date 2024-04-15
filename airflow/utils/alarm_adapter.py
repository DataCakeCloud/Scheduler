# -*- coding: utf-8 -*-
"""
author: heshan
Copyright (c) 20200103 Shareit.com Co., Ltd. All Rights Reserved.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from past.builtins import basestring
try:
    from collections.abc import Iterable as CollectionIterable
except ImportError:
    from collections import Iterable as CollectionIterable

from typing import Iterable, List, Union

from airflow.exceptions import AirflowException
from airflow.configuration import conf
from airflow.utils.log.logging_mixin import LoggingMixin

try:
    from collections.abc import Iterable as CollectionIterable
except ImportError:
    from collections import Iterable as CollectionIterable

import requests


def send_alarm(groups, subject, severity, message):
    """
    Send alarm to Cloud Manager Platform.
    """
    log = LoggingMixin().log
    if not conf.get('cloud_manager_platform', 'cmp_url'):
        log.error("Expecting cloud manager platform URL")
        raise AirflowException("cloud manager platform URL not provided in airflow configuration")

    cmp_url = conf.get('cloud_manager_platform', 'cmp_url')

    if severity.upper() == 'CRITICAL':
        severity_level = '1'
    elif severity.upper() == 'EMERGENCY':
        severity_level = '2'
    elif severity.upper() == 'WARNING':
        severity_level = '3'
    else:
        raise AirflowException('severity {} is incorrectly flagged, '
                               'severity must be critical, emergency or warning (ignore case)'.format(severity))

    for group in get_alarm_group_list(groups):
        payload = [
            {
                "labels": {
                    "alertname": subject,
                    "group": group,
                    "severity": severity_level,
                    "message": message
                },
                "annotations": {
                    "value": "True"
                }
            }
        ]
        headers = {'Content-Type': 'application/json;charset=UTF-8'}
        response = requests.post(cmp_url, json=payload, headers=headers)
        log.info('alarm payload: %s', payload)
        status_code = response.status_code
        if status_code == 201:
            log.debug('Alarm has been sent to {}'.format(cmp_url))
        elif status_code == 400:
            raise AirflowException('Alarm has not been sent to {cmp_url} yet, missing required request parameters'
                                   'status code is {status_code}'.format(cmp_url=cmp_url, status_code=status_code))
        else:
            raise AirflowException('unknown error, status code is {status_code}'.format(status_code=status_code))


def get_alarm_group_list(group):  # type: (Union[str, Iterable[str]]) -> List[str]
    if isinstance(group, basestring):
        return _get_alarm_group_list_from_str(group)

    elif isinstance(group, CollectionIterable):
        if not all(isinstance(item, basestring) for item in group):
            raise TypeError("The items in your iterable must be strings.")
        return list(group)

    received_type = type(group).__name__
    raise TypeError("Unexpected argument type: Received '{}'.".format(received_type))


def _get_alarm_group_list_from_str(group):  # type: (str) -> List[str]
    delimiters = [",", ";"]
    for delimiter in delimiters:
        if delimiter in group:
            return [group.strip() for group in group.split(delimiter)]
    return [group]

