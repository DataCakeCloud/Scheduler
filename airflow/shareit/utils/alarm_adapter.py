# -*- coding: utf-8 -*-
"""
Author: Tao Zhang
Date: 2021/9/13
If you are doing your best,you will not have to worry about failure.
"""
import datetime
import json
import logging
import time

import six
import requests
from past.builtins import basestring

from airflow.shareit.constant.taskInstance import PIPELINE, FIRST, BACKFILL, CLEAR
from airflow.shareit.models.regular_alert import RegularAlert
from airflow.shareit.models.task_desc import TaskDesc
from airflow.shareit.utils.normal_util import NormalUtil

try:
    from collections.abc import Iterable as CollectionIterable
except ImportError:
    from collections import Iterable as CollectionIterable

from typing import Iterable, List, Union

from airflow.configuration import conf
from airflow.utils.state import State
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.exceptions import AirflowException

email_API_token = None
phone_result_message = None

def get_token():
    # b_url = 'https://sentry.ushareit.me/dex/token'
    b_url = conf.get('email', 'token_url')
    username = conf.get('email', 'username')
    password = conf.get('email', 'password')
    datas = {
        "username": username,
        "password": password,
        "client_id": 'sgt-notify-openapi',
        "scope": "openid groups",
        "grant_type": 'password'

    }
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    r = requests.post(url=b_url, data=datas, headers=headers)
    result = r.json()
    token = 'Bearer ' + result.get('id_token')
    return token


def build_notify_content(state=None, task_name=None, execution_date=None, template_code='', user_group=''):
    task_type = NormalUtil.template_code_transform(template_code)
    datacake_alert_title = conf.get('email','datacake_alert_title')
    base_content = """[{datacake_alert_title} 任务状态通知]{alert_head}  
任务: {task_name}  
执行入参时间: {execution_date}  
任务类型: {task_type}  
用户组: {user_group}    
[任务实例列表页链接]({task_url})"""
    if state == State.RETRY:
        alert_head = "任务本次执行失败，正在准备重试"
    elif state == State.START:
        alert_head = "任务开始执行"
    elif state == State.SUCCESS:
        alert_head = "任务执行成功"
    elif state == State.FAILED:
        alert_head = "任务执行失败"
    else:
        return None
    if isinstance(execution_date, datetime.datetime):
        date_str = execution_date.strftime('%Y-%m-%dT%H:%M:%S')
    elif isinstance(execution_date, six.string_types):
        date_str = execution_date
    else:
        date_str = " "
    task_url = _get_task_url(task_id=task_name)
    res = base_content.format(alert_head=alert_head, task_name=task_name, execution_date=date_str, task_url=task_url,
                              task_type=task_type, user_group=user_group,datacake_alert_title=datacake_alert_title)
    return res

def build_notify_content_email(state=None, task_name=None, execution_date=None, template_code='', user_group=''):
    task_type = NormalUtil.template_code_transform(template_code)
    datacake_alert_title = conf.get('email','datacake_alert_title')
    base_content = """[{datacake_alert_title} 任务状态通知]{alert_head}
            任务: {task_name} 
            执行入参时间: {execution_date}
            任务类型: {task_type} 
            用户组: {user_group}   
            <p><a href="{task_url}" target="_blank">任务实例列表页链接</a></p>   
            """
    if state == State.RETRY:
        alert_head = "任务本次执行失败，正在准备重试"
    elif state == State.START:
        alert_head = "任务开始执行"
    elif state == State.SUCCESS:
        alert_head = "任务执行成功"
    elif state == State.FAILED:
        alert_head = "任务执行失败"
    else:
        return None
    if isinstance(execution_date, datetime.datetime):
        date_str = execution_date.strftime('%Y-%m-%dT%H:%M:%S')
    elif isinstance(execution_date, six.string_types):
        date_str = execution_date
    else:
        date_str = " "
    task_url = _get_task_url(task_id=task_name)
    res = base_content.format(alert_head=alert_head, task_name=task_name, execution_date=date_str, task_url=task_url,
                              task_type=task_type, user_group=user_group,datacake_alert_title=datacake_alert_title)
    return res

def build_backfill_notify_content(task_names, upstream_tasks, start_date, end_date, connectors,operator):
    datacake_alert_title = conf.get('email', 'datacake_alert_title')
    base_content = """__[{datacake_alert_title} 提示信息]__  
    <font color='#dd0000'>注意，您负责的任务已经被上游深度补数</font><br />
    操作人: {operator}
    您负责的任务:  {task_names}  
    依赖的上游任务:  {upstream_tasks}  
    补数日期: {start_date} ~ {end_date} 
    深度补数有可能会对您的任务造成影响，您可与操作人进行确认   
            """
    if isinstance(task_names, list):
        task_names = "  \n".join(task_names)
    if isinstance(upstream_tasks, list):
        upstream_tasks = "  \n".join(upstream_tasks)
    if isinstance(connectors, list):
        connectors = "__" + "  ".join(connectors) + "__"
    if isinstance(start_date, datetime.datetime):
        start_date = start_date.strftime('%Y-%m-%dT%H:%M:%S')
    elif isinstance(start_date, six.string_types):
        pass
    else:
        raise ValueError("Bad argument passed：start_date")
    if isinstance(end_date, datetime.datetime):
        end_date = end_date.strftime('%Y-%m-%dT%H:%M:%S')
    elif isinstance(end_date, six.string_types):
        pass
    else:
        raise ValueError("Bad argument passed：end_date")
    return base_content.format(task_names=task_names, upstream_tasks=upstream_tasks,
                               start_date=start_date, end_date=end_date,
                               connectors=connectors,operator=operator,datacake_alert_title=datacake_alert_title)

def build_normal_backfill_notify_content(task_names, upstream_tasks, execution_date_list, connectors,operator):
    datacake_alert_title = conf.get('email', 'datacake_alert_title')
    base_content = """__[{datacake_alert_title} 提示信息]__  
    <font color='#dd0000'>注意，您负责任务的上游任务正在补数,请关注是否需要重跑自己的任务</font><br />
    操作人: {operator}
    您负责的任务:  {task_names}  
    依赖的上游任务:  {upstream_tasks}  
    补数日期: {execution_dates} 
            """
    if isinstance(task_names, list):
        task_names = "  \n".join(task_names)
    if isinstance(upstream_tasks, list):
        upstream_tasks = "  \n".join(upstream_tasks)
    if isinstance(connectors, list):
        connectors = "__" + "  ".join(connectors) + "__"
    execution_dates = ','.join(map(lambda x:x.strftime('%Y-%m-%dT%H:%M:%S'),execution_date_list))
    return base_content.format(task_names=task_names, upstream_tasks=upstream_tasks,
                               execution_dates=execution_dates,
                               connectors=connectors,operator=operator,datacake_alert_title=datacake_alert_title)


def build_import_sharestore_state_notify(dag_id,task_name,execution_date):
    datacake_alert_title = conf.get('email', 'datacake_alert_title')
    base_content = """__[{datacake_alert_title} 提示信息]__  
        <font color='#dd0000'>注意，您负责任务的sharestorere任务已经在队列中排队超过两个小时了,请重点关注</font><br /> \n
        任务名称:  {dag_id}  \n
        import sharestore 任务名称{task_name} \n
        任务执行时间:  {execution_date}
                """
    return base_content.format(dag_id=dag_id,task_name=task_name,execution_date=execution_date,datacake_alert_title=datacake_alert_title)

def build_timeout_notify(task_name,execution_date,duration):
    task_name = TaskDesc.get_task(task_name=task_name).ds_task_name
    datacake_alert_title = conf.get('email', 'datacake_alert_title')
    base_content = """__[{datacake_alert_title} 任务超时通知]__ \n
        任务名称:  {task_name}  \n
        执行入参时间:  {execution_date} \n
        执行耗时: {minutes}m {seconds}s\n
        [任务实例列表页链接]({task_url})
                """
    task_url = _get_task_url(task_id=task_name)
    minutes = int(duration / 60)
    seconds = int(duration % 60)
    return base_content.format(task_name=task_name,execution_date=execution_date,minutes=minutes,seconds=seconds,task_url=task_url,
                               datacake_alert_title=datacake_alert_title)


def _get_task_url(task_id=None):
    # https://datastudio.ushareit.org/#/task/detail?id=10019&name=qz_dws_svideo_shareit_report_ice_inc_daily&isOnline=1
    baseurl = conf.get("email", "ds_front_base_url")
    task_url = baseurl + "?name=" + task_id + "&isOnline=1"
    return task_url


def get_receiver_list(receiver):  # type: (Union[str, Iterable[str]]) -> List[str]
    res = None
    if isinstance(receiver, basestring):
        res = _get_receiver_list_from_str(receiver)
    elif isinstance(receiver, CollectionIterable):
        if not all(isinstance(item, basestring) for item in receiver):
            raise TypeError("The items in your iterable must be strings.")
        res = list(receiver)
    if res is not None:
        return {}.fromkeys(res).keys()
    received_type = type(receiver).__name__
    raise TypeError("Unexpected argument type: Received '{}'.".format(received_type))


def _get_receiver_list_from_str(receiver):  # type: (str) -> List[str]
    delimiters = [",", ";"]
    for delimiter in delimiters:
        if delimiter in receiver:
            return [receiver.strip() for receiver in receiver.split(delimiter)]
    return [receiver]

def send_ding_group_chat_notify(title,content,dry_run=False,access_token=None,at_mobiles=None):
    log = LoggingMixin().log
    if content is None:
        log.info("can not send ding notify because content is none")
        return
    global email_API_token
    # email_API_token = get_token()
    # baseurl = "http://prod.openapi-notify.sgt.sg2.api/notify/dingapp/send"
    baseurl = conf.get('email', 'ding_group_chat_url')
    access_token = conf.get('email','access_token') if not access_token else access_token
    datas = {
        "msgtype": "markdown",
        "markdown": {
            "title": title,
            "text": content,
        },
        "at":{
            "atMobiles":at_mobiles if at_mobiles else [],
            "isAtAll": False if at_mobiles else True
        },
        "access_token": access_token
    }

    if "email_API_token" not in locals().keys() or email_API_token is None:
        email_API_token = get_token()

    if not dry_run:
        requests.packages.urllib3.disable_warnings()
        error = None
        for i in range(4):
            try:
                headers = {
                    'Authorization': email_API_token,
                    'Content-Type': 'application/json',
                }
                resp = requests.post(baseurl, headers=headers, json=datas, verify=False)
                if resp.status_code == 200:
                    return True
                else:
                    raise AirflowException(resp.text)
            except Exception as e:
                error = e
                log.info("[email operator]: send alarm message failed %s time:%s" % (i, str(e.message)))
        if error is not None:
            raise error
        raise Exception("Failed to send alarm message for unkown error")


def send_ding_notify(title="DataCake 任务状态通知", content=None, receiver=None, dryRun=False):
    log = LoggingMixin().log
    if receiver is None or (isinstance(receiver, list) and len(receiver) == 0):
        log.info("can not send ding notify because invalid receiver")
        return
    if content is None:
        log.info("can not send ding notify because content is none")
        return
    global email_API_token
    baseurl = conf.get('email', 'ding_notify_url')

    datas = {
        "msgtype": "markdown",
        "markdown": {
            "title": title,
            "text": content,
        },
        "receiver": receiver
    }

    if "email_API_token" not in locals().keys() or email_API_token is None:
        email_API_token = get_token()

    if not dryRun:
        requests.packages.urllib3.disable_warnings()
        error = None
        for i in range(4):
            try:
                headers = {
                    'Authorization': email_API_token,
                    'Content-Type': 'application/json',
                }
                resp = requests.post(baseurl, headers=headers, json=datas, verify=False)
                if resp.status_code == 200:
                    log.debug("send notify to {name}".format(name=str(receiver)))
                    return True
                elif "invalid authorization token" in resp.text:
                    email_API_token = get_token()
                elif resp.status_code == 400:
                    tmp_dict = resp.json()
                    if tmp_dict["code"] == 9:
                        # 有 shareid 不存在
                        log.debug("send notify to {name}".format(name=str(receiver)))
                        log.debug("some shareid is invalid")
                        return True
                    else:
                        raise AirflowException(resp.text)
                else:
                    raise AirflowException(resp.text)
            except Exception as e:
                error = e
                log.info("[email operator]: send alarm message failed %s time:%s" % (i, str(e.message)))
        if error is not None:
            raise error
        raise Exception("Failed to send alarm message for unkown error")


def send_ding_action_card(title=None, content=None, url_list=None, receiver=None, dryRun=False):
    log = LoggingMixin().log
    global email_API_token
    # email_API_token = get_token()
    # baseurl = "http://prod.openapi-notify.sgt.sg2.api/notify/dingapp/send"
    baseurl = conf.get('email', 'ding_notify_url')
    if "email_API_token" not in locals().keys() or email_API_token is None:
        email_API_token = get_token()
    datas = {
        "msgtype": "action_card",
        "action_card": {
            "title": title,
            "markdown":
                content
            ,
            "btn_orientation": "1",
            "btn_json_list": url_list,
        },
        "receiver": receiver
    }
    headers = {
        'Authorization': email_API_token,
        'Content-Type': 'application/json',
    }

    if not dryRun:
        requests.packages.urllib3.disable_warnings()
        error = None
        for i in range(4):
            try:
                resp = requests.post(baseurl, headers=headers, json=datas, verify=False)
                if resp.status_code == 200:
                    log.debug("send notify to {name}".format(name=str(receiver)))
                    return True
                elif "invalid authorization token" in resp.text:
                    email_API_token = get_token()
                elif resp.status_code == 400:
                    tmp_dict = resp.json()
                    if tmp_dict["code"] == 9:
                        # 有 shareid 不存在
                        log.debug("send notify to {name}".format(name=str(receiver)))
                        log.debug("some shareid is invalid")
                        return True
                    else:
                        raise AirflowException(resp.text)
                else:
                    raise AirflowException(resp.text)
            except Exception as e:
                error = e
                log.info("[email operator]: send alarm message failed %s time:%s" % (i, str(e.message)))
        if error is not None:
            raise error
        raise Exception("Failed to send alarm message for unkown error")


def build_backfill_handle_content(operator,core_task,start_date,end_date):
    """
    构造任务补数进度查询 handle 通知
    """
    datacake_alert_title = conf.get('email', 'datacake_alert_title')
    from airflow.utils.timezone import utcnow
    content = """__[{datacake_alert_title} 任务补数进度查询]__  
    操作人SHAREid: {operator}  
    操作类型: 补数  
    触发任务: {core_task}    
    触发时间: {date_now}   
    补数起止时间: {start_date}~{end_date}  
    """
    if isinstance(start_date, datetime.datetime):
        start_date = start_date.strftime('%Y-%m-%dT%H:%M:%S')
    if isinstance(end_date, datetime.datetime):
        end_date = end_date.strftime('%Y-%m-%dT%H:%M:%S')
    date_now = utcnow().strftime('%Y-%m-%dT%H:%M:%S')

    task_name = TaskDesc.get_task(task_name=core_task).ds_task_name
    return content.format(date_now=date_now,core_task=task_name,
                          start_date=start_date,end_date=end_date,
                          operator=operator,datacake_alert_title=datacake_alert_title)


def build_backfill_process_reminder_content(instance_info_list):
    """
    [{"dag_id":"","execution_date":"","state":""}]
    """
    datacake_alert_title = conf.get('email', 'datacake_alert_title')
    from airflow.shareit.utils.state import State as myState
    content = """__[{datacake_alert_title}}] 回算任务进度__  
    {instance_info}  
    """
    task_base_str = """__任务: {task_name}__  \n"""
    item_base_str = """入参时间: {time} {state}"""
    tasks = {}
    for ins in instance_info_list:
        exe_date = ins["execution_date"].strftime('%Y-%m-%dT%H:%M:%S') if isinstance(ins["execution_date"],
                                                                                     datetime.datetime) else ins[
            "execution_date"]
        state = myState.task_ins_state_map(ins["state"])
        tmp = item_base_str.format(time=exe_date,state=state.encode("utf-8"))
        if ins["dag_id"] in tasks.keys():
            tasks[ins["dag_id"]].append(tmp)
        else:
            tasks[ins["dag_id"]] = [tmp]
    instance_info = ""
    for task_name in tasks:
        task_info = task_base_str.format(task_name=task_name)
        tasks[task_name].sort()
        task_info += "  \n".join(tasks[task_name])
        task_info += "  \n"
        instance_info += task_info
    # inl = [i["dag_id"] + "  \n" + i["execution_date"] + "  " + i["state"] for i in instance_info_list]
    # instance_info = "  \n".join(inl)
    return content.format(instance_info=instance_info,datacake_alert_title=datacake_alert_title)


def build_backfill_process_reminder_url_list(base_url,user_action_id):
    return [
        {
            "title": "查看最新状态",
            "action_url": base_url+"?userActionId="+str(user_action_id)
        }]


def send_email_by_notifyAPI(title="DataCake 任务状态通知", content=None, receiver=None):
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText


    password = conf.get("smtp","smtp_password")
    msg = MIMEMultipart('related')
    msg['From'] = conf.get("smtp","smtp_mail_from")
    msg['Subject'] =  title

    html_text = u"<html><body style='font-family:Times New Roman'>%s</body></html>" % content
    msg_text = MIMEText(html_text, 'html', 'utf-8')
    msg.attach(msg_text)
    smtp_server = conf.get("smtp","smtp_host")
    smtp_port = conf.get("smtp","smtp_port").strip()
    server = smtplib.SMTP_SSL(smtp_server, int(smtp_port))
    server.login(msg['From'], password)
    server.sendmail(msg['From'], receiver, msg.as_string())


def send_telephone_list(receivers,dag_id=None,execution_date=None,status=None,alert_head=None):
    baseurl = conf.get('phone', 'status_url')
    log = LoggingMixin().log
    headers = {
        'Authorization': get_token()
    }
    ra = RegularAlert()
    ra.sync_regular_alert_to_db(
        task_name=dag_id,
        execution_date=execution_date,
        trigger_condition=status,
        state=State.SUCCESS,
        is_regular=False
    )
    for i in range(2):
        for name in receivers:
            send_telephone_by_notifyAPI([name], alert_head)
            time.sleep(60)
            log.info("[telephone notify] send user:{receivers} message: {phone_result_message}"
                     .format(receivers=receivers,phone_result_message=phone_result_message))
            if phone_result_message:
                session_id = phone_result_message["details"]["sessionIdList"][0]
                res = requests.get(baseurl +"/"+ session_id,headers=headers)
                log.info("[get notify status] message: {}".format(res.json()))
                if "answer" in res.json()["details"]["status"]:
                    log.info("send telephone notify success")
                    return

def send_telephone_by_notifyAPI(receivers,alert_head=None):
    # baseurl = "http://prod.openapi-notify.sgt.sg2.api/notify/telephone/send"
    global phone_result_message
    baseurl = conf.get('phone', 'alert_url')
    phone_API_token = get_token()

    if alert_head:
        datas = {"receiver": receivers,
                 "template_id": "0bf9c563efa34331be75fd7e26b4cd75",
                 "template_params": [alert_head]
                 }
    else:
        datas = {"receiver": receivers}

    for i in range(4):
        try:
            headers = {
                'Authorization': phone_API_token,
                'Content-Type': 'application/json',
            }
            requests.packages.urllib3.disable_warnings()
            resp = requests.post(baseurl, headers=headers, json=datas, verify=False)
            if resp.status_code == 200:
                phone_result_message = resp.json()
                return True
            elif "invalid authorization token" in resp.text:
                phone_API_token = get_token()
            else:
                raise ValueError(resp.text)
        except Exception as e:
            error = e
            print("[telephone operator]: send telephone failed %s time:%s" % (i, str(e.message)))
    if error:
        raise error
    raise Exception("send telephone failed for unkown error")

def send_notify_by_alert_type(task_name,execution_date,alert_type,owners,alert_head):
    from airflow.shareit.hooks.message.wechat_hook import WechatHook
    log = LoggingMixin().log
    notify_type = conf.get('email', 'notify_type')
    datacake_alert_title = conf.get('email', 'datacake_alert_title')
    task = TaskDesc.get_task(task_name=task_name)
    task_name = task.ds_task_name
    user_group = task.user_group.get('currentGroup')
    template_code = task.template_code
    task_type = NormalUtil.template_code_transform(template_code)
    task_url = _get_task_url(task_id=task_name)
    content = """[{datacake_alert_title} 定时告警通知]{alert_head} \n
任务: {task_name} \n
执行入参时间: {execution_date} \n
任务类型: {task_type} \n
用户组: {user_group} \n
[任务实例列表页链接]({task_url}) \n"""\
        .format(alert_head=alert_head,task_name=task_name,execution_date=execution_date,task_url=task_url,
                task_type=task_type,user_group=user_group,datacake_alert_title=datacake_alert_title)

    email_content = """[{datacake_alert_title} 定时告警通知]{alert_head} \n
任务: {task_name} \n
执行入参时间: {execution_date} \n
任务类型: {task_type} \n
用户组: {user_group} \n
<p><a href="{task_url}" target="_blank">任务实例列表页链接</a></p>""" \
        .format(alert_head=alert_head, task_name=task_name, execution_date=execution_date, task_url=task_url,
                task_type=task_type,user_group=user_group,datacake_alert_title=datacake_alert_title)


    if "dingTalk" in alert_type and "dingding" in notify_type:
        try:
            send_ding_notify(content=content,receiver=get_receiver_list(owners))
        except Exception as e:
            logging.error("dingding send failed")
            raise e
    if "email" in alert_type and "email" in notify_type:
        try:
            alert_model = json.loads(task.extra_param['regularAlert'])
            reciver_list = [i.get('email', '') for i in alert_model.get('emailReceivers', [])]
            send_email_by_notifyAPI(title="DataCake 定时告警通知",content=email_content.decode('utf-8').replace('\n', '<br>'),receiver=reciver_list)
        except Exception as e:
            log.error("email send failed")
            raise e
    if "phone" in alert_type and "phone" in notify_type:
        try:
            send_telephone_by_notifyAPI(receivers=get_receiver_list(owners),alert_head=alert_head)
        except Exception as e:
            log.error("phone send failed")
            raise e
    if "wechat" in alert_type and "wechat" in notify_type:
        try:
            alert_model = json.loads(task.extra_param['regularAlert'])
            robot_key = alert_model.get('wechatRobotKey', '')
            reciver_list = [i.get('wechatId', '') for i in alert_model.get('wechatReceivers', [])]
            WechatHook(robot_key=robot_key).send_markdown(content,mentioned_list=reciver_list)
        except Exception as e:
            log.error("wechat send failed")
            raise e



def build_alert_content(task_name,execution_date):
    datacake_alert_title = conf.get('email', 'datacake_alert_title')
    execution_date = execution_date.strftime('%Y-%m-%d %H:%M:%S.%f')
    base_content = """__[{datacake_alert_title} 任务运行时间超20分钟通知]__  
__任务: {task_name}__  
__执行入参时间: {execution_date}__    
任务执行时间已超过20分钟，请确认任务状态是否正常""".format(task_name=task_name,execution_date=execution_date,datacake_alert_title=datacake_alert_title)
    return base_content

def build_callback_failed_content(task_name,taskinstance_id):
    datacake_alert_title = conf.get('email', 'datacake_alert_title')
    base_content = """__[{datacake_alert_title} 回调失败通知]__  
__任务: {task_name}__  
__实例ID: {taskinstance_id}__    
回调10次仍然失败""".format(task_name=task_name,taskinstance_id=taskinstance_id,datacake_alert_title=datacake_alert_title)
    return base_content


def ding_recovery_state(task_name,batch_id,state,app_state,end_time,ti_state):
    title = '任务状态异常'
    base_content = """__[DataCake 调度报警]__  
        <font color='#dd0000'>{title}</font><br />
        -  
        任务状态异常，与gateway不一致，进行自动恢复： 
        任务名：{task_name}  
        实例id：{batch_id}  
        任务当前状态：{ti_state}  
        gateway状态：{state}  
        gatewayApp状态：{app_state}  
        gateway结束时间：{end_time}  
                """
    content = base_content.format(title=title,task_name=task_name,batch_id=batch_id,state=state,app_state=app_state,end_time=end_time,ti_state=ti_state)
    access_token = conf.get('email', 'access_token_p3')
    ding_mobile_str = conf.get('email','ding_mobiles')
    send_ding_group_chat_notify(title=title, content=content,access_token=access_token,at_mobiles=ding_mobile_str.split(','))

def build_new_backfill_notify_content(task_name, tis,operator):
    datacake_alert_title = conf.get('email', 'datacake_alert_title')
    base_content = """[{datacake_alert_title} 补数通知]  
    <font color='#dd0000'>注意，您负责的任务已经被上游深度补数</font><br />
    操作人: {operator}
    上游任务:  {task_name}  
    
    您的补数任务及日期:  
    {tis} 
    深度补数有可能会对您的任务造成影响，您可与操作人进行确认。
    """
    return base_content.format(task_name=task_name, operator=operator, tis=tis,
                               datacake_alert_title=datacake_alert_title)
