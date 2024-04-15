# -*- coding: utf-8 -*-
import json

import dill
from airflow.configuration import conf
from sqlalchemy import Column

from airflow import LoggingMixin
from airflow.models import Base, ID_LEN
from sqlalchemy import Column, Float, Index, Integer, PickleType, String, func, DateTime

from airflow.shareit.constant.taskInstance import MANUALLY
from airflow.utils.db import provide_session
from airflow.utils.sqlalchemy import UtcDateTime
from airflow.utils.state import State
from airflow.utils.timezone import utcnow, beijing


class TaskInstanceLog(Base, LoggingMixin):
    """
    taskinstance的记录表
    """

    __tablename__ = "task_instance_log"
    id = Column(Integer, primary_key=True)
    task_id = Column(String(ID_LEN))
    dag_id = Column(String(ID_LEN))
    execution_date = Column(UtcDateTime)
    start_date = Column(UtcDateTime)
    end_date = Column(UtcDateTime)
    duration = Column(Float)
    state = Column(String(20))
    _try_number = Column('try_number', Integer, default=0)
    external_conf = Column(String(500))
    task_type = Column(String(50), default='pipeline')  # backCalculation,pipeline
    operator = Column(String(1000))
    create_date = Column(DateTime, server_default=func.now())
    update_date = Column(DateTime, server_default=func.now(), onupdate=func.now())
    backfill_label = Column(String(1000))

    def __init__(self,task_instance):
        self.task_id = task_instance.task_id
        self.dag_id = task_instance.dag_id
        self.execution_date = task_instance.execution_date
        self.start_date = task_instance.start_date
        self.end_date = task_instance.end_date
        self.duration = task_instance.duration
        self._try_number = task_instance.get_try_number()
        self.task_type = task_instance.task_type
        self.external_conf = task_instance.external_conf
        self.state = State.FAILED if task_instance.state == State.UP_FOR_RETRY else task_instance.state
        self.operator = task_instance.operator
        self.backfill_label = task_instance.backfill_label

    def get_try_number(self):
        return self._try_number

    @staticmethod
    @provide_session
    def find_by_execution_date(task_name, execution_date, session=None):
        qry = session.query(TaskInstanceLog).filter(TaskInstanceLog.dag_id == task_name,
                                                    TaskInstanceLog.task_id == task_name,
                                                    TaskInstanceLog.execution_date == execution_date)
        res = qry.order_by(TaskInstanceLog.start_date).all()
        return res

    @property
    def try_number(self):
        # This is designed so that task logs end up in the right file.
        if self.state == State.RUNNING:
            return self._try_number
        return self._try_number + 1

    def get_external_conf(self):
        try:
            res = json.loads(self.external_conf) if self.external_conf else None
        except Exception as e:
            res = self.external_conf
        return res

    @staticmethod
    def update_name(old_name,new_name,transaction=False,session=None):
        task_instances = session.query(TaskInstanceLog).filter(TaskInstanceLog.dag_id == old_name).all()
        for t1 in task_instances:
            t1.dag_id = new_name
            if t1.task_id == old_name:
                t1.task_id = new_name
            session.merge(t1)
        if not transaction:
            session.commit()

    def get_duration(self):
        if self.start_date is None:
            return 0
        end_date = self.end_date if self.end_date is not None else utcnow()
        return (end_date.replace(tzinfo=beijing) - self.start_date.replace(tzinfo=beijing)).total_seconds()

    def get_genie_info(self):
        genie_job_id = ''
        genie_job_url = ''
        ex_conf = self.get_external_conf()
        if ex_conf:
            if 'batch_id' in ex_conf.keys():
                try:
                    genie_job_id = ex_conf['batch_id']
                    app_url = ex_conf.get('app_url','')
                    # if ex_conf.get('is_spark',False) and app_url:
                    #     genie_job_url = app_url
                    # else :
                    # log_web_host = conf.get("job_log", "log_web_host")
                    log_cloud_home = conf.get('job_log', 'log_cloud_file_location').strip()
                    genie_job_url = '{host}/BDP/logs/pipeline_log/{time}/{batch_id}.txt'.format(
                        host=log_cloud_home,
                        # dag_id=self.dag_id,
                        time=self.start_date.strftime('%Y%m%d'),
                        batch_id=genie_job_id
                    )
                except Exception:
                    pass
        return genie_job_id, genie_job_url


    @staticmethod
    @provide_session
    def check_is_exists(dag_id,task_id,execution_date,try_number,session=None):
        qry = session.query(TaskInstanceLog).filter(TaskInstanceLog.dag_id==dag_id,TaskInstanceLog.task_id==task_id)
        qry = qry.filter(TaskInstanceLog.execution_date == execution_date)
        qry = qry.filter(TaskInstanceLog._try_number == try_number)
        results = qry.all()
        if results and len(results) > 0:
            return True
        return False

    def get_version(self):
        ex_conf = self.get_external_conf()
        if ex_conf:
            try:
                for k in ex_conf:
                    if k == "version":
                        return ex_conf["version"]
            except Exception:
                pass
        return -1

    def is_kyuubi_job(self):
        ex_conf = self.get_external_conf()
        if ex_conf:
            if 'is_kyuubi' in ex_conf.keys():
                return True
        return False

    @staticmethod
    @provide_session
    def find_one_hour_ti(start, session=None):
        result = []
        offset = 0
        size = 1000
        while offset < 5000 :
            tis = session.query(TaskInstanceLog).order_by(TaskInstanceLog.id.desc()).limit(size).offset(offset).all()
            result += tis
            if tis[len(tis) - 1].start_date > start:
                offset += size
            else:
                break
        return [ti for ti in result if ti.start_date > start]



    def get_spark_webui(self,url_dict):
        ex_conf = self.get_external_conf()
        if ex_conf:
            if 'app_id' in ex_conf.keys() and 'app_url' in ex_conf.keys():
                if self.state == State.RUNNING:
                    # return ex_conf['app_url']
                    pass
                else:
                    region = ex_conf['region']
                    if region in url_dict.keys():
                        return '{}/{}/jobs'.format(url_dict[region],ex_conf['app_id'])
                    else:
                        return ex_conf['app_url']
        return ''