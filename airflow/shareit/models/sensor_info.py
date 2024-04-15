# -*- coding: utf-8 -*-
"""
author: zhangtao
Copyright (c) 2021/8/5 Shareit.com Co., Ltd. All Rights Reserved.
"""
from airflow.utils.timezone import make_naive, utcnow, my_make_naive
from sqlalchemy import Column, String, Integer, DateTime
from airflow.models.base import Base, ID_LEN, OPTION_LEN, MESSAGE_LEN
from airflow.utils.db import provide_session
from airflow.shareit.utils.state import State


class SensorInfo(Base):
    __tablename__ = "sensor_info"

    id = Column(Integer, primary_key=True)
    dag_id = Column(String(ID_LEN))
    metadata_id = Column(String(ID_LEN))
    execution_date = Column(DateTime)
    detailed_execution_date = Column(DateTime)
    granularity = Column(String(OPTION_LEN))
    offset = Column(Integer)
    check_path = Column(String(MESSAGE_LEN))
    timeout_seconds = Column(Integer)
    poke_interval = Column(Integer)  # seconds
    state = Column(String(OPTION_LEN))
    cluster_tags = Column(String(ID_LEN))
    last_check = Column(DateTime)
    first_check = Column(DateTime)
    sensor_type = Column(String(OPTION_LEN))
    completion_time = Column(DateTime)

    @staticmethod
    @provide_session
    def register_sensor(dag_id=None, metadata_id=None, execution_date=None, detailed_execution_date=None,
                        granularity=None, offset=None, check_path=None, timeout_seconds=None,
                        poke_interval=None, cluster_tags=None,back_fill=False,completion_time=None,transaction=False,session=None):
        qry = session.query(SensorInfo)
        if dag_id and metadata_id and execution_date and granularity and cluster_tags and detailed_execution_date:
            SI = qry.filter(SensorInfo.dag_id == dag_id,
                            SensorInfo.metadata_id == metadata_id,
                            SensorInfo.execution_date == execution_date,
                            SensorInfo.detailed_execution_date == detailed_execution_date,
                            SensorInfo.granularity == granularity).one_or_none()
            timeout_seconds = SensorInfo._get_timeout_seconds(
                granularity=granularity) if timeout_seconds is None else timeout_seconds
            poke_interval = 300 if poke_interval is None else poke_interval
            if SI is not None:
                if back_fill:
                    SI.check_path = check_path
                    SI.cluster_tags = cluster_tags
                    SI.state = State.CHECKING
                    SI.first_check = make_naive(utcnow())
                else:
                    SI.check_path = check_path
                    SI.cluster_tags = cluster_tags
                    SI.detailed_execution_date = detailed_execution_date
            else:
                SI = SensorInfo()
                SI.dag_id = dag_id,
                SI.metadata_id = metadata_id,
                SI.execution_date = execution_date,
                SI.detailed_execution_date = detailed_execution_date
                SI.granularity = granularity
                SI.offset = offset
                SI.check_path = check_path
                SI.cluster_tags = cluster_tags
                SI.state = State.CHECKING
            if completion_time:
                SI.completion_time = completion_time
            if check_path:
                SI.check_path = check_path
            if timeout_seconds != SI.timeout_seconds:
                SI.timeout_seconds = timeout_seconds
            if poke_interval != SI.poke_interval:
                SI.poke_interval = poke_interval
            if check_path:
                SI.sensor_type = State.CHECK_PATH
            else:
                SI.sensor_type = State.READY_TIME
                SI.poke_interval = 5
            session.merge(SI)
            if not transaction:
                session.commit()

    @staticmethod
    @provide_session
    def get_checking_info(session=None,dag_id=None,sensor_type=None):
        qry = session.query(SensorInfo).filter(SensorInfo.state == State.CHECKING)
        if dag_id:
            qry = qry.filter(SensorInfo.dag_id == dag_id)
        if sensor_type:
            qry = qry.filter(SensorInfo.sensor_type == sensor_type)
        return qry.all()

    @staticmethod
    @provide_session
    def get_latest_sensor_data(metadata_id=None, session=None):
        res = session.query(SensorInfo)\
                .filter(SensorInfo.metadata_id == metadata_id)\
                .order_by(SensorInfo.execution_date.desc()).first()
        return res


    @staticmethod
    @provide_session
    def get_external_state(dag_id, metadata_id, detailed_execution_date, session=None):
        qry = session.query(SensorInfo).with_entities(SensorInfo.state).\
            filter(SensorInfo.dag_id == dag_id, SensorInfo.metadata_id == metadata_id,
                   SensorInfo.detailed_execution_date == detailed_execution_date)
        states = qry.order_by(SensorInfo.execution_date.desc()).first()
        state = State.SUCCESS
        if states[0] != State.SUCCESS:
            state = State.WAITING
        return state

    @provide_session
    def update_state(self, state, session=None):
        if self.state != state:
            self.state = state
            session.merge(self)
            session.commit()

    @provide_session
    def update_checked_time(self, session=None):
        if self.first_check is None:
            self.first_check = utcnow()
        self.last_check = utcnow()
        session.merge(self)
        session.commit()

    @staticmethod
    @provide_session
    def update_si_state(metadata_id, execution_date, granularity, offset, state, detailed_execution_date,
                        dag_id=None,
                        session=None,
                        commit=True):
        si = session.query(SensorInfo).filter(SensorInfo.metadata_id == metadata_id,
                                              SensorInfo.execution_date == execution_date,
                                              SensorInfo.detailed_execution_date == detailed_execution_date,
                                              SensorInfo.granularity == granularity,
                                              SensorInfo.offset == offset)
        if dag_id is not None:
            si = si.filter(SensorInfo.dag_id == dag_id)
        res = si.all()
        for sensor_info in res:
            sensor_info.state = state if sensor_info.state != state else sensor_info.state
            session.merge(sensor_info)
        if commit:
            session.commit()

    @staticmethod
    @provide_session
    def update_state_by_dag_id(dag_id,execution_date,state,session=None,transaction=False):
        sis = session.query(SensorInfo).filter(SensorInfo.dag_id == dag_id,
                                              SensorInfo.execution_date == execution_date).all()
        for si in sis:
            si.state = state
            session.merge(si)
        if not transaction:
            session.commit()


    @staticmethod
    @provide_session
    def update_si_checked_time(dag_id, metadata_id, execution_date, granularity, offset, detailed_execution_date,
                               session=None):
        si = session.query(SensorInfo).filter(SensorInfo.dag_id == dag_id,
                                              SensorInfo.metadata_id == metadata_id,
                                              SensorInfo.execution_date == execution_date,
                                              SensorInfo.detailed_execution_date == detailed_execution_date,
                                              SensorInfo.granularity == granularity,
                                              SensorInfo.offset == offset).one_or_none()
        if si is not None:
            if si.first_check is None:
                si.first_check = make_naive(utcnow())
            si.last_check = make_naive(utcnow())
            session.merge(si)
            session.commit()

    @staticmethod
    @provide_session
    def get_all_si(session=None):
        return session.query(SensorInfo).all()

    @staticmethod
    @provide_session
    def find(dag_id=None,metadata_id=None,execution_date=None,detailed_execution_date=None,state=None,session=None):
        qry = session.query(SensorInfo)
        if dag_id:
            qry = qry.filter(SensorInfo.dag_id == dag_id)
        if metadata_id:
            qry = qry.filter(SensorInfo.metadata_id == metadata_id)
        if execution_date:
            qry = qry.filter(SensorInfo.execution_date == execution_date)
        if detailed_execution_date:
            qry = qry.filter(SensorInfo.detailed_execution_date == detailed_execution_date)
        if state:
            qry = qry.filter(SensorInfo.state == state)
        return qry.all()

    @staticmethod
    @provide_session
    def get_detailed_sensors_dict(dag_id=None,metadata_id=None,execution_date=None,detailed_execution_date=None,state=None,session=None):
        '''
        dict :
            key: 日期 %Y%m%d %H%M%S
            value: sensor

        '''
        dict = {}
        siInfos = SensorInfo.find(dag_id=dag_id,metadata_id=metadata_id,execution_date=execution_date,detailed_execution_date=detailed_execution_date,state=state,session=session)
        if len(siInfos) > 0:
            for si in siInfos:
                if si.detailed_execution_date is None or si.detailed_execution_date == '':
                    continue
                dict[si.detailed_execution_date.strftime('%Y%m%d %H%M%S')] = si
        return dict

    @staticmethod
    @provide_session
    def reopen_sensor(dag_id,execution_date,state=State.FAILED,transaction=False,session=None):
        qry = session.query(SensorInfo)
        qry = qry.filter(SensorInfo.dag_id == dag_id).filter(SensorInfo.execution_date == execution_date).filter(SensorInfo.state == state)
        sensors = qry.all()
        if sensors is not None and len(sensors) > 0:
            for si in sensors:
                si.state = State.CHECKING
                si.first_check = make_naive(utcnow())
                session.merge(si)

        if not transaction :
            session.commit()


    @classmethod
    def _get_timeout_seconds(cls, granularity):
        from airflow.shareit.utils.granularity import Granularity
        gra = Granularity.getIntGra(granularity)
        if gra == 1:
            return 86400 * 30
        elif gra == 2:
            return 86400 * 30
        elif gra == 3:
            return 86400 * 30
        elif gra == 4:
            return 86400 * 7
        elif gra == 5:
            return 86400
        elif gra == 6:
            return 86400

    @provide_session
    def update_poke(self, new_poke_interval, commit=True, session=None):
        if new_poke_interval > 300:
            self.poke_interval = new_poke_interval
            session.merge(self)
        if commit:
            session.commit()

    @staticmethod
    @provide_session
    def get_latest_sensor_info(dag_id=None,metadata_id=None, session=None):
        res = session.query(SensorInfo) \
            .filter(SensorInfo.metadata_id == metadata_id,
                    SensorInfo.dag_id == dag_id) \
            .order_by(SensorInfo.execution_date.desc()).first()
        return res

    def __hash__(self):
        return hash(str(self.metadata_id) + str(self.execution_date)
                    + str(self.detailed_execution_date) + str(self.offset)
                    + str(self.check_path) + str(self.cluster_tags)
                    + str(self.granularity))

    def __eq__(self, other):
        if self.metadata_id == other.metadata_id \
                and self.execution_date == other.execution_date \
                and self.detailed_execution_date == other.detailed_execution_date \
                and self.offset == other.offset \
                and self.check_path == other.check_path \
                and self.cluster_tags == other.cluster_tags \
                and self.granularity == other.granularity:
            return True
        else:
            return False

    @staticmethod
    @provide_session
    def update_dag_id(old_task_name,new_task_name,transaction=False,session=None):
        sensors = session.query(SensorInfo).filter(SensorInfo.dag_id == old_task_name).all()
        for sen in sensors :
            sen.dag_id = new_task_name
            session.merge(sen)
        if not transaction:
            session.commit()

    @staticmethod
    @provide_session
    def delete_by_execution_date(task_name, execution_date,transaction=False,session=None):
        qry = session.query(SensorInfo) \
            .filter(SensorInfo.dag_id == task_name) \
            .filter(SensorInfo.execution_date == execution_date)
        qry.delete()
        if not transaction:
            session.commit()

    @staticmethod
    @provide_session
    def close_sensors(task_name,execution_date,session=None):
        sensors = session.query(SensorInfo) \
            .filter(SensorInfo.dag_id == task_name) \
            .filter(SensorInfo.execution_date == my_make_naive(execution_date)).all()
        for s in sensors:
            s.state = State.FAILED
            session.merge(s)
        session.commit()
