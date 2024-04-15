# -*- coding: utf-8 -*-
"""
Author: zhangtao
CreateDate: 2021/4/15
"""
from datetime import datetime

from sqlalchemy import Column, String, Integer, DateTime, Boolean, func, INTEGER

from airflow.models.base import Base, ID_LEN, OPTION_LEN
from airflow.shareit.constant.trigger import TRI_FORCE
from airflow.utils.db import provide_session

from airflow.shareit.utils.state import State
from airflow.utils.timezone import my_make_naive as make_naive


class Trigger(Base):
    __tablename__ = "trigger"

    id = Column(Integer, primary_key=True)
    dag_id = Column(String(ID_LEN))
    trigger_type = Column(String(OPTION_LEN))
    execution_date = Column(DateTime)
    state = Column(String(OPTION_LEN))
    lock_id = Column(String(OPTION_LEN))
    editor = Column(String(OPTION_LEN))
    edit_date = Column(DateTime, index=True, default=datetime.now)
    is_executed = Column(Boolean, default=False)
    create_date = Column(DateTime, server_default=func.now())
    update_date = Column(DateTime, server_default=func.now(), onupdate=func.now())
    priority = Column(Integer,default=1)
    ignore_check = Column(Boolean)
    start_check_time = Column(DateTime)
    backfill_label = Column(String)

    # trigger_type:backCalculation,clear_backCalculation, pipeline manual
    @staticmethod
    @provide_session
    def find(trigger_id=None, dag_id=None, trigger_type=None, execution_date=None, state=None,
             lock_id=None, editor=None, edit_date=None, session=None):
        qry = session.query(Trigger)
        if trigger_id:
            qry = qry.filter(Trigger.id == trigger_id)
        if dag_id:
            qry = qry.filter(Trigger.dag_id == dag_id)
        if trigger_type:
            qry = qry.filter(Trigger.trigger_type == trigger_type)
        if execution_date:
            qry = qry.filter(Trigger.execution_date == execution_date)
        if state:
            qry = qry.filter(Trigger.state == state)
        if lock_id:
            qry = qry.filter(Trigger.lock_id == lock_id)
        if editor:
            qry = qry.filter(Trigger.editor == editor)
        if edit_date:
            qry = qry.filter(Trigger.edit_date == edit_date)
        return qry.all()

    @staticmethod
    @provide_session
    def find_lastest_trigger(dag_id=None,start_date=None, end_date=None, session=None):
        qry = session.query(Trigger)
        if dag_id  is None and end_date is None and start_date is None:
            return None
        if dag_id is not None:
            qry = qry.filter(Trigger.dag_id == dag_id)

        if start_date is not None:
            qry = qry.filter(Trigger.execution_date >= make_naive(start_date))
        if end_date is not None:
            qry = qry.filter(Trigger.execution_date <= make_naive(end_date))
            
        return qry.order_by(Trigger.update_date.desc()).first()

    @staticmethod
    @provide_session
    def sync_trigger_to_db(dag_id=None, trigger_type=None, execution_date=None, state=None, ignore_check=False,
                           lock_id=None, editor=None, priority=None, transaction=False, is_executed=None,
                           start_check_time=None, backfill_label=None, session=None):
        qry = session.query(Trigger)
        if dag_id:
            qry = qry.filter(Trigger.dag_id == dag_id)
        if execution_date:
            qry = qry.filter(Trigger.execution_date == execution_date)
        tri = qry.one_or_none()
        if tri:
            if trigger_type in ["backCalculation","clear_backCalculation"]:
                tri.trigger_type = trigger_type
                tri.state = state
                tri.edit_date = datetime.utcnow()

            # 补数任务不需要提升优先级
            if trigger_type != 'backCalculation' and priority is not None:
                tri.priority = priority

            if state == State.TASK_STOP_SCHEDULER:
                tri.state = state
            if is_executed:
                tri.is_executed = is_executed

            if start_check_time:
                tri.start_check_time = make_naive(start_check_time)

            if backfill_label:
                tri.backfill_label = backfill_label
            tri.ignore_check = ignore_check
        else:
            tri = Trigger()
            tri.dag_id = dag_id
            tri.trigger_type = trigger_type
            tri.execution_date = execution_date
            tri.state = state
            tri.priority = priority
            tri.ignore_check = ignore_check
            if lock_id:
                tri.lock_id = lock_id
            if editor:
                tri.editor = editor
            if is_executed:
                tri.is_executed = is_executed
            if start_check_time:
                tri.start_check_time = make_naive(start_check_time)
            tri.edit_date = datetime.utcnow()
            tri.backfill_label = backfill_label
        session.merge(tri)
        if not transaction:
            session.commit()

    @staticmethod
    @provide_session
    def sync_backfill_trigger_to_db(dag_id=None, trigger_type=None, execution_date=None, state=None,
                                    lock_id=None, editor=None, session=None):
        qry = session.query(Trigger)
        if dag_id:
            qry = qry.filter(Trigger.dag_id == dag_id)
        if execution_date:
            qry = qry.filter(Trigger.execution_date == execution_date)
        if trigger_type:
            qry = qry.filter(Trigger.trigger_type == trigger_type)
        tri = qry.one_or_none()
        if tri:
            if tri.state != state:
                tri.state = state
            tri.edit_date = datetime.utcnow()
        else:
            tri = Trigger()
            tri.dag_id = dag_id
            tri.trigger_type = trigger_type
            tri.execution_date = execution_date
            tri.state = state
            if lock_id:
                tri.lock_id = lock_id
            if editor:
                tri.editor = editor
            tri.edit_date = datetime.utcnow()
        session.merge(tri)
        session.commit()

    @staticmethod
    @provide_session
    def sync_trigger(dag_id=None, trigger_type=None, execution_date=None, state=None,
                           lock_id=None, editor=None, is_executed=None , priority=None,transaction=False ,session=None):
        qry = session.query(Trigger)
        if dag_id:
            qry = qry.filter(Trigger.dag_id == dag_id)
        if execution_date:
            qry = qry.filter(Trigger.execution_date == execution_date)
        tri = qry.one_or_none()
        if tri:
            if trigger_type is not None:
                tri.trigger_type = trigger_type
            if tri.state != state and state in State.trigger_states:
                tri.state = state
            if lock_id is not None:
                tri.lock_id = lock_id
            if editor is not None:
                tri.editor = editor
            if is_executed is not None:
                tri.is_executed = is_executed
            if priority is not None:
                tri.priority = priority
        else:
            return
        session.merge(tri)
        if not transaction:
            session.commit()

    @staticmethod
    @provide_session
    def get_active_triggers(session=None):
        qry = session.query(Trigger).filter(Trigger.state.notin_(State.finished()))
        return qry.all()

    @staticmethod
    @provide_session
    def get_untreated_triggers(dag_id=None,session=None):
        qry = session.query(Trigger).filter(Trigger.state == State.WAITING)
        if dag_id:
            qry = qry.filter(Trigger.dag_id == dag_id)
        return qry.all()

    @staticmethod
    @provide_session
    def get_untreated_data_triggers(session=None):
        qry = session.query(Trigger).filter(Trigger.state == State.WAITING).filter(Trigger.trigger_type != 'cronTrigger')
        return qry.all()

    @staticmethod
    @provide_session
    def get_untreated_cron_triggers(session=None):
        qry = session.query(Trigger).filter(Trigger.state == State.WAITING).filter(Trigger.trigger_type == 'cronTrigger')
        return qry.all()

    @provide_session
    def get_trigger_state(self, session=None):
        res = session.query(Trigger).filter(Trigger.id == self.id).one_or_none()
        return res.state

    def set_state(self, state):
        if self.state != state and state in State.trigger_states:
            self.state = state
    # @provide_session
    # def update_state(self, session=None):
    #     # 创建之后一直是waiting状态
    #     # 当对应的 dagrun 是RUNNING、SUCCESS、FAILED时 为RUNNING、SUCCESS、FAILED状态
    #     dagrun = session.query(DagRun).filter(DagRun.dag_id == self.dag_id,
    #                                           DagRun.execution_date == self.execution_date,
    #                                           DagRun.run_id.like("pipeline__%")).one_or_none()
    #     self.state = dagrun.get_state()
    #     session.merge(self)
    #     session.commit()

    @provide_session
    def sync_state_to_db(self, state, session=None, commit=True):
        if self.state != state and state in State.trigger_states:
            self.state = state
            session.merge(self)
            if commit:
                session.commit()

    # @provide_session
    # def sync_state_to_db_with_flush(self, state, session=None, commit=True):
    #     # self.refresh_from_db(session=session)
    #     if self.state in [State.TASK_STOP_SCHEDULER, State.FAILED, State.SUCCESS]:
    #         return
    #     else:
    #         self.state = state
    #         session.merge(self)
    #         if commit:
    #             session.commit()

    @staticmethod
    @provide_session
    def sync_state(dag_id=None, execution_date=None, trigger_type=None, state=None, priority=None, ignore_check=False,
                   start_check_time=None,transaction=False, clear_label=False, session=None):
        qry = session.query(Trigger)
        if dag_id:
            qry = qry.filter(Trigger.dag_id == dag_id)
        if execution_date:
            qry = qry.filter(Trigger.execution_date == execution_date)
        # if trigger_type:
        #     qry = qry.filter(Trigger.trigger_type == trigger_type)

        trigger = qry.one_or_none()
        if not trigger:
            return

        if trigger.state != state and state in State.trigger_states:
            if priority is not None:
                trigger.priority = priority
            trigger.state = state
            if trigger_type:
                trigger.trigger_type = trigger_type
            if start_check_time:
                trigger.start_check_time = make_naive(start_check_time)
            trigger.ignore_check = ignore_check
            if clear_label:
                trigger.backfill_label = None

            session.merge(trigger)
            if not transaction:
                session.commit()

    @staticmethod
    @provide_session
    def sync_state_and_get_trigger_type(dag_id=None, execution_date=None, trigger_type=None, state=None,priority=None,transaction=False,session=None):
        trigger_type=None
        qry = session.query(Trigger)
        if dag_id:
            qry = qry.filter(Trigger.dag_id == dag_id)
        if execution_date:
            qry = qry.filter(Trigger.execution_date == execution_date)
        if trigger_type:
            qry = qry.filter(Trigger.trigger_type == trigger_type)

        trigger = qry.one_or_none()
        if not trigger:
            return trigger_type

        trigger_type = trigger.trigger_type
        if trigger.state != state and state in State.trigger_states:
            if priority is not None:
                trigger.priority = priority
            trigger.state = state
            session.merge(trigger)
            if not transaction:
                session.commit()
        return trigger_type
    @staticmethod
    def update_name(old_name,new_name,transaction=False,session=None):
        triggers = session.query(Trigger).filter(Trigger.dag_id == old_name).all()
        for t in triggers:
            t.dag_id = new_name
            t.edit_date = datetime.utcnow()
            session.merge(t)
        if not transaction:
            session.commit()

    @staticmethod
    def get_qry_with_date_range(trigger_id=None, dag_id=None, trigger_type=None, start_date=None, end_date=None,
                                states=None,lock_id=None, editor=None, edit_date=None, task_name_list=None,tri_types=None,
                                backfill_label=None,session=None):

        qry = session.query(Trigger)
        if trigger_id:
            qry = qry.filter(Trigger.id == trigger_id)
        if dag_id or task_name_list:
            if dag_id:
                qry = qry.filter(Trigger.dag_id == dag_id)
            else:
                qry = qry.filter(Trigger.dag_id.in_(task_name_list))
        if trigger_type:
            qry = qry.filter(Trigger.trigger_type == trigger_type)
        if start_date:
            qry = qry.filter(Trigger.execution_date >= make_naive(start_date))
        if end_date:
            qry = qry.filter(Trigger.execution_date <= make_naive(end_date))
        if states:
            qry = qry.filter(Trigger.state.in_(states))
        if tri_types:
            qry = qry.filter(Trigger.trigger_type.in_(tri_types))
        if lock_id:
            qry = qry.filter(Trigger.lock_id == lock_id)
        if editor:
            qry = qry.filter(Trigger.editor == editor)
        if edit_date:
            qry = qry.filter(Trigger.edit_date == edit_date)
        if backfill_label:
            qry = qry.filter(Trigger.backfill_label == backfill_label)
        return qry

    @staticmethod
    @provide_session
    def first_with_date_range(trigger_id=None, dag_id=None, trigger_type=None, start_date=None, end_date=None,
                             states=None, lock_id=None, editor=None, edit_date=None,backfill_label=None, session=None):
        qry = Trigger.get_qry_with_date_range(trigger_id=trigger_id, dag_id=dag_id, trigger_type=trigger_type,
                                              start_date=start_date, end_date=end_date, states=states,
                                              lock_id=lock_id, editor=editor, edit_date=edit_date, backfill_label=backfill_label, session=session)
        # execution_date倒序
        qry = qry.order_by(Trigger.execution_date.desc())
        return qry.first()

    @staticmethod
    @provide_session
    def find_with_date_range(trigger_id=None, dag_id=None, trigger_type=None, start_date=None, end_date=None,
                             states=None,lock_id=None, editor=None, edit_date=None, session=None, page=None, size=None,offset=None,
                             task_name_list=None,tri_types=None,backfill_label=None):
        qry = Trigger.get_qry_with_date_range(trigger_id=trigger_id, dag_id=dag_id, trigger_type=trigger_type,
                                              start_date=start_date, end_date=end_date, states=states,
                                              lock_id=lock_id, editor=editor, edit_date=edit_date, task_name_list=task_name_list,tri_types=tri_types,
                                              backfill_label=backfill_label,session=session)
        # execution_date倒序
        qry = qry.order_by(Trigger.execution_date.desc())
        if size is not None and page is not None:
            return qry.limit(size).offset(offset).all()
        return qry.all()


    @staticmethod
    @provide_session
    def find_by_execution_dates(dag_id=None, execution_dates=None, session=None):
        qry = session.query(Trigger).filter(Trigger.dag_id == dag_id,Trigger.execution_date.in_(execution_dates))
        # execution_date倒序
        qry = qry.order_by(Trigger.execution_date.desc())
        return qry.all()

    @staticmethod
    @provide_session
    def find_by_execution_date(task_name=None, execution_date=None, session=None):
        qry = session.query(Trigger).filter(Trigger.dag_id == task_name,Trigger.execution_date == execution_date)
        return qry.one_or_none()


    @staticmethod
    @provide_session
    def count_with_date_range(trigger_id=None, dag_id=None, trigger_type=None, start_date=None, end_date=None,
                              states=None,lock_id=None, editor=None, edit_date=None,tri_types=None, task_name_list=None,
                              backfill_label=None,session=None):
        qry = Trigger.get_qry_with_date_range(task_name_list=task_name_list,trigger_id=trigger_id, dag_id=dag_id, trigger_type=trigger_type,
                                              start_date=start_date, end_date=end_date, states=states,backfill_label=backfill_label,
                                              lock_id=lock_id, editor=editor, edit_date=edit_date,tri_types=tri_types,session=session)
        return qry.count()

    @staticmethod
    @provide_session
    def get_last_trigger_data(dag_id=None,session=None):
        res = session.query(Trigger) \
            .filter(Trigger.dag_id == dag_id) \
            .order_by(Trigger.execution_date.desc()).first()
        return res

    @staticmethod
    @provide_session
    def get_triggers(trigger_type=None,state=None, session=None):
        qry = session.query(Trigger)
        if state in State.trigger_states:
            qry = qry.filter(Trigger.state == state)
        if trigger_type is not None:
            qry = qry.filter(Trigger.trigger_type == trigger_type)
        return qry.all()

    @staticmethod
    @provide_session
    def decrease_priority_by_tris(tris,session=None):
        for tri in tris:
            tri.priority = 1 if tri.priority == 1 else tri.priority - 1
            session.merge(tri)
        session.commit()

    @staticmethod
    @provide_session
    def get_instance_dates(dag_id, end_date, num=0, session=None):
        from sqlalchemy import and_
        if num == 0:
            num = 20
        return session.query(Trigger).with_entities(Trigger.execution_date).filter(and_(
            Trigger.dag_id == dag_id,
            Trigger.execution_date < end_date
        )).order_by(Trigger.execution_date.desc()).limit(num).all()

    @staticmethod
    @provide_session
    def get_data_link_dates(dag_id, end_date, num=0, session=None):
        from sqlalchemy import and_
        if num == 0:
            num = 20
        result = session.query(Trigger).with_entities(Trigger.execution_date).filter(and_(
            Trigger.dag_id == dag_id,
            Trigger.execution_date <= end_date
        )).order_by(Trigger.execution_date.desc()).limit(num).all()
        return [i.execution_date for i in result]


    @staticmethod
    @provide_session
    def delete_by_execution_date_range(task_name, start_date, transaction=False,session=None):
        qry = session.query(Trigger)\
            .filter(Trigger.dag_id == task_name)\
            .filter(Trigger.execution_date > start_date)
        qry.delete()
        if not transaction:
            session.commit()


    @staticmethod
    @provide_session
    def check_is_forced(task_name,execution_date,session=None):
        qry = session.query(Trigger).filter(Trigger.dag_id == task_name)\
            .filter(Trigger.execution_date == execution_date).all()
        if qry and len(qry) > 0:
            if qry[0].trigger_type and qry[0].trigger_type == TRI_FORCE:
                return True
        return False

    @staticmethod
    @provide_session
    def delete(task_name,transaction=False,session=None):
        qry = session.query(Trigger).filter(Trigger.dag_id == task_name)
        qry.delete(synchronize_session='fetch')
        if not transaction:
            session.commit()


    @provide_session
    def refresh_from_db(self,session=None):
        tri = session.query(Trigger).filter(Trigger.dag_id == self.dag_id,Trigger.execution_date == self.execution_date).first()
        self.state = tri.state
        self.is_executed = tri.is_executed
        self.priority = tri.priority
        self.trigger_type = tri.trigger_type
        self.start_check_time = tri.start_check_time
        self.ignore_check = tri.ignore_check
        self.backfill_label = tri.backfill_label
