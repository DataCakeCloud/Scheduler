from airflow.shareit.utils.state import State
from airflow.utils.db import provide_session
from airflow.utils.sqlalchemy import UtcDateTime

from airflow.models.base import Base, ID_LEN, OPTION_LEN, MESSAGE_LEN

from sqlalchemy import Column, String, Integer, DateTime, func, update, Boolean


class RegularAlert(Base):
    __tablename__ = "regular_alert"
    id = Column(Integer, primary_key=True)
    task_name = Column(String(ID_LEN))
    execution_date = Column(DateTime)
    alert_type = Column(String(ID_LEN))
    granularity = Column(String(OPTION_LEN))
    check_time = Column(DateTime)
    trigger_condition = Column(String(ID_LEN))
    is_notify_collaborator = Column(Boolean, default=False)
    state = Column(String(20))
    is_regular = Column(Boolean, default=True)

    @provide_session
    def sync_regular_alert_to_db(self,
                                 task_name=None,
                                 execution_date=None,
                                 alert_type=None,
                                 granularity=None,
                                 check_time=None,
                                 state=None,
                                 trigger_condition=None,
                                 is_notify_collaborator=False,
                                 is_regular=True,
                                 session=None
                                 ):
        ra = session.query(RegularAlert).filter(RegularAlert.task_name == task_name,
                        RegularAlert.execution_date == execution_date,
                        RegularAlert.trigger_condition == trigger_condition,
                        RegularAlert.is_regular == is_regular).one_or_none()
        if ra is None:
            self.task_name = task_name
            self.execution_date = execution_date
            self.alert_type = alert_type
            self.granularity = granularity
            self.check_time = check_time
            self.state = state
            self.trigger_condition = trigger_condition
            self.is_notify_collaborator = is_notify_collaborator
            self.is_regular=is_regular
            session.merge(self)
        else:
            ra.execution_date = execution_date
            ra.granularity = granularity
            ra.check_time = check_time
            ra.trigger_condition = trigger_condition
            ra.is_notify_collaborator = is_notify_collaborator
            if ra.is_regular == False:
                ra.state = state
            else:
                ra.alert_type = alert_type
            session.merge(ra)
            session.commit()

    @staticmethod
    @provide_session
    def get_regular_alert_all_task(session=None):
        regular_alert= session.query(RegularAlert)\
            .filter(RegularAlert.state == State.CHECKING)\
            .filter(RegularAlert.is_regular == True)\
            .all()
        return regular_alert

    @staticmethod
    @provide_session
    def get_task_alert_all_task(session=None):
        regular_alert = session.query(RegularAlert) \
            .filter(RegularAlert.state == State.CHECKING) \
            .filter(RegularAlert.is_regular == False) \
            .all()
        return regular_alert

    @staticmethod
    @provide_session
    def update_ra_state(task_name, execution_date, state,trigger_condition ,session=None, transaction=False):
        ra = session.query(RegularAlert).filter(RegularAlert.task_name == task_name,
                                                 RegularAlert.execution_date == execution_date,
                                                 RegularAlert.trigger_condition == trigger_condition).one_or_none()
        if ra:
            ra.state = state
            session.merge(ra)
        if not transaction:
            session.commit()
