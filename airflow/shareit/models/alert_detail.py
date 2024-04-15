


from sqlalchemy import Column, String, Integer, DateTime,func
from airflow.models.base import Base, ID_LEN, OPTION_LEN
from airflow.utils.db import provide_session

class AlertDetail(Base):
    __tablename__ = "alert_detail"
    id = Column(Integer, primary_key=True)
    task_name = Column(String(ID_LEN))
    execution_date = Column(DateTime)
    owner = Column(String(OPTION_LEN))
    _try_number = Column('try_number', Integer, default=0)
    create_date = Column(DateTime, server_default=func.now())
    update_date = Column(DateTime, server_default=func.now(), onupdate=func.now())

    @staticmethod
    @provide_session
    def get_alerts(task_name,execution_date,try_number=None,session=None):
        qry = session.query(AlertDetail)\
                    .filter(AlertDetail.task_name == task_name) \
                    .filter(AlertDetail.execution_date == execution_date) \
                    .filter(AlertDetail._try_number == try_number)
        return qry.all()

    @provide_session
    def sync_alert_detail_to_db(self,
                             task_name=None,
                             execution_date=None,
                             owner=None,
                             try_number=None,
                             session=None
                             ):
        self.task_name = task_name
        self.execution_date = execution_date
        self.owner = owner
        self._try_number = try_number
        session.merge(self)
