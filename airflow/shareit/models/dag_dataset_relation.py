# -*- coding: utf-8 -*-
"""
Author: zhangtao
CreateDate: 2021/4/16
"""

from sqlalchemy import Column, String, Integer, Boolean, tuple_

from airflow.models.base import Base, ID_LEN, MESSAGE_LEN, OPTION_LEN
from airflow.utils.db import provide_session
from airflow.shareit.utils.granularity import Granularity


class DagDatasetRelation(Base):
    __tablename__ = "dag_dataset_relation"

    id = Column(Integer, primary_key=True)
    dag_id = Column(String(ID_LEN))
    metadata_id = Column(String(ID_LEN))
    dataset_type = Column(Boolean)  # True is input, False is output
    granularity = Column(String(OPTION_LEN))
    offset = Column(Integer)
    ready_time = Column(String(ID_LEN))
    check_path = Column(String(MESSAGE_LEN))
    detailed_dependency = Column(String(MESSAGE_LEN))
    detailed_gra = Column(String(OPTION_LEN))
    date_calculation_param = Column(String(MESSAGE_LEN))

    @provide_session
    def sync_ddr_to_db(self, dag_id, metadata_id, dataset_type, granularity, offset, ready_time, check_path,
                       detailed_dependency=None,
                       detailed_gra=None,
                       date_calculation_param=None,
                       transaction=False,
                       session=None):
        if detailed_dependency is not None:
            if isinstance(detailed_dependency, list):
                detailed_dependency = ','.join(map(str, detailed_dependency))
            detailed_dependency = detailed_dependency.replace("All", "ALL").replace("all", "ALL")
        self.dag_id = dag_id
        self.metadata_id = metadata_id
        self.dataset_type = dataset_type
        self.granularity = granularity
        self.offset = offset
        self.ready_time = ready_time
        self.check_path = check_path
        self.detailed_dependency = detailed_dependency
        self.detailed_gra = detailed_gra
        self.date_calculation_param=date_calculation_param
        session.merge(self)
        if not transaction:
            session.commit()

    @staticmethod
    @provide_session
    def get_relation(dag_id, metadata_id, dataset_type, session=None):
        return session.query(DagDatasetRelation) \
            .filter(DagDatasetRelation.dag_id == dag_id,
                    DagDatasetRelation.metadata_id == metadata_id,
                    DagDatasetRelation.dataset_type == dataset_type).one_or_none()

    @staticmethod
    @provide_session
    def get_inputs(dag_id, session=None):
        return session.query(DagDatasetRelation) \
            .filter(DagDatasetRelation.dag_id == dag_id,
                    DagDatasetRelation.dataset_type.is_(True)).all()

    @staticmethod
    @provide_session
    def get_outputs(dag_id, session=None):
        return session.query(DagDatasetRelation) \
            .filter(DagDatasetRelation.dag_id == dag_id,
                    DagDatasetRelation.dataset_type.is_(False)).all()

    @staticmethod
    def get_metadata_id(dag_id):
        dag_dataset_relation = DagDatasetRelation.get_outputs(dag_id)  # dag_dataset_relation：条件dag_id, datasetType = 0
        return dag_dataset_relation[0].metadata_id if dag_dataset_relation else ''

    @staticmethod
    @provide_session
    def get_downstream_dags(metadata_id, session=None):
        return session.query(DagDatasetRelation) \
            .filter(DagDatasetRelation.metadata_id == metadata_id,
                    DagDatasetRelation.dataset_type.is_(True)).all()

    @staticmethod
    @provide_session
    def get_active_downstream_dags(metadata_id, session=None,dag_id=None):
        from airflow.models.dag import DagModel
        subquery = session.query(DagModel.dag_id) \
            .filter(DagModel.is_active.is_(True),
                    DagModel.is_paused.is_(False))
        if dag_id:
            return session.query(DagDatasetRelation) \
                .filter(DagDatasetRelation.metadata_id == metadata_id,
                        DagDatasetRelation.dataset_type.is_(True),
                        DagDatasetRelation.dag_id == dag_id,
                        DagDatasetRelation.dag_id.in_(subquery)).all()
        else:
            return session.query(DagDatasetRelation) \
                .filter(DagDatasetRelation.metadata_id == metadata_id,
                        DagDatasetRelation.dataset_type.is_(True),
                        DagDatasetRelation.dag_id.in_(subquery)).all()

    @staticmethod
    @provide_session
    def get_upstream_dags(metadata_id, session=None):
        return session.query(DagDatasetRelation) \
            .filter(DagDatasetRelation.metadata_id == metadata_id,
                    DagDatasetRelation.dataset_type.is_(False)).all()

    @staticmethod
    @provide_session
    def get_active_upstream_dags(metadata_id, session=None):
        """获取活跃上游"""
        from airflow.models.dag import DagModel
        all_upstreams = session.query(DagDatasetRelation) \
            .filter(DagDatasetRelation.metadata_id == metadata_id,
                    DagDatasetRelation.dataset_type.is_(False)).all()
        res = []
        for a in all_upstreams:
            if DagModel.is_active_and_on(a.dag_id):
                res.append(a)
        return res

    @staticmethod
    @provide_session
    def get_downstream_dags_by_dag_id(dag_id, session=None):
        """
        获取当前任务的所有活跃的直接下游任务
        """
        from airflow.models.dag import DagModel
        subquery = session.query(DagDatasetRelation.metadata_id) \
            .filter(DagDatasetRelation.dag_id == dag_id,
                    DagDatasetRelation.dataset_type.is_(False),
                    )
        return session.query(DagDatasetRelation) \
            .filter(DagDatasetRelation.metadata_id.in_(subquery),
                    DagDatasetRelation.dataset_type.is_(True),
                    DagModel.is_active.is_(True),
                    DagModel.is_paused.is_(False)) \
            .join(DagModel, DagModel.dag_id == DagDatasetRelation.dag_id).all()

    @staticmethod
    @provide_session
    def delete(dag_id=None, metadata_ids=None, dataset_type=None, transaction=False, session=None):
        qry = session.query(DagDatasetRelation)
        if dag_id:
            qry = qry.filter(DagDatasetRelation.dag_id == dag_id)
        if metadata_ids:
            qry = qry.filter(DagDatasetRelation.metadata_id.in_(metadata_ids))
        if dataset_type is not None:
            qry = qry.filter(DagDatasetRelation.dataset_type == dataset_type)
        qry.delete(synchronize_session='fetch')
        if not transaction:
            session.commit()

    @staticmethod
    @provide_session
    def get_all_external_dataset(session=None):
        res = session.query(DagDatasetRelation).filter(DagDatasetRelation.dataset_type.is_(True)).all()
        return res


    @staticmethod
    @provide_session
    def get_all_outer_dataset(session=None):
        from airflow.models.dag import DagModel
        subquery = session.query(DagDatasetRelation.metadata_id) \
            .filter(DagDatasetRelation.dataset_type.is_(False), DagModel.is_active.is_(True),
                    DagModel.is_paused.is_(False)) \
            .join(DagModel, DagModel.dag_id == DagDatasetRelation.dag_id)
        res = session.query(DagDatasetRelation).filter(DagDatasetRelation.dataset_type.is_(True),
                                                       DagDatasetRelation.metadata_id.notin_(subquery)).all()
        return res

    @staticmethod
    @provide_session
    def get_outer_dataset(dag_id=None, session=None):
        from airflow.models.dag import DagModel
        subquery = session.query(DagDatasetRelation.metadata_id) \
            .filter(DagDatasetRelation.dataset_type.is_(False), DagModel.is_active.is_(True),
                    DagModel.is_paused.is_(False)) \
            .join(DagModel, DagModel.dag_id == DagDatasetRelation.dag_id)
        res = session.query(DagDatasetRelation).filter(DagDatasetRelation.dataset_type.is_(True),
                                                       DagDatasetRelation.metadata_id.notin_(subquery),
                                                       DagDatasetRelation.dag_id == dag_id).all()
        return res

    @staticmethod
    @provide_session
    def get_self_reliance_dataset(session=None):
        subquery = session.query(DagDatasetRelation.dag_id, DagDatasetRelation.metadata_id).filter(
            DagDatasetRelation.dataset_type.is_(False))
        res = session.query(DagDatasetRelation).filter(DagDatasetRelation.dataset_type.is_(True),
                                                       tuple_(DagDatasetRelation.dag_id,
                                                              DagDatasetRelation.metadata_id).in_(subquery)).all()
        return res

    @staticmethod
    def get_dataset_granularity(metadata_id):
        res = DagDatasetRelation.get_active_upstream_dags(metadata_id=metadata_id)
        if res:
            return res[0].granularity
        else:
            # 无上游记录。可能是个外部数据
            return -1

    @staticmethod
    def get_task_granularity(dag_id):
        res = DagDatasetRelation.get_outputs(dag_id=dag_id)
        granularity = -1
        for ddr in res:
            granularity = max(Granularity.getIntGra(ddr.granularity), granularity)
        return granularity

    @staticmethod
    def update_name(old_name, new_name, transaction=False,session=None):
        ddrs = session.query(DagDatasetRelation).filter(DagDatasetRelation.dag_id == old_name).all()
        for ddr in ddrs:
            ddr.dag_id = new_name
            session.merge(ddr)
        if not transaction:
            session.commit()

    def get_last_detailed_dependency(self, gra, data_partation_sign):
        """
        gra : 实际产出的粒度
        data_partation_sign: 数据的时间标识，应该是待依赖的时间段
        """
        # 待处理阶段的是时间标志
        # 返回该阶段的最后一个依赖的时间标志
        gra = Granularity.getIntGra(gra)
        if self.detailed_dependency is not None and self.detailed_gra is not None:
            if Granularity.getIntGra(self.granularity) >= Granularity.getIntGra(
                    self.detailed_gra) > Granularity.getIntGra(gra):
                raise ValueError("[DataStudio] 粒度配置错误 {dag_id} {metadata_id}".format(dag_id=self.dag_id,
                                                                                     metadata_id=self.metadata_id))
            res = Granularity.get_detail_time_range(base_gra=Granularity.getIntGra(gra),
                                                    depend_gra=Granularity.getIntGra(self.granularity),
                                                    detailed_gra=Granularity.getIntGra(self.detailed_gra),
                                                    detailed_dependency=self.detailed_dependency,
                                                    base_date=data_partation_sign)
            if res:
                res.sort()
                return res[-1]
        return False

    @staticmethod
    @provide_session
    def check_output(dag_id,metadata_id,session=None):
        """
        获取当前所有活跃任务中，产出为metadata_id且非dag_id的任务
        """
        from airflow.models.dag import DagModel
        res = session.query(DagDatasetRelation) \
            .filter(DagModel.is_active.is_(True),
                    DagModel.is_paused.is_(False),
                    DagDatasetRelation.dataset_type.is_(False),
                    DagDatasetRelation.dag_id != dag_id,
                    DagDatasetRelation.metadata_id == metadata_id) \
            .join(DagModel, DagModel.dag_id == DagDatasetRelation.dag_id).all()
        return res

    # @staticmethod
    # @provide_session
    # def check_is_