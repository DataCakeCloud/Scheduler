# -*- coding: utf-8 -*-
"""
Author: Tao Zhang
Date: 2021/12/21
If you are doing your best,you will not have to worry about failure.
"""
import six

from airflow.models.connection import Connection
from airflow.utils.db import provide_session


class ConfigService:
    def __init__(self):
        pass

    @staticmethod
    @provide_session
    def get_conn(conn_ids=None, conn_type=None, session=None):
        res = []
        qry = session.query(Connection)
        if conn_ids is not None:
            if isinstance(conn_ids, six.string_types):
                conn_ids = conn_ids.split(',')
            qry = qry.filter(Connection.conn_id.in_(conn_ids))
        if conn_type is not None:
            qry = qry.filter(Connection.conn_type == conn_type)
        conns = qry.all()
        for conn in conns:
            res.append({"conn_id": conn.conn_id,
                        "conn_type": conn.conn_type,
                        "host": conn.host,
                        "schema": conn.schema,
                        "login": conn.login,
                        "port": conn.port,
                        "extra": conn.extra
                        })
        return res

    @staticmethod
    @provide_session
    def create_or_update_conn(conn_id=None, conn_type=None, host=None, schema=None,
                              login=None, password=None, port=None, extra=None, session=None):
        if conn_id and conn_type:
            conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
            if not conn:
                conn = Connection()
            conn.conn_id = conn_id
            conn.conn_type = conn_type
            conn.host = host
            conn.schema = schema
            conn.login = login
            conn.password = password
            conn.port = port
            conn.extra = extra
            session.merge(conn)
            session.commit()

    @staticmethod
    @provide_session
    def delete_conn(conn_ids=None, session=None):
        if conn_ids is not None:
            if isinstance(conn_ids, six.string_types):
                conn_ids = conn_ids.split(',')
            session.query(Connection).filter(Connection.conn_id.in_(conn_ids)).delete(synchronize_session='fetch')
            session.commit()
