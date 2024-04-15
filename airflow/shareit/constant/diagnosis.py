# -*- coding: utf-8 -*-

class DiagnosisConstant(object) :
    """
    Static class with task and dataset states constants  to
    avoid hardcoding.
    """

    # type
    TASK = 'task'
    METADATA = 'metadata'

    # state
    NORMAL = 0
    UPSTREAM_ABNORMAL = 1
    SELF_ABNORMAL = 2

    # 信息
    UPSTREAM_NOT_READY = "上游任务未就绪"
    NO_TRIGGER = "trigger未生成"

    # operation
    CALL_ADMIN = "请联系管理员"
    WAIT_UPSTREAM = "等待上游就绪"


    @classmethod
    def get_operation(cls,info):
        if info == cls.UPSTREAM_NOT_READY:
            return cls.WAIT_UPSTREAM

        if info == cls.NO_TRIGGER:
            return cls.CALL_ADMIN
