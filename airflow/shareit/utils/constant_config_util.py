# -*- coding: utf-8 -*-
from airflow.shareit.models.constants_map import ConstantsMap


class ConstantConfigUtil(object):
    @staticmethod
    def get_value(_type, key):
        return ConstantsMap.get_value(_type=_type, key=key)

    @staticmethod
    def get_int(_type, key):
        try:
            return int(ConstantConfigUtil.get_value(_type, key))
        except ValueError:
            raise ValueError("The value for key {key} is not a valid integer.")

    @staticmethod
    def get_float(_type, key):
        try:
            return float(ConstantConfigUtil.get_value(_type, key))
        except ValueError:
            raise ValueError("The value for key {key} is not a valid float.")

    @staticmethod
    def get_string(_type, key):
        return str(ConstantConfigUtil.get_value(_type, key))

    @staticmethod
    def get_bool(_type, key):
        value = ConstantConfigUtil.get_value(_type, key)
        if value.lower() == "true":
            return True
        elif value.lower() == "false":
            return False
        else:
            raise ValueError("The value for key {key} is not a valid boolean.")
