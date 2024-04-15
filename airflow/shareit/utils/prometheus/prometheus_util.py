# -*- coding: utf-8 -*-
from prometheus_client import Gauge


class PrometheusUtil:

    @staticmethod
    def set_gauge_metrix(metrix_name,description,label_names,label_values,metrix_value):
        c = Gauge(metrix_name, description, label_names)
        c.labels(label_values).set(metrix_value)