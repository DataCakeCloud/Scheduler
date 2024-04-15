# -*- coding: utf-8 -*-
import unittest

from airflow.shareit.grpc.grpcRun import GrpcRun


class GrpcRunTest(unittest.TestCase):

    def test_grpc_run(self):
        GrpcRun()._run(10, 9999)
