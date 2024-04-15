# -*- coding: utf-8 -*-
# import logging
import logging
import sys
import unittest

from kafka import KafkaProducer

from airflow.configuration import conf


from airflow.shareit.log.kafka_handler import KafkaLoggingHandler
from airflow.utils.log.logging_mixin import LoggingMixin

class TestKafka(unittest.TestCase):

    def test_kafka(self):
        kafka_server = '10.20.6.235:9092'
        kafka_topic = 'bdp_task-log_info'
        print kafka_server
        print kafka_topic
        # kafka_client = KafkaClient(kafka_server)
        # kafka_server = '10.20.6.235:9092'
        producer = KafkaProducer(bootstrap_servers=kafka_server)
        data = b'abcd'
        producer.send(kafka_topic, data)
        producer.send(kafka_topic, data)
        producer.send(kafka_topic, data)

        # kafka_client = KafkaClient(kafka_server)
        producer = KafkaProducer(bootstrap_servers=kafka_server)
        data = 'abcd'
        producer.send(kafka_topic, data)
        producer.send(kafka_topic, data)
        producer.send(kafka_topic, data)

    def test_handler(self):
        kafka_server = conf.get('kafka', 'servers')
        kafka_topic = conf.get('kafka', 'topic')
        print kafka_server
        print kafka_topic
        kafka_hanlder = KafkaLoggingHandler(hosts_list=kafka_server, topic=kafka_topic,)
        # kafka_hanlder.pr11('4444')
        log = logging.getLogger('airflow.tasks')
        log.setLevel(logging.INFO)
        # sys.stderr.write(123)
        log.addHandler(hdlr=kafka_hanlder)
        log.info('[STATUS INFO]--RUNNING: 开始启动任务......')
        # log.wa


    def test_print(self):
        print self.__class__.__module__ + '.' + self.__class__.__name__