# -*- coding: utf-8 -*-
import json


import logging
import time
import traceback
import sys

from pykafka import KafkaClient

class KafkaLoggingHandler(logging.Handler):

    def __init__(self, hosts_list, topic, task_id=None,chat_id=None,key=None):
        super(KafkaLoggingHandler, self).__init__()
        self.key = key
        # self.kafka_client = KafkaClient(hosts_list)
        self.kafka_topic_name = topic
        self.task_id = task_id
        self.chat_id = chat_id
        self.client = KafkaClient(hosts=hosts_list)
        self.topic = self.client.topics[topic]
        self.producer = self.topic.get_producer()

    def emit(self, record):
        # print record
        # drop kafka logging to avoid infinite recursion
        if 'kafka' in str(record.name):
            return
        try:
            # use default formatting
            msg = self.format(record)
            # print msg
            # format
            if '[STATUS INFO]' in msg:
                if 'SUCCESS' in msg:
                    status = 'success'
                elif 'FAILED' in msg:
                    status = 'failed'
                else :
                    status = 'running'
                msg_dict = {
                    "type":"STATUS_INFO",
                    "data":{
                        "chatId":self.chat_id,
                        "taskId":self.task_id,
                        "status":status
                    }
                }
            else :
                msg_dict = {
                    "type":"RUN_INFO",
                    "data":{
                        "chatId":self.chat_id,
                        "taskId":self.task_id,
                        "message":msg
                    }
                }

            # produce message
            # if not self.key:
            self.producer.produce(json.dumps(msg_dict,ensure_ascii=False).encode('utf-8'))
            # self.producer.flush()
            # else:
            #     self.producer.send(self.kafka_topic_name, self.key, json.dumps(msg_dict,ensure_ascii=False))
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)

    def close(self):
        self.producer.stop()
        logging.Handler.close(self)