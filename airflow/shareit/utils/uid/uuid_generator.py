# -*- coding: utf-8 -*-
import uuid

class UUIDGenerator:
    def __init__(self):
        self.counter = 0
        self.max_count = 9999

    def generate_uuid(self):
        self.counter += 1
        if self.counter > self.max_count:
            self.counter = 0
        base_uuid = uuid.uuid1()
        unique_uuid = "{}-{:04d}".format(base_uuid,self.counter)
        return unique_uuid

    @staticmethod
    def generate_uuid_no_index():
        return str(uuid.uuid1())


# # 使用示例
# generator = UUIDGenerator()
#
# unique_uuid1 = generator.generate_uuid()
# print(unique_uuid1)
#
# unique_uuid2 = generator.generate_uuid()
# print(unique_uuid2)
