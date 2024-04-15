# -*- coding: utf-8 -*-
import jaydebeapi
import time
import hashlib
from airflow.configuration import conf
from airflow.shareit.models.task_desc import TaskDesc

driver_path = conf.get("kyuubi","kyuubi_hive_driver_path")
driver = conf.get("kyuubi","kyuubi_hive_driver")
url = conf.get("kyuubi","kyuubi_hive_url")
user = conf.get("kyuubi","kyuubi_hive_user")
# key = conf.get("kyuubi","kyuubi_key")
def execute(sql,user,user_group_id,tendent_name,user_group_name):
    # 获取当前时间的毫秒数
    open_time = int(time.time() * 1000)

    # # 拼接字符串
    # sign_str = ":".join([user_group_name, key, str(open_time)])

    # 计算 MD5
    # sign = hashlib.md5(sign_str.encode()).hexdigest()
    url_format = url.format(
        user=user,
        user_group_id=user_group_id,
        tendent=tendent_name,
        user_group_name=user_group_name,
        # sign=sign,
        # open_time=str(open_time)
    )
    print url_format
    conn = jaydebeapi.connect(driver, url_format, ["", ""], driver_path)
    with conn.cursor() as cur:
        cur.execute(sql)
        result = cur.fetchall()
        print result


def execute_msck_partion(table,user,user_group_id,tendent_name,user_group_name,partions):
    sql = "alter table {table} add if not exists partition ({partions})".format(table=table,partions=partions)
    print sql
    execute(sql=sql,user=user,user_group_id=user_group_id,tendent_name=tendent_name,user_group_name=user_group_name)
