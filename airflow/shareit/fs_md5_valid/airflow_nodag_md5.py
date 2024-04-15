# -*- coding: utf-8 -*-

#  Copyright (c) 2020 Shareit.com Co., Ltd. All Rights Reserved.

# !/usr/bin/env python

"""
@Project    :   Airflow of SHAREit
@File       :   airflow_nodag_md5.py
@Date       :   2020/6/30 3:41 下午
@Author     :   Yunqing Mo
@Concat     :   moyq@ushareit.com
@Company    :   SHAREit
@Desciption :   None
"""
import hashlib
import os

NO_DAGS = "no_dags"
MD5_FILE = "NO_DAG.md5"


def get_md5_str(input_str, start=0, end=None):
    """
    获取字符串的md5值
    :param input_str: 输入字符串
    :param start: md5后返回字符串截取start
    :param end: md5后返回字符串截取end
    :return: 返回字符串的md5[start:end]
    """
    md5_str = hashlib.md5(input_str).hexdigest()
    if end is None:
        return md5_str[start:]
    return md5_str[start:end]


def get_file_md5(input_path, input_file):
    """
    获取文件md5
    :param input_path: 待md5文件目录
    :param input_file: 待md5文件
    :return:
    """
    file_full_path = os.path.join(input_path, input_file)
    filename_md5 = get_md5_str(input_file)
    with open(file_full_path, "rb") as fp:
        content_md5 = get_md5_str(fp.read())
    md5_arr = [filename_md5, content_md5]
    md5_arr.sort()
    return md5_arr


def generate_dir_md5(input_path):
    """
    获取某个目录的md5
    :param input_path: 输入目录
    :return:
    """
    md5_arr = []
    for dirpath, dirnames, filenames in os.walk(input_path):
        md5_arr.extend([get_file_md5(dirpath, f) for f in filenames
                        if os.path.isfile(os.path.join(dirpath, f))
                        and not f.startswith(".") and not f.endswith(".pyc") and not f.endswith(MD5_FILE)])
    return md5_arr


def generate_md5(root_path):
    """
    生成md5
    :param root_path: root path
    :param md5_file: md5 filename
    :return:
    """
    for dirpath, dirnames, filenames in os.walk(root_path):
        dirname = os.path.split(dirpath)[-1]
        abs_dirpath = os.path.abspath(dirpath)
        if dirname == NO_DAGS:
            md5_arr = [get_file_md5(abs_dirpath, f) for f in filenames
                       if os.path.isfile(os.path.join(abs_dirpath, f)) and
                       not f.startswith(".") and not f.endswith(".pyc") and not f.endswith(
                    MD5_FILE)]
            for subdir in dirnames:
                md5_arr.extend(generate_dir_md5(os.path.join(abs_dirpath, subdir)))
            md5_arr = ["".join(m) for m in md5_arr]
            md5_arr.sort()
            nodags_md5 = get_md5_str("".join(md5_arr))
            md5_file_full_path = os.path.join(abs_dirpath, MD5_FILE)
            with open(md5_file_full_path, "wb") as fp:
                fp.write(nodags_md5)


def load_md5_from_dir(input_path):
    """
    从文件读取md5
    :param input_path:
    :return:
    """
    md5_path = os.path.join(input_path, MD5_FILE)
    if not os.path.exists(md5_path):
        return ""
    with open(md5_path, "rb") as fp:
        return fp.read()


def check_md5(root_path, input_path):
    """
    检查dag依赖nodags md5是否一致
    :param root_path:
    :param input_path:
    :return: True if source md5 equals target md5 else False
    """
    if not os.path.isabs(root_path):
        root_path = os.path.abspath(root_path)
    if not os.path.isabs(input_path):
        input_path = os.path.abspath(input_path)
    # print(root_path, input_path)
    input_dir = os.path.split(input_path)[0]
    if root_path in input_dir:
        nodags_dir = os.path.join(input_dir, NO_DAGS)
        check_result = True
        if os.path.exists(nodags_dir):
            source_md5 = load_md5_from_dir(nodags_dir)
            target_md5_arr = generate_dir_md5(nodags_dir)
            target_md5_arr = ["".join(t) for t in target_md5_arr]
            target_md5_arr.sort()
            target_md5 = get_md5_str("".join(target_md5_arr))
            check_result = source_md5 == target_md5
        if check_result:
            return check_md5(root_path, input_dir)
        return False
    return True


if __name__ == "__main__":
    generate_md5("/work/airflow/dags")
    print(check_md5("/work/airflow/dags", "/work/airflow/dags/Game/test/environment.py"))
