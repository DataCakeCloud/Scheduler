#!/usr/bin/env bash

DesDir="/work/airflow-source"
cd ${DesDir}
sudo SLUGIFY_USES_TEXT_UNIDECODE=yes python setup.py compile_assets
if [[ $? -ne 0 ]];then
    echo "compile failed"
    exit 1
fi

# 如果为python setup.py install 则无法安装在/usr/lib/python2.7/site-package/airflow
sudo SLUGIFY_USES_TEXT_UNIDECODE=yes pip install . -i https://pypi.doubanio.com/simple
if [[ $? -ne 0 ]];then
    echo "install failed"
    exit 1
fi

sudo SLUGIFY_USES_TEXT_UNIDECODE=yes python setup.py extra_clean
if [[ $? -ne 0 ]];then
    echo "clean failed"
    exit 1
fi
