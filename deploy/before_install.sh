#!/bin/env  bash

AppDir="/work/CodeDeploy-source/airflow-source"

if [[ -d  "$AppDir" ]];then
    rm -rf ${AppDir}/*
else
    mkdir -p ${AppDir}
fi
