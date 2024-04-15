#!/bin/env  bash

CodeDir="/work/CodeDeploy-source/`date +%F-%H-%M`"

AppDir="/work/CodeDeploy-source/airflow-source"

DesDir="/work/airflow-source"

if [[ -d  "$DesDir" ]];then
    rm -rf ${DesDir}/*
else
    mkdir -p ${DesDir}
fi

mkdir -p ${CodeDir}

cp -rp ${AppDir}/* ${CodeDir}/

cp -rp ${CodeDir}/* ${DesDir}/