#!/usr/bin/env bash

echo 'start reboot airflow-scheduler'

url='http://ec2-54-172-122-140.compute-1.amazonaws.com:2812/airflow-scheduler'
if [[ -n $1 && $1 = 'prod' ]]; then
    url='http://ec2-3-85-18-197.compute-1.amazonaws.com:2812/airflow-scheduler'
fi

curl -X GET -I -H "Authorization: Basic YWRtaW46YWRtaW4=" $url -D ~/restart_scheduler_header.txt > /dev/null

ok=`cat ~/header.txt | grep '200 OK'`

if [[ -z $ok ]]; then
    echo 'monit api call failed, please take a look!'
    exit  1
fi

token=`cat ~/header.txt | grep 'Set-Cookie' | sed 's/.*securitytoken=\(.*\); Max-Age.*/\1/g'`

curl $url -H 'Authorization: Basic YWRtaW46YWRtaW4=' -H 'Cookie: securitytoken=$token' --data \
'securitytoken=$token&action=restart' -D ~/restart_scheduler_header.txt > /dev/null

ok=`cat ~/header.txt | grep '200 OK'`

if [[ -z $ok ]]; then
    echo 'monit api call failed, please check!'
    exit  1
else
    echo 'reboot succeed!'
fi