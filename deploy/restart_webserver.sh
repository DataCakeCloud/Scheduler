#!/usr/bin/env bash

echo 'start reboot airflow-webserver'

urls=('http://ec2-54-172-122-140.compute-1.amazonaws.com:2812/airflow-webserver')
if [[ -n $1 && $1 = 'prod' ]]; then
    urls=('http://ec2-3-85-18-197.compute-1.amazonaws.com:2812/airflow-webserver' 'http://ec2-3-85-18-197.compute-1.amazonaws.com:2812/airflow-webserver')
fi

for url in ${urls[*]}; do
    curl -X GET -I -H "Authorization: Basic YWRtaW46YWRtaW4=" $url -D ~/restart_webserver_header.txt > /dev/null

    ok=`cat ~/header.txt | grep '200 OK'`

    if [[ -z $ok ]]; then
        echo 'monit api call failed, please take a look!'
        exit  1
    fi

    token=`cat ~/header.txt | grep 'Set-Cookie' | sed 's/.*securitytoken=\(.*\); Max-Age.*/\1/g'`

    curl $url -H 'Authorization: Basic YWRtaW46YWRtaW4=' -H 'Cookie: securitytoken=$token' --data \
    'securitytoken=$token&action=restart' -D ~/restart_webserver_header.txt > /dev/null

    ok=`cat ~/header.txt | grep '200 OK'`

    if [[ -z $ok ]]; then
        echo 'monit api call failed, please check!'
        echo 'reboot failed'
    else
        echo 'reboot succeed!'
    fi
done