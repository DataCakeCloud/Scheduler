#!/bin/bash
set -ex

root_path=$PWD
history_version=$root_path/history_version.txt

function get_json_value(){
	  res_str=`grep $1 $root_path/$2 | grep -v '#'`
	    echo ${res_str#*=}
    }

function get_dict_value(){
	  res_str=`grep $1 $root_path/$2 | grep -v '#'`
	    values=${res_str#*: }
	      arr=(${values//:/ })
	        echo ${arr[1]}
	}

function replace_yaml(){
	  yaml_path=$root_path/$3
	    sed -i "s/$1/$2/g" $yaml_path
	      kubectl apply -f $yaml_path
      }

#replace_prefix='1.10.10_'
if [[ $# -lt 2 ]];then
	  echo "need 2 params, e.g.: 1.10.10_test_38 1.10.10_test_38_1"
	    exit 1
fi

airflow_version=$1
plugin_version=$2
echo "airflow_version:$airflow_version, plugin_version:$plugin_version"

# model format
#echo "date    cm_version:scheduler_version:webserver_version" > history_version.txt
# cm history
cm_version=`get_json_value 'worker_container_tag' 'configmaps-cfg.yaml'`
scheduler_version=`get_dict_value "image: " 'deployment-scheduler.yaml'`
webserver_version=`get_dict_value "image: " 'deployment-webserver.yaml'`
echo `date +"%Y%m%d:%H%M"`    $cm_version:$scheduler_version:$webserver_version >> $history_version

replace_yaml $cm_version $airflow_version 'configmaps-cfg.yaml'
replace_yaml $scheduler_version $airflow_version 'deployment-scheduler.yaml'
replace_yaml $webserver_version $plugin_version 'deployment-webserver.yaml'

