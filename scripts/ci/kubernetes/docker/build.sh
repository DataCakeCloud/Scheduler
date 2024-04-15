#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations

# under the License.

while getopts ":i:t:a:" opt
do
    case $opt in
        i)
        echo "input args: $OPTARG"
        IMAGE=$OPTARG
        ;;
        t)
        echo "input args: $OPTARG"
        TAG=$OPTARG
        ;;
        a)
        echo "input args: $OPTARG"
        AIRFLOW_ENV=$OPTARG
        ;;
        ?)
        echo "input args: $OPTARG"
        echo "usage: sh build.sh [<options>]"
        echo "options:"
        echo "  -image  airflow image name, default airflow"
        echo "  -tag    airflow image tag, default latest"
        exit 1;;
    esac
done

IMAGE=${IMAGE:-airflow}
TAG=${TAG:-latest}
echo "image: $IMAGE"
echo "tag: $TAG"
echo "airflow_env: $AIRFLOW_ENV"

DIRNAME=$(cd "$(dirname "$0")" && pwd)
AIRFLOW_ROOT="${DIRNAME}/../../../.."
PYTHON_DOCKER_IMAGE=python:3.6

set -e

echo "Airflow directory ${AIRFLOW_ROOT}"
echo "Airflow Docker directory ${DIRNAME}"

# cd "${AIRFLOW_ROOT}"
#docker run -i --rm -v "${AIRFLOW_ROOT}:/airflow" \
#    -w /airflow "${PYTHON_DOCKER_IMAGE}" ./scripts/ci/kubernetes/docker/compile.sh
#
#sudo rm -rf "${AIRFLOW_ROOT}/airflow/www/node_modules"
#sudo rm -rf "${AIRFLOW_ROOT}/airflow/www_rbac/node_modules"

cd "${AIRFLOW_ROOT}/.."
tar -zcf airflow.tar.gz "${AIRFLOW_ROOT}/../datastudio-pipeline"
echo "Copy distro ${AIRFLOW_ROOT}/airflow.tar.gz ${DIRNAME}/airflow.tar.gz"
cp airflow.tar.gz "${DIRNAME}/airflow.tar.gz"

#echo "Copy distro ${AIRFLOW_ROOT}/dist/*.tar.gz ${DIRNAME}/airflow.tar.gz"
#cp "${AIRFLOW_ROOT}"/dist/*.tar.gz "${DIRNAME}/airflow.tar.gz"
cd "${DIRNAME}" && docker build --no-cache --rm=true --pull "${DIRNAME}" --tag="${IMAGE}:${TAG}" --build-arg airflow_env=${AIRFLOW_ENV}
rm "${DIRNAME}/airflow.tar.gz"
