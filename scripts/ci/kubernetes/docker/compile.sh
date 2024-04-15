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

set -e
export DEBIAN_FRONTEND=noninteractive

yum install -y npm
npm -g install n
n stable

pip install GitPython
yum install -y wget
wget https://dl.yarnpkg.com/rpm/yarn.repo -O /etc/yum.repos.d/yarn.repo
yum install -y yarn

git clone https://github.com/facebookincubator/bowler
cd bowler
python setup.py install
# compile webUI
cd /tmp/datastudio-pipeline

SLUGIFY_USES_TEXT_UNIDECODE=yes python setup.py compile_assets

