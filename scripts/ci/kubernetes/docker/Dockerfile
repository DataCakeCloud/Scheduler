FROM public.ecr.aws/l1f5u0h6/shareit-bdp/airflow-environment:scheduler-base

ADD . /data/code
COPY scripts/ci/kubernetes/docker/requirements.txt /tmp/requirements.txt
COPY scripts/ci/kubernetes/docker/requirements_shareit.txt /tmp/requirements_shareit.txt
COPY scripts/ci/kubernetes/docker/compile.sh /tmp/compile.sh
COPY scripts/ci/kubernetes/docker/kyuubi-hive-jdbc-shaded-1.7.0.jar /tmp/
RUN cd /data/code && python setup.py compile_assets \
    && python2 -m pip install --upgrade pip \
    && pip install setuptools==44.1.1 \
    && pip install -r /tmp/requirements.txt \
    && pip install -r /tmp/requirements_shareit.txt  \
    && pip install --no-use-pep517 . \
    && pip uninstall -y dataclasses

RUN yum install yum install -y epel-release \
    && yum install -y sshpass


ENV LC_ALL en_US.UTF-8
ENV LC_CTYPE en_US.UTF-8

# prod
ENV AIRFLOW_HOME /work/airflow/
ENV FLINK_HOME /home/flink
ENV JAVA_HOME /home/jdk1.8.0_311
ENV CLASSPATH $CLASSPATH:$JAVA_HOME/lib/
ENV PATH $PATH:$JAVA_HOME/bin:$FLINK_HOME/bin
