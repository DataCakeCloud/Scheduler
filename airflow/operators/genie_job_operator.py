# -*- coding: utf-8 -*-
import base64

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from airflow.hooks.genie_hook import GenieHook
import zlib


class GenieJobOperator(BaseOperator):
    """
    Execute a task through genie.

    :param genie_conn_id: reference to a genie service
    :type genie_conn_id: str
    :param cluster_type: value of the 'type' tag for Genie to use when selecting which cluster to
        run the job on. Deprecated, use cluster_tags instead.
    :type: cluster_type: str
    :param cluster: value of the 'sched' tag for Genie to use when selecting which cluster to
        run the job on. Deprecated, use cluster_tags instead.
    :type cluster: str
    :param command_type: value of the 'type' tag for Genie to use when selecting which command to
        use when executing the job. Deprecated, use command_tags instead.
    :type command_type: str
    :param command: the command to run for the job.
    :type command: str
    :param cluster_tags: tags for Genie to use when selecting which cluster to run the job on.
    :type cluster_tags: list or comma separated str.
    :param command_tags: tags for Genie to use when selecting which command to use when executing the job.
    :type: list or comma separated str.
    :param dependencies: file dependencies for the genie job.
    :type: dependencies: list
    :param job_name: a name for the job. The name does not have to be unique.
    :type: job_name: str
    :param setup_file: a Bash file to source before the job is executed.
    :type: setup_file: str
    :param cpu: the number of millicore(m) for Genie to allocate when executing the job.
    :type: cpu: int
    :param memory: the amount of memory(MB) for Genie to allocate when executing the job.
    :type: memory: int
    :param timeout: the timeout(in seconds) for the job. If the job does not finish
        within the specified timeout, Genie will kill the job.
    :type: timeout: int
    """

    template_fields = ('command',)
    ui_color = '#f0ed04'

    @apply_defaults
    def __init__(self,
                 genie_conn_id='genie_default',
                 cluster_type='yarn',
                 cluster=None,
                 command_type=None,
                 command=None,
                 cluster_tags=None,
                 command_tags=None,
                 dependencies=None,
                 setup_file=None,
                 cpu=None,
                 memory=None,
                 timeout=None,
                 job_name=None,
                 cluster_sla='normal',
                 *args, **kwargs):
        super(GenieJobOperator, self).__init__(*args, **kwargs)
        self.genie_conn_id = genie_conn_id
        self.cluster_type = cluster_type
        self.cluster = cluster
        self.command_type = command_type
        self.command = command
        self.cluster_tags = cluster_tags
        self.command_tags = command_tags
        self.dependencies = dependencies
        self.job_name = job_name or kwargs['task_id']
        self.setup_file = setup_file
        self.cpu = cpu
        self.memory = memory
        self.timeout = timeout
        self.cluster_sla = cluster_sla
        self.genie_hook = None

    def execute(self, context):
        self.genie_hook = GenieHook(genie_conn_id=self.genie_conn_id)
        ti = None
        if isinstance(context, dict) and "ti" in context.keys():
            ti = context['ti'] if self.task_id == context['ti'].dag_id else None
        self.encode_command_with_base64()
        status = self.genie_hook.submit_job(cluster_sla=self.cluster_sla,
                                            cluster_type=self.cluster_type,
                                            cluster=self.cluster,
                                            command_type=self.command_type,
                                            command=self.command,
                                            cluster_tags=self.cluster_tags,
                                            command_tags=self.command_tags,
                                            dependencies=self.dependencies,
                                            job_name=self.job_name,
                                            setup_file=self.setup_file,
                                            cpu=self.cpu,
                                            memory=self.memory,
                                            timeout=self.timeout,
                                            task_ins=ti)
        if status.upper() != 'SUCCEEDED':
            raise AirflowException('Genie job failed')

    def encode_command_with_base64(self):
        tmpList = self.command.split('/*zheshiyigebianmabiaoshi*/')
        if len(tmpList) == 3:
            if "psql" in self.command_tags:
                self.command = " ".join(tmpList)
            else:
                compressed = zlib.compress(tmpList[1])
                base64Code = base64.b64encode(compressed)
                base64Code = self._insert_space(line=base64Code, interval=1024)
                tmpList[1] = base64Code
                self.command = " ".join(tmpList)

    def _insert_space(self, line=None, interval=None):
        if interval is None:
            interval = 1024
        tmpList = [line[i:i + interval] for i in range(0, len(line), interval)]
        return " ".join(tmpList)

    def pre_execute(self, context):
        # super(GenieJobOperator,self).pre_execute(context=context)
        try:
            conf = context['dag_run'].conf
            if conf and self.task_id in conf:
                print("Replace the corresponding parameter from dag_run conf!")
                argus = conf[self.task_id]
                if argus:
                    if "cluster_tags" in argus:
                        self.cluster_tags = str(argus["cluster_tags"])
                        print("Replace cluster_tags to " + str(argus["cluster_tags"]))
                    if "command_tags" in argus:
                        self.command_tags = str(argus["command_tags"])
                        print("Replace command_tags to " + str(argus["command_tags"]))
                    if "command" in argus:
                        self.command = str(argus["command"])
                        print("Replace command to " + str(argus["command"]))
                    if "dependencies" in argus:
                        self.dependencies = str(argus["dependencies"])
                        print("Replace dependencies to " + str(argus["dependencies"]))
        except Exception as e:
            print("Can not set argument from conf." + str(e))

    def on_kill(self):
        self.genie_hook.kill()
