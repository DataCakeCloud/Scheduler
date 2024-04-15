# -*- coding: utf-8 -*-
import time

from pygenie.adapter import Genie3Adapter
from pygenie.exceptions import GenieJobNotFoundError
from pygenie.jobs.hadoop import HadoopJob
from pygenie.conf import GenieConf
from pygenie.conf import GenieConfSection
from pygenie.client import Genie
from pygenie.utils import str_to_list

from airflow import AirflowException
from airflow.hooks.base_hook import BaseHook


class GenieHook(BaseHook):
    """
    Hook to interact with genie.
    """

    def __init__(self,
                 conf=None,
                 genie_conn_id='genie_default'):
        self.conf = conf
        self.genie_conn_id = genie_conn_id
        self.base_url = None
        self.username = None
        self.password = None
        self.running_job = None
        self.genie_conf = self._resolve_connection()

    def _resolve_connection(self):
        try:
            conn = self.get_connection(self.genie_conn_id)

            if "://" in conn.host:
                self.base_url = conn.host
            else:
                # schema defaults to HTTP
                schema = conn.schema if conn.schema else "http"
                self.base_url = schema + "://" + conn.host

            if conn.port:
                self.base_url = self.base_url + ":" + str(conn.port)
            if conn.login:
                self.username = conn.login
                self.password = conn.password
        except Exception as e:
            print("Failed to get connection: %s", e.message)

        if self.conf:
            if isinstance(self.conf, dict):
                if self.conf.has_key('url'):
                    self.base_url = self.conf['url']

                if self.conf.has_key('username'):
                    self.username = self.conf['username']

                if self.conf.has_key('password'):
                    self.password = self.conf['password']

        genie_conf = GenieConf()
        genie_conf.genie.set('version', '3')
        genie_conf.genie.set('auth', 'pygenie.auth.HTTPBasicGenieAuth')
        genie_conf.genie.set('url', self.base_url)
        genie_conf.genie.set('username', self.username)
        genie_auth = GenieConfSection(name='genie_auth')
        genie_auth.set('username', self.username)
        genie_auth.set('password', self.password)
        genie_conf.__dict__.__setitem__('genie_auth', genie_auth)

        return genie_conf

    def get_conn(self):
        """
        Get the genie client.

        :return: the genie client
        """
        return Genie(self.genie_conf)

    def submit_job(self,
                   cluster_sla=None,
                   cluster_type=None,
                   cluster=None,
                   command_type=None,
                   command=None,
                   cluster_tags=None,
                   command_tags=None,
                   dependencies=None,
                   job_name=None,
                   setup_file=None,
                   cpu=None,
                   memory=None,
                   timeout=None,
                   task_ins=None):
        """
        Submit a job to genie.

        :param task_ins: this taskInstance
        :param cluster_sla: value of the 'sla' tag for Genie to use when selecting which cluster to
            run the job on. Deprecated, use cluster_tags instead.
        :type cluster_type: str normal/high
        :param cluster_type: value of the 'type' tag for Genie to use when selecting which cluster to
            run the job on. Deprecated, use cluster_tags instead.
        :type cluster_type: str
        :param cluster: value of the 'sched' tag for Genie to use when selecting which cluster to
            run the job on. Deprecated, use cluster_tags instead.
        :type cluster: str
        :param command_type: value of the 'type' tag for Genie to use when selecting which command to
            use when executing the job. Deprecated, use command_tags instead.
        :type command_type: str
        :param command: the command to run for the job.
        :type command: str
        :param cluster_tags: tags for Genie to use when selecting which cluster to run the job on.
        :type: cluster_tags: list or comma separated str.
        :param command_tags: tags for Genie to use when selecting which command to use when executing the job.
        :type: command_tags: list or comma separated str.
        :param dependencies: file dependencies for the Genie job.
        :type: dependencies: list
        :param job_name: a name for the job. The name does not have to be unique.
        :type job_name: str
        :param setup_file: a Bash file to source before the job is executed.
        :type setup_file: str
        :param cpu: the number of millicore(m) for Genie to allocate when executing the job.
        :type cpu: int
        :param memory: the amount of memory(MB) for Genie to allocate when executing the job.
        :type memory: int
        :param timeout: the timeout (in seconds) for the job. If the job does not finish within the
            specified timeout, Genie will kill the job.
        :type memory: int
        :return: the job status
        """
        _cluster_tags = []
        if cluster_tags:
            if isinstance(cluster_tags, list):
                _cluster_tags.extend(cluster_tags)
            elif isinstance(cluster_tags, str) or isinstance(cluster_tags, unicode):
                _cluster_tags.extend(str_to_list(cluster_tags))
            else:
                raise AirflowException("cluster_tags is not list or str")
        if cluster:
            _cluster_tags.append('sched:' + cluster)
        if cluster_type:
            # "type" is subject to cluster_tags
            ct_flag = True
            for tag in _cluster_tags:
                if tag.startswith("type:"):
                    ct_flag = False
                    break
            if ct_flag:
                _cluster_tags.append('type:' + cluster_type)
        if cluster_sla:
            cs_flag = True
            for tag in _cluster_tags:
                if tag.startswith("sla:"):
                    cs_flag = False
                    break
            if cs_flag:
                _cluster_tags.append('sla:' + cluster_sla)

        _command_tags = []
        if command_tags:
            if isinstance(command_tags, list):
                _command_tags.extend(command_tags)
            elif isinstance(command_tags, str) or isinstance(command_tags, unicode):
                _command_tags.extend(str_to_list(command_tags))
            else:
                raise AirflowException("command_tags is not list or str")
        if command_type:
            _command_tags.append('type:' + command_type)

        job = HadoopJob(self.genie_conf)
        job.cluster_tags(_cluster_tags)
        job.command_tags(_command_tags)
        job.command(command)

        if job_name:
            job.job_name(job_name)

        if dependencies:
            job.dependencies(dependencies)

        if setup_file:
            job.genie_setup_file(setup_file)

        if cpu:
            job.genie_cpu(cpu)

        if memory:
            job.genie_memory(memory)

        if timeout:
            job.genie_timeout(timeout)

        self.running_job = job.execute()
        try:
            if task_ins:
                ex_conf = task_ins.get_external_conf()
                ex_conf = task_ins.format_external_conf(ex_conf)
                ex_conf['genie_job_id'] = self.running_job.job_id
                if ex_conf is not None and isinstance(ex_conf, dict) and "callback_info" in ex_conf.keys():
                    task_ins.save_external_conf(ex_conf=ex_conf)
                    # 回调，把genie_id传回其他系统
                    try:
                        from airflow.utils.state import State
                        from airflow.shareit.utils.callback_util import CallbackUtil
                        callback_info = ex_conf['callback_info']
                        url = callback_info['callback_url']
                        data = {"callback_id": callback_info['callback_id'], "state": State.RUNNING,
                                "instance_id": self.running_job.job_id,"args":callback_info['extra_info']}
                        CallbackUtil.callback_post_guarantee(url,data,task_ins.dag_id,task_ins.execution_date,self.running_job.job_id)
                    except Exception as e:
                        self.log.error("[DataStudio pipeline] Callback ti state request failed: " + str(e))
                else:
                    task_ins.save_external_conf(ex_conf=ex_conf)
        except Exception as e:
            self.log.error("Can not save genie job id " + str(e))
        self.log.info('Job {0} is {1}'.format(self.running_job.job_id, self.running_job.status))
        self.log.info('Job link: ' + self.running_job.job_link)
        self.running_job.wait()
        if self.running_job.status == "FAILED":
            self.log.info('Job failed, start recording stderr of the job execution:')
            errs = self.running_job.stderr(iterator=True)
            for err in errs:
                self.log.info(err)
            self.log.info('Finish recording stderr of the job execution.')

        return self.running_job.status

    def submit_job_with_no_status(self,
                                  cluster_type=None,
                                  cluster=None,
                                  command_type=None,
                                  command=None,
                                  cluster_tags=None,
                                  command_tags=None,
                                  dependencies=None,
                                  job_name=None,
                                  setup_file=None,
                                  cpu=None,
                                  memory=None,
                                  timeout=None):
        """
        Submit a job to genie.

        :param cluster_type: value of the 'type' tag for Genie to use when selecting which cluster to
            run the job on. Deprecated, use cluster_tags instead.
        :type cluster_type: str
        :param cluster: value of the 'sched' tag for Genie to use when selecting which cluster to
            run the job on. Deprecated, use cluster_tags instead.
        :type cluster: str
        :param command_type: value of the 'type' tag for Genie to use when selecting which command to
            use when executing the job. Deprecated, use command_tags instead.
        :type command_type: str
        :param command: the command to run for the job.
        :type command: str
        :param cluster_tags: tags for Genie to use when selecting which cluster to run the job on.
        :type: cluster_tags: list or comma separated str.
        :param command_tags: tags for Genie to use when selecting which command to use when executing the job.
        :type: command_tags: list or comma separated str.
        :param dependencies: file dependencies for the Genie job.
        :type: dependencies: list
        :param job_name: a name for the job. The name does not have to be unique.
        :type job_name: str
        :param setup_file: a Bash file to source before the job is executed.
        :type setup_file: str
        :param cpu: the number of millicore(m) for Genie to allocate when executing the job.
        :type cpu: int
        :param memory: the amount of memory(MB) for Genie to allocate when executing the job.
        :type memory: int
        :param timeout: the timeout (in seconds) for the job. If the job does not finish within the
            specified timeout, Genie will kill the job.
        :type memory: int
        :return: the job status
        """
        _cluster_tags = []
        if cluster_tags:
            if isinstance(cluster_tags, list):
                _cluster_tags.extend(cluster_tags)
            elif isinstance(cluster_tags, str):
                _cluster_tags.extend(str_to_list(cluster_tags))
            else:
                raise AirflowException("cluster_tags is not list or str")
        if cluster:
            _cluster_tags.append('sched:' + cluster)
        if cluster_type:
            # "type" is subject to cluster_tags
            ct_flag = True
            for tag in _cluster_tags:
                if tag.startswith("type:"):
                    ct_flag = False
                    break
            if ct_flag:
                _cluster_tags.append('type:' + cluster_type)

        _command_tags = []
        if command_tags:
            if isinstance(command_tags, list):
                _command_tags.extend(command_tags)
            elif isinstance(command_tags, str):
                _command_tags.extend(str_to_list(command_tags))
            else:
                raise AirflowException("command_tags is not list or str")
        if command_type:
            _command_tags.append('type:' + command_type)

        job = HadoopJob(self.genie_conf)
        job.cluster_tags(_cluster_tags)
        job.command_tags(_command_tags)
        job.command(command)

        if job_name:
            job.job_name(job_name)

        if dependencies:
            job.dependencies(dependencies)

        if setup_file:
            job.genie_setup_file(setup_file)

        if cpu:
            job.genie_cpu(cpu)

        if memory:
            job.genie_memory(memory)

        if timeout:
            job.genie_timeout(timeout)

        self.running_job = job.execute()
        self.log.info('Job {0} is {1}'.format(self.running_job.job_id, self.running_job.status))
        self.log.info('Job link: ' + self.running_job.job_link)
        # self.running_job.wait()
        time.sleep(10)
        if self.running_job.status == "FAILED":
            self.log.info('Job failed, start recording stderr of the job execution:')
            errs = self.running_job.stderr(iterator=True)
            for err in errs:
                self.log.info(err)
            self.log.info('Finish recording stderr of the job execution.')
        # After the task is submitted, success will be returned regardless of the status
        return "SUCCEEDED"

    def kill(self):
        """
        kill running job
        :return:
        """
        self.running_job.kill()

    def kill_job_by_id(self, job_id):
        client = Genie3Adapter(conf=self.genie_conf)
        client.kill_job(job_id)

    def check_job_exists(self,job_id):
        try:
            client = Genie3Adapter(conf=self.genie_conf)
            client.get(job_id)
            return True
        except GenieJobNotFoundError :
            self.log.info('genie job not exists : {}'.format(job_id))
            return False
        except Exception as e:
            self.log.error('genie job check failed : {}'.format(str(e)))
            return False


    @staticmethod
    def file_exist(job, not_exist_message='The file does not exist yet'):
        message = next(job.stdout(iterator=True), "")
        if message == "":
            return True
        elif message == not_exist_message:
            return False
        else:
            raise AirflowException('message: {0} is error'.format(message))

    def get_job_info(self,job_id):
        try:
            client = Genie3Adapter(conf=self.genie_conf)
            job = client.get(job_id)
            return job
        except GenieJobNotFoundError :
            self.log.info('genie job not exists : {}'.format(job_id))
            return None
        except Exception as e:
            self.log.error('genie job check failed : {}'.format(str(e)))
            return None