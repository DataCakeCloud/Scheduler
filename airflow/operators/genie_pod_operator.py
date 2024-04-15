# -*- coding: utf-8 -*-

import os
import yaml

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.contrib.kubernetes import pod_generator
from airflow.contrib.kubernetes.pod import Resources
from airflow.contrib.kubernetes.kubernetes_request_factory import pod_request_factory as pod_factory
from airflow.utils.file import TemporaryDirectory
from airflow.utils.decorators import apply_defaults

from airflow.hooks.genie_hook import GenieHook


class GeniePodOperator(BaseOperator):
    """
    Execute a task in a kubernetes pod through genie.

    :param namespace: the namespace to run within kubernetes
    :type namespace: str
    :param image: Docker image you wish to launch. Defaults to dockerhub.io,
        but fully qualified URLS will point to custom repositories
    :type image: str
    :param cmds: entrypoint of the container. (templated)
        The docker images's entrypoint is used if this is not provide.
    :type cmds: list[str]
    :param arguments: arguments of the entrypoint. (templated)
        The docker image's CMD is used if this is not provided.
    :type arguments: list[str]
    :param image_pull_policy: Specify a policy to cache or always pull an image
    :type image_pull_policy: str
    :param image_pull_secrets: Any image pull secrets to be given to the pod.
                               If more than one secret is required, provide a
                               comma separated list: secret_a,secret_b
    :type image_pull_secrets: str
    :param ports: ports for launched pod
    :type ports: list[airflow.kubernetes.pod.Port]
    :param volume_mounts: volumeMounts for launched pod
    :type volume_mounts: list[airflow.contrib.kubernetes.volume_mount.VolumeMount]
    :param volumes: volumes for launched pod. Includes ConfigMaps and PersistentVolumes
    :type volumes: list[airflow.contrib.kubernetes.volume.Volume]
    :param labels: labels to apply to the Pod
    :type labels: dict
    :param startup_timeout_seconds: timeout in seconds to startup the pod
    :type startup_timeout_seconds: int
    :param name: name of the task you want to run,
        will be used to generate a pod id
    :type name: str
    :param env_vars: Environment variables initialized in the container. (templated)
    :type env_vars: dict
    :param secrets: Kubernetes secrets to inject in the container,
        They can be exposed as environment vars or files in a volume.
    :type secrets: list[airflow.contrib.kubernetes.secret.Secret]
    :param get_logs: get the stdout of the container as logs of the tasks
    :type get_logs: bool
    :param annotations: non-identifying metadata you can attach to the Pod.
                        Can be a large range of data, and can include characters
                        that are not permitted by labels.
    :type annotations: dict
    :param resources: A dict containing a group of resources requests and limits
    :type resources: dict
    :param affinity: A dict containing a group of affinity scheduling rules
    :type affinity: dict
    :param node_selectors: A dict containing a group of scheduling rules
    :type node_selectors: dict
    :param config_file: The path to the Kubernetes config file.
        If not specified, default value is ``~/.kube/config``
    :type config_file: str
    :param is_delete_operator_pod: What to do when the pod reaches its final
        state, or the execution is interrupted.
        If False (default): do nothing, If True: delete the pod
    :type is_delete_operator_pod: bool
    :param hostnetwork: If True enable host networking on the pod
    :type hostnetwork: bool
    :param tolerations: A list of kubernetes tolerations
    :type tolerations: list tolerations
    :param configmaps: A list of configmap names objects that we
        want mount as env variables
    :type configmaps: list[str]
    :param pod_runtime_info_envs: environment variables about
                                  pod runtime information (ip, namespace, nodeName, podName)
    :type pod_runtime_info_envs: list[PodRuntimeEnv]
    :param dnspolicy: Specify a dnspolicy for the pod
    :type dnspolicy: str
    :param genie_conn_id: reference to a genie service
    :type genie_conn_id: str
    :param cluster: value of the 'sched' tag for Genie to use when selecting which cluster to
        run the job on. Deprecated, use cluster_tags instead.
    :type cluster: str
    :param cluster_tags: tags for Genie to use when selecting which cluster to run the job on.
    :type cluster_tags: list or comma separated str.
    :param job_name: a name for the job. The name does not have to be unique.
    :type job_name: str
    :param timeout: the timeout(in seconds) for the job. If the job does not finish
        within the specified timeout, Genie will kill the job.
    :type: timeout: int
    """
    template_fields = ('name', 'cmds', 'arguments', 'env_vars')

    @apply_defaults
    def __init__(self,
                 namespace=None,
                 image=None,
                 name=None,
                 cmds=None,
                 arguments=None,
                 ports=None,
                 volume_mounts=None,
                 volumes=None,
                 env_vars=None,
                 secrets=None,
                 labels=None,
                 startup_timeout_seconds=7200,
                 get_logs=True,
                 image_pull_policy='IfNotPresent',
                 annotations=None,
                 resources=None,
                 affinity=None,
                 config_file=None,
                 node_selectors=None,
                 init_containers=None,
                 image_pull_secrets=None,
                 service_account_name="genie",
                 is_delete_operator_pod=False,
                 hostnetwork=False,
                 kind = None,
                 api_version=None,
                 tolerations=None,
                 configmaps=None,
                 security_context=None,
                 pod_runtime_info_envs=None,
                 dnspolicy=None,
                 cluster=None,
                 genie_conn_id='genie_default',
                 cluster_tags=None,
                 job_name=None,
                 timeout=None,
                 preStop_exec_cmds=None,
                 terminationGracePeriodSeconds=30,
                 *args,
                 **kwargs):
        super(GeniePodOperator, self).__init__(*args, **kwargs)
        self.image = image
        self.namespace = namespace
        self.cmds = cmds or []
        self.arguments = arguments or []
        self.labels = labels or {}
        self.startup_timeout_seconds = startup_timeout_seconds
        self.name = name
        self.env_vars = env_vars or {}
        self.ports = ports or []
        self.volume_mounts = volume_mounts or []
        self.volumes = volumes or []
        self.secrets = secrets or []
        self.get_logs = get_logs
        self.image_pull_policy = image_pull_policy
        self.node_selectors = node_selectors or {}
        self.init_containers = init_containers or []
        self.annotations = annotations or {}
        self.affinity = affinity or {}
        self.resources = self._set_resources(resources)
        self.config_file = config_file
        self.image_pull_secrets = image_pull_secrets
        self.service_account_name = service_account_name
        self.is_delete_operator_pod = is_delete_operator_pod
        self.hostnetwork = hostnetwork
        self.tolerations = tolerations or []
        self.configmaps = configmaps or []
        self.security_context = security_context or {}
        self.pod_runtime_info_envs = pod_runtime_info_envs or []
        self.dnspolicy = dnspolicy
        self.kind = kind or "Pod"
        self.api_version = api_version or "v1"

        self.kube_req_factory = pod_factory.SimplePodRequestFactory()
        self.job_name = job_name or kwargs['task_id']
        self.cluster = cluster
        self.genie_conn_id = genie_conn_id
        self.cluster_tags = cluster_tags
        self.timeout = timeout
        self.preStop_exec_cmds = preStop_exec_cmds
        self.terminationGracePeriodSeconds = terminationGracePeriodSeconds

    def execute(self, context):
        try:
            gen = pod_generator.PodGenerator()

            for port in self.ports:
                gen.add_port(port)
            for mount in self.volume_mounts:
                gen.add_mount(mount)
            for volume in self.volumes:
                gen.add_volume(volume)

            pod = gen.make_pod(
                namespace=self.namespace,
                image=self.image,
                pod_id=self.name,
                cmds=self.cmds,
                arguments=self.arguments,
                labels=self.labels,
            )
            pod.kind = self.kind
            pod.api_version = self.api_version
            pod.service_account_name = self.service_account_name
            pod.secrets = self.secrets
            pod.envs = self.env_vars
            pod.image_pull_policy = self.image_pull_policy
            pod.image_pull_secrets = self.image_pull_secrets
            pod.annotations = self.annotations
            pod.resources = self.resources
            pod.affinity = self.affinity
            pod.node_selectors = self.node_selectors
            pod.init_containers = self.init_containers
            pod.hostnetwork = self.hostnetwork
            pod.tolerations = self.tolerations
            pod.configmaps = self.configmaps
            pod.security_context = self.security_context
            pod.pod_runtime_info_envs = self.pod_runtime_info_envs
            pod.dnspolicy = self.dnspolicy
            pod.preStop_exec_cmds = self.preStop_exec_cmds
            pod.terminationGracePeriodSeconds = self.terminationGracePeriodSeconds
            group = None
            cts = self.cluster_tags
            if self.cluster_tags and (isinstance(self.cluster_tags, str) or isinstance(self.cluster_tags, unicode)):
                cts = [i.strip() for i in self.cluster_tags.split(",")]
            for tag in cts:
                if tag.startswith("rbac.cluster:"):
                    group = tag.split("-")[0].split('rbac.cluster:')[1]
            pod.labels = {"app": "spark", "env": "prod"}
            if group:
                pod.labels["group"] = group

            req = self.kube_req_factory.create(pod)
            dependencies = []
            ti = None
            if isinstance(context, dict) and "ti" in context.keys():
                ti = context['ti'] if self.task_id == context['ti'].task_id else None
            with TemporaryDirectory(prefix='airflowtmp') as tmp_dir:
                tmp_file_name = self.task_id + ".yaml"
                with open(os.path.join(tmp_dir, tmp_file_name), 'w') as tmp_file:
                    yaml.safe_dump(req, tmp_file, default_flow_style=False, encoding='utf-8', allow_unicode=True)
                    location = os.path.abspath(tmp_file.name)
                    self.log.info('Temporary file location: %s', location)
                    dependencies.append(location)
                    command = self._build_command(tmp_file_name)
                    genie_hook = GenieHook(genie_conn_id=self.genie_conn_id)
                    status = genie_hook.submit_job(cluster_type='k8s',
                                                   cluster=self.cluster,
                                                   command_type='kubernetes-pod-operator',
                                                   command=command,
                                                   cluster_tags=self.cluster_tags,
                                                   dependencies=dependencies,
                                                   job_name=self.job_name,
                                                   timeout=self.timeout,
                                                   task_ins=ti)
                    if status.upper() != "SUCCEEDED":
                        raise AirflowException('Genie job failed')
        except AirflowException as ex:
            raise AirflowException('Pod Launching failed: {error}'.format(error=ex))

    def _set_resources(self, resources):
        input_resource = Resources()
        if resources:
            for item in resources.keys():
                setattr(input_resource, item, resources[item])
        return input_resource

    def _build_command(self, pod_config_file):
        command = '--pod-config ' + pod_config_file \
                  + " --namespace " + self.namespace \
                  + " --startup-timeout-seconds " + str(self.startup_timeout_seconds)
        if self.is_delete_operator_pod:
            command = command + " --delete-pod"

        if self.get_logs:
            command = command + " --get-logs"

        return command

