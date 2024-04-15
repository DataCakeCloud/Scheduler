# -*- coding: utf-8 -*-
import re

from airflow.shareit.utils.parse_args_util import ParseArgs


class SparkUtil(object):
    @staticmethod
    def spark_param_parsed(command):
        class_pattern = re.compile('--class\s+([^\s]+)')
        jar_py_pattern = re.compile('\s+-+[^\s]+\s+[^\s-][^\s]+\s+([^-\s][^\s]+(jar|py))')
        class_path = re.search(class_pattern, command)
        jar_py = re.search(jar_py_pattern, command)
        is_spak_sql = False
        class_name = None
        jar_py_path = None
        if class_path:
            class_name = class_path.group(1)
        if jar_py:
            jar_py_path = jar_py.group(1)
            param_list = command.split(jar_py.group())
            if jar_py.group().strip().startswith('--conf'):
                param_list[0] =param_list[0] + ' ' + jar_py.group().replace(jar_py.group(1), '')
        else:
            param_list = command.split(' -e ')
            is_spak_sql = True
            class_name = ''
            jar_py_path = ''

        # conf_pattern = re.compile('\s+-+([^\s]+)\s+([^\s]+)')
        conf_pattern = re.compile('\s+-+([^\s=]+)(\s+|=)([^\s]+)')

        spark_conf_dict = {}
        spark_param_list = []
        param_formatted = re.sub('\s+=\s+','=',param_list[0])
        patterned_param = re.findall(conf_pattern, param_formatted)
        for param in patterned_param:
            if param[0] == "conf":
                kv_param = param[2].split('=',1)
                if kv_param[0] == 'spark.kubernetes.driverEnv.HADOOP_USER_NAME':
                    if '#' not in kv_param[1]:
                        continue
                spark_conf_dict[kv_param[0]] = kv_param[1]
            elif param[0] == "o":
                spark_param_list.extend(("-o", param[2]))
            elif param[0] == 'sep':
                spark_param_list.extend(("-sep", param[2]))
            elif param[0] == 'e':
                spark_param_list.extend(("-e", param[2]))
            elif param[0] == "class":
                continue
            else:
                # spark_conf_dict["spark." + param[0].replace("-", ".")] = param[2].lstrip('"').rstrip('"')
                spark_conf_dict[SparkUtil.submit_args_mapping(param[0])] = param[2].lstrip('"').rstrip('"')
        if is_spak_sql:
            spark_param_list.append("-e")
            spark_param_list.append(param_list[1].replace(' ','').replace('"',''))
        else:
            param_arr = ParseArgs.parse(param_list[1])
            spark_param_list.extend(param_arr)

        return class_name,jar_py_path,spark_conf_dict,spark_param_list

    @staticmethod
    def submit_args_mapping(arg):
        dict = {
            'deploy-mode':'spark.submit.deployMode',
            'executor-memory':'spark.executor.memory',
            'executor-cores':'spark.executor.cores',
            'total-executor-cores':'spark.cores.max',
            # 'properties-file':'',
            'driver-cores':'spark.driver.cores',
            'driver-memory':'spark.driver.memory',
            'driver-class-path ':'spark.driver.extraClassPath',
            'driver-library-path':'spark.driver.extraLibraryPath',
            'driver-java-options':'spark.driver.extraJavaOptions',
            'queue':'spark.yarn.queue',
            'num-executors':'spark.executor.instances',
            'files':'spark.files',
            'archives':'spark.archives',
            # 'main-class':'',
            # 'primary-resource':'',
            # 'name':'',
            'jars':'spark.jars',
            'packages':'spark.jars.packages',
            'repositories':'spark.jars.repositories',
            'py-files':'spark.submit.pyFiles',
            'ivy-repo-path':'spark.jars.ivy',
            'ivy-settings-path':'spark.jars.ivySettings',
            'exclude-packages':'spark.jars.excludes',
            # 'verbose':'',
            # 'action':'',
            # 'proxy-user':'',
            'principal':'spark.kerberos.principal',
            'keytab':'spark.kerberos.keyta'
        }
        if arg in dict.keys():
            return dict[arg]
        else:
            return arg