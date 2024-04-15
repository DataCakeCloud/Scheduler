# -*- coding: utf-8 -*-
import multiprocessing

from airflow import LoggingMixin
from airflow.shareit.models.dag_dataset_relation import DagDatasetRelation
from airflow.shareit.utils.task_util import TaskUtil

log = LoggingMixin().logger




def external_processing(datasets):
    TaskUtil().external_data_processing(datasets)


class ExternalManager(LoggingMixin):
    def __init__(self):
        self._processors = {}
        # Pipe for communicating signals
        self._process = None
        self._done = False
        # Initialized as true so we do not deactivate w/o any actual DAG parsing.
        self._all_files_processed = True

        self._parent_signal_conn = None
        self._collected_dag_buffer = []

    @staticmethod
    def main_scheduler_process(check_parallelism):
        pool = multiprocessing.Pool(processes=check_parallelism)
        datasets = DagDatasetRelation.get_all_external_dataset()
        # merge_datasets = TaskUtil._merger_dataset_check_info(datasets=datasets)
        # merge_datasets_dict = merge_datasets[1]
        log.info("[DataStudio Pipeline] check_parallelism : {}".format(str(check_parallelism)))
        log.info("[DataStudio Pipeline] Main_scheduler_process start..., {} externals".format(len(datasets)))
        # datasets_num = len(datasets)
        divide_datasets = ExternalManager.list_split(datasets,len(datasets)/check_parallelism + 1)
        # divide_datasets = ExternalManager.divide_datasets(merge_datasets_dict,check_parallelism)
        log.info("[DataStudio Pipeline] divide_datasets length {}".format(len(divide_datasets)))
        for dataset in divide_datasets:
            log.info("[DataStudio Pipeline] split dataset length {}".format(len(dataset)))
            pool.apply_async(external_processing, (dataset,))

        pool.close()
        pool.join()

        log.info("[DataStudio Pipeline] Main_scheduler_process end...".format())

    @staticmethod
    def divide_datasets(merge_datasets_dict,check_parallelism):
        result = []
        flag = check_parallelism
        while flag > 0 :
            result.append([])
            flag -= 1

        index = 0
        for key, value in merge_datasets_dict.iteritems():
            if index < check_parallelism:
                result[index].extend(value)
                index += 1
            else :
                index = 0
                result[index].extend(value)

        return result




    @staticmethod
    def list_split(items, n):
        return [items[i:i + n] for i in range(0, len(items), n)]