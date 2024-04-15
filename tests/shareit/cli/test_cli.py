import unittest

from airflow.bin.cli import CLIFactory
from airflow.bin import cli
from airflow.shareit.utils.task_manager import get_dag
from airflow.utils.timezone import datetime


class CliTest(unittest.TestCase):
    def test_get_parse(self):
        CLIFactory.get_parser()

    # @mock.patch("airflow.bin.cli.jobs.LocalTaskJob")
    def test_cli_run(self):
            """
            Test that we can run naive (non-localized) task instances
            """
            parser = cli.CLIFactory.get_parser()
            NAIVE_DATE = datetime(2022, 12, 1)
            dag_id = 'test_reay_time1'

            dag = get_dag(dag_id)

            task0_id = 'test_reay_time1'
            args0 = ['run',
                     '-A',
                     '--local',
                     dag_id,
                     task0_id,
                     NAIVE_DATE.isoformat()]

            cli.run(parser.parse_args(args0), dag=dag)
            # mock_local_job.assert_called_with(
            #     task_instance=mock.ANY,
            #     mark_success=False,
            #     ignore_all_deps=True,
            #     ignore_depends_on_past=False,
            #     ignore_task_deps=False,
            #     ignore_ti_state=False,
            #     pickle_id=None,
            #     pool=None,
            # )
