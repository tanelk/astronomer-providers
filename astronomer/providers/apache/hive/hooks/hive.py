import os
from typing import Any, Dict

from airflow.providers.apache.hive.hooks.hive import HiveCliHook
from airflow.utils.operator_helpers import AIRFLOW_VAR_NAME_FORMAT_MAPPING
from pyhive import hive

HIVE_QUEUE_PRIORITIES = ["VERY_HIGH", "HIGH", "NORMAL", "LOW", "VERY_LOW"]


def get_context_from_env_var() -> Dict[Any, Any]:
    """
    Extract context from env variable, e.g. dag_id, task_id and execution_date,
    so that they can be used inside BashOperator and PythonOperator.

    :return: The context of interest.
    """
    return {
        format_map["default"]: os.environ.get(format_map["env_var_format"], "")
        for format_map in AIRFLOW_VAR_NAME_FORMAT_MAPPING.values()
    }


class HiveCliHookAsync(HiveCliHook):
    """Simple wrapper around the hive CLI.

    It also supports the ``beeline``
    a lighter CLI that runs JDBC and is replacing the heavier
    traditional CLI. To enable ``beeline``, set the use_beeline param in the
    extra field of your connection as in ``{ "use_beeline": true }``

    Note that you can also set default hive CLI parameters using the
    ``hive_cli_params`` to be used in your connection as in
    ``{"hive_cli_params": "-hiveconf mapred.job.tracker=some.jobtracker:444"}``
    Parameters passed here can be overridden by run_cli's hive_conf param

    The extra connection parameter ``auth`` gets passed as in the ``jdbc``
    connection string as is.

    :param hive_cli_conn_id: Connection to the hive cluster.
    :param mapred_queue: queue used by the Hadoop Scheduler (Capacity or Fair)
    :param mapred_queue_priority: priority within the job queue.
        Possible settings include: VERY_HIGH, HIGH, NORMAL, LOW, VERY_LOW
    :param mapred_job_name: This name will appear in the jobtracker.
        This can make monitoring easier.
    """

    def get_hive_client(self) -> hive.Connection:
        """Makes a connection to the hive client"""
        return hive.connect(host=self.conn.host, port=self.conn.port, database=self.conn.schema)
