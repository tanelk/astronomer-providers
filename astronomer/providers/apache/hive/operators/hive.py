import os
import re
from typing import TYPE_CHECKING, Any, Dict

from airflow import AirflowException
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.utils import operator_helpers
from airflow.utils.operator_helpers import context_to_airflow_vars
from TCLIService.ttypes import TOperationState

from astronomer.providers.apache.hive.hooks.hive import HiveCliHookAsync
from astronomer.providers.apache.hive.triggers.hive import HiveTrigger

if TYPE_CHECKING:
    from airflow.utils.context import Context


class HiveOperatorAsync(HiveOperator):
    """
    Executes hql code or hive script in a specific Hive database.

    :param hql: the hql to be executed. Note that you may also use
        a relative path from the dag file of a (template) hive
        script. (templated)
    :param hive_cli_conn_id: Connection to the hive cluster.
    :param hiveconfs: if defined, these key value pairs will be passed
        to hive as ``-hiveconf "key"="value"``
    :param hiveconf_jinja_translate: when True, hiveconf-type templating
        ${var} gets translated into jinja-type templating {{ var }} and
        ${hiveconf:var} gets translated into jinja-type templating {{ var }}.
        Note that you may want to use this along with the
        ``DAG(user_defined_macros=myargs)`` parameter. View the DAG
        object documentation for more details.
    :param script_begin_tag: If defined, the operator will get rid of the
        part of the script before the first occurrence of `script_begin_tag`
    :param run_as_owner: Run HQL code as a DAG's owner.
    :param mapred_queue: queue used by the Hadoop CapacityScheduler. (templated)
    :param mapred_queue_priority: priority within CapacityScheduler queue.
        Possible settings include: VERY_HIGH, HIGH, NORMAL, LOW, VERY_LOW
    :param mapred_job_name: This name will appear in the jobtracker.
        This can make monitoring easier.
    """

    def get_hook(self) -> HiveCliHookAsync:
        """Get Hive cli hook"""
        return HiveCliHookAsync(hive_cli_conn_id=self.hive_cli_conn_id)

    def prepare_template(self) -> None:
        """Prepare the template for the beeline  hql command"""
        if self.hiveconf_jinja_translate:
            self.hql = re.sub(r"(\$\{(hiveconf:)?([ a-zA-Z0-9_]*)\})", r"{{ \g<3> }}", self.hql)
        if self.script_begin_tag and self.script_begin_tag in self.hql:
            self.hql = "\n".join(self.hql.split(self.script_begin_tag)[1:])

    def execute(self, context: "Context") -> None:
        """
        Gets the  hive connection from hooks and execute the cursor async
        Then use the cursor in trigger for polling for the executed queries.
        """
        self.log.info("Executing: %s", self.hql)
        self.hook = self.get_hook()

        # set the mapred_job_name if it's not set with dag, task, execution time info
        if not self.mapred_job_name:
            ti = context["ti"]
            self.hook.mapred_job_name = self.mapred_job_name_template.format(
                dag_id=ti.dag_id,
                task_id=ti.task_id,
                execution_date=ti.execution_date.isoformat(),
                hostname=ti.hostname.split(".")[0],
            )

        if self.hiveconf_jinja_translate:
            self.hiveconfs = context_to_airflow_vars(context)
        else:
            self.hiveconfs.update(context_to_airflow_vars(context))

        self.log.info("Passing HiveConf: %s", self.hiveconfs)

        cursor = self.hook.get_hive_client().cursor()
        cursor.execute(self.hql, async_=True)
        try:
            status = cursor.poll().operationState
            while status in (TOperationState.INITIALIZED_STATE, TOperationState.RUNNING_STATE):
                logs = cursor.fetch_logs()
                for message in logs:
                    print(message)

                # If needed, an asynchronous query can be cancelled at any time with:
                # cursor.cancel()

                status = cursor.poll().operationState

            print(cursor.fetchall())
        except Exception as e:
            print(e)

        # self.defer(
        #     timeout=self.execution_timeout,
        #     trigger=HiveTrigger(
        #         cursor=cursor,
        #     ),
        #     method_name="execute_complete",
        # )

    def execute_complete(self, context: Dict[str, Any], event: Dict[str, str]) -> str:
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event["status"] == "error":
            raise AirflowException(event["message"])
        self.log.info("Passing HiveConf: %s", self.hiveconfs)
        self.log.info(event["message"])
        return event["message"]

    def dry_run(self) -> None:
        """
        Reset airflow environment variables to prevent
        existing env vars from impacting behavior.
        """
        self.clear_airflow_vars()

        self.hook = self.get_hook()
        self.hook.test_hql(hql=self.hql)

    def on_kill(self) -> None:
        """Kill the connection and stop the execution"""
        if self.hook:
            self.hook.kill()

    def clear_airflow_vars(self) -> None:
        """Reset airflow environment variables to prevent existing ones from impacting behavior."""
        blank_env_vars = {
            value["env_var_format"]: "" for value in operator_helpers.AIRFLOW_VAR_NAME_FORMAT_MAPPING.values()
        }
        os.environ.update(blank_env_vars)
