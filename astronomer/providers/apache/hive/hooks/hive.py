import os
import re
import subprocess
import time
from collections import OrderedDict
from tempfile import NamedTemporaryFile, TemporaryDirectory
from typing import Any, Dict, List, Optional

import pandas
from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.providers.apache.hive.hooks.hive import HiveCliHook
from airflow.security import utils
from airflow.utils.helpers import as_flattened_list
from airflow.utils.operator_helpers import AIRFLOW_VAR_NAME_FORMAT_MAPPING

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

    :param hive_cli_conn_id: Reference to the
    :param mapred_queue: queue used by the Hadoop Scheduler (Capacity or Fair)
    :param mapred_queue_priority: priority within the job queue.
        Possible settings include: VERY_HIGH, HIGH, NORMAL, LOW, VERY_LOW
    :param mapred_job_name: This name will appear in the jobtracker.
        This can make monitoring easier.
    """

    conn_name_attr = "hive_cli_conn_id"
    default_conn_name = "hive_cli_default"
    conn_type = "hive_cli"
    hook_name = "Hive Client Wrapper"

    def __init__(
        self,
        hive_cli_conn_id: str = default_conn_name,
        run_as: Optional[str] = None,
        mapred_queue: Optional[str] = None,
        mapred_queue_priority: Optional[str] = None,
        mapred_job_name: Optional[str] = None,
    ) -> None:
        super().__init__()
        conn = self.get_connection(hive_cli_conn_id)
        self.hive_cli_params: str = conn.extra_dejson.get("hive_cli_params", "")
        self.use_beeline: bool = conn.extra_dejson.get("use_beeline", False)
        self.auth = conn.extra_dejson.get("auth", "noSasl")
        self.conn = conn
        self.run_as = run_as
        self.sub_process: Any = None

        if mapred_queue_priority:
            mapred_queue_priority = mapred_queue_priority.upper()
            if mapred_queue_priority not in HIVE_QUEUE_PRIORITIES:
                raise AirflowException(
                    f"Invalid Mapred Queue Priority. Valid values are: {', '.join(HIVE_QUEUE_PRIORITIES)}"
                )

        self.mapred_queue = mapred_queue or conf.get("hive", "default_hive_mapred_queue")
        self.mapred_queue_priority = mapred_queue_priority
        self.mapred_job_name = mapred_job_name

    def _get_proxy_user(self) -> str:
        """This function set the proper proxy_user value in case the user overwrite the default."""
        conn = self.conn

        proxy_user_value: str = conn.extra_dejson.get("proxy_user", "")
        if proxy_user_value == "login" and conn.login:
            return f"hive.server2.proxy.user={conn.login}"
        if proxy_user_value == "owner" and self.run_as:
            return f"hive.server2.proxy.user={self.run_as}"
        if proxy_user_value != "":  # There is a custom proxy user
            return f"hive.server2.proxy.user={proxy_user_value}"
        return proxy_user_value  # The default proxy user (undefined)

    def _prepare_cli_cmd(self) -> List[Any]:
        """This function creates the command list from available information"""
        conn = self.conn
        hive_bin = "hive"
        cmd_extra = []

        if self.use_beeline:
            hive_bin = "beeline"
            jdbc_url = f"jdbc:hive2://{conn.host}:{conn.port}/{conn.schema}"
            if conf.get("core", "security") == "kerberos":
                template = conn.extra_dejson.get("principal", "hive/_HOST@EXAMPLE.COM")
                if "_HOST" in template:
                    template = utils.replace_hostname_pattern(utils.get_components(template))

                proxy_user = self._get_proxy_user()

                jdbc_url += f";principal={template};{proxy_user}"
            elif self.auth:
                jdbc_url += ";auth=" + self.auth

            jdbc_url = f'"{jdbc_url}"'

            cmd_extra += ["-u", jdbc_url]
            if conn.login:
                cmd_extra += ["-n", conn.login]
            if conn.password:
                cmd_extra += ["-p", conn.password]

        hive_params_list = self.hive_cli_params.split()

        return [hive_bin] + cmd_extra + hive_params_list

    @staticmethod
    def _prepare_hiveconf(d: Dict[Any, Any]) -> List[Any]:
        """
        This function prepares a list of hiveconf params
        from a dictionary of key value pairs.

        :param d:

        >>> hh = HiveCliHook()
        >>> hive_conf = {"hive.exec.dynamic.partition": "true",
        ... "hive.exec.dynamic.partition.mode": "nonstrict"}
        >>> hh._prepare_hiveconf(hive_conf)
        ["-hiveconf", "hive.exec.dynamic.partition=true",\
        "-hiveconf", "hive.exec.dynamic.partition.mode=nonstrict"]
        """
        if not d:
            return []
        return as_flattened_list(zip(["-hiveconf"] * len(d), [f"{k}={v}" for k, v in d.items()]))

    def run_cli(
        self,
        hql: str,
        schema: Optional[str] = None,
        verbose: bool = True,
        hive_conf: Optional[Dict[Any, Any]] = None,
    ) -> Any:
        """
        Run an hql statement using the hive cli. If hive_conf is specified
        it should be a dict and the entries will be set as key/value pairs
        in HiveConf.

        :param hql: an hql (hive query language) statement to run with hive cli
        :param schema: Name of hive schema (database) to use
        :param verbose: Provides additional logging. Defaults to True.
        :param hive_conf: if specified these key value pairs will be passed
            to hive as ``-hiveconf "key"="value"``. Note that they will be
            passed after the ``hive_cli_params`` and thus will override
            whatever values are specified in the database.

        >>> hh = HiveCliHook()
        >>> result = hh.run_cli("USE airflow;")
        >>> ("OK" in result)
        True
        """
        conn = self.conn
        schema = schema or conn.schema
        if schema:
            hql = f"USE {schema};\n{hql}"

        with TemporaryDirectory(prefix="airflow_hiveop_") as tmp_dir:
            with NamedTemporaryFile(dir=tmp_dir) as f:
                hql += "\n"
                f.write(hql.encode("UTF-8"))
                f.flush()
                hive_cmd = self._prepare_cli_cmd()
                env_context = get_context_from_env_var()
                # Only extend the hive_conf if it is defined.
                if hive_conf:
                    env_context.update(hive_conf)
                hive_conf_params = self._prepare_hiveconf(env_context)
                if self.mapred_queue:
                    hive_conf_params.extend(
                        [
                            "-hiveconf",
                            f"mapreduce.job.queuename={self.mapred_queue}",
                            "-hiveconf",
                            f"mapred.job.queue.name={self.mapred_queue}",
                            "-hiveconf",
                            f"tez.queue.name={self.mapred_queue}",
                        ]
                    )

                if self.mapred_queue_priority:
                    hive_conf_params.extend(
                        ["-hiveconf", f"mapreduce.job.priority={self.mapred_queue_priority}"]
                    )

                if self.mapred_job_name:
                    hive_conf_params.extend(["-hiveconf", f"mapred.job.name={self.mapred_job_name}"])

                hive_cmd.extend(hive_conf_params)
                hive_cmd.extend(["-f", f.name])

                if verbose:
                    self.log.info("%s", " ".join(hive_cmd))
                sub_process: Any = subprocess.Popen(
                    hive_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, cwd=tmp_dir, close_fds=True
                )
                self.sub_process = sub_process
                stdout = ""
                while True:
                    line = sub_process.stdout.readline()
                    if not line:
                        break
                    stdout += line.decode("UTF-8")
                    if verbose:
                        self.log.info(line.decode("UTF-8").strip())
                sub_process.wait()

                if sub_process.returncode:
                    raise AirflowException(stdout)

                return stdout

    def test_hql(self, hql: str) -> None:
        """Test an hql statement using the hive cli and EXPLAIN"""
        create, insert, other = [], [], []
        for query in hql.split(";"):  # naive
            query_original = query
            query = query.lower().strip()

            if query.startswith("create table"):
                create.append(query_original)
            elif query.startswith(("set ", "add jar ", "create temporary function")):
                other.append(query_original)
            elif query.startswith("insert"):
                insert.append(query_original)
        other_ = ";".join(other)
        for query_set in [create, insert]:
            for query in query_set:

                query_preview = " ".join(query.split())[:50]
                self.log.info("Testing HQL [%s (...)]", query_preview)
                if query_set == insert:
                    query = other_ + "; explain " + query
                else:
                    query = "explain " + query
                try:
                    self.run_cli(query, verbose=False)
                except AirflowException as e:
                    message = e.args[0].split("\n")[-2]
                    self.log.info(message)
                    error_loc = re.search(r"(\d+):(\d+)", message)
                    if error_loc and error_loc.group(1).isdigit():
                        lst = int(error_loc.group(1))
                        begin = max(lst - 2, 0)
                        end = min(lst + 3, len(query.split("\n")))
                        context = "\n".join(query.split("\n")[begin:end])
                        self.log.info("Context :\n %s", context)
                else:
                    self.log.info("SUCCESS")

    def load_df(
        self,
        df: pandas.DataFrame,
        table: str,
        field_dict: Optional[Dict[Any, Any]] = None,
        delimiter: str = ",",
        encoding: str = "utf8",
        pandas_kwargs: Any = None,
        **kwargs: Any,
    ) -> None:
        """
        Loads a pandas DataFrame into hive.

        Hive data types will be inferred if not passed but column names will
        not be sanitized.

        :param df: DataFrame to load into a Hive table
        :param table: target Hive table, use dot notation to target a
            specific database
        :param field_dict: mapping from column name to hive data type.
            Note that it must be OrderedDict so as to keep columns' order.
        :param delimiter: field delimiter in the file
        :param encoding: str encoding to use when writing DataFrame to file
        :param pandas_kwargs: passed to DataFrame.to_csv
        :param kwargs: passed to self.load_file
        """

        def _infer_field_types_from_df(df: pandas.DataFrame) -> Dict[Any, Any]:
            dtype_kind_hive_type = {
                "b": "BOOLEAN",  # boolean
                "i": "BIGINT",  # signed integer
                "u": "BIGINT",  # unsigned integer
                "f": "DOUBLE",  # floating-point
                "c": "STRING",  # complex floating-point
                "M": "TIMESTAMP",  # datetime
                "O": "STRING",  # object
                "S": "STRING",  # (byte-)string
                "U": "STRING",  # Unicode
                "V": "STRING",  # void
            }

            order_type = OrderedDict()
            for col, dtype in df.dtypes.iteritems():
                order_type[col] = dtype_kind_hive_type[dtype.kind]
            return order_type

        if pandas_kwargs is None:
            pandas_kwargs = {}

        with TemporaryDirectory(prefix="airflow_hiveop_") as tmp_dir:
            with NamedTemporaryFile(dir=tmp_dir, mode="w") as f:
                if field_dict is None:
                    field_dict = _infer_field_types_from_df(df)

                df.to_csv(
                    path_or_buf=f,
                    sep=delimiter,
                    header=False,
                    index=False,
                    encoding=encoding,
                    date_format="%Y-%m-%d %H:%M:%S",
                    **pandas_kwargs,
                )
                f.flush()

                return self.load_file(
                    filepath=f.name, table=table, delimiter=delimiter, field_dict=field_dict, **kwargs
                )

    def load_file(
        self,
        filepath: str,
        table: str,
        delimiter: str = ",",
        field_dict: Optional[Dict[Any, Any]] = None,
        create: bool = True,
        overwrite: bool = True,
        partition: Optional[Dict[str, Any]] = None,
        recreate: bool = False,
        tblproperties: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Loads a local file into Hive

        Note that the table generated in Hive uses ``STORED AS textfile``
        which isn't the most efficient serialization format. If a
        large amount of data is loaded and/or if the tables gets
        queried considerably, you may want to use this operator only to
        stage the data into a temporary table before loading it into its
        final destination using a ``HiveOperator``.

        :param filepath: local filepath of the file to load
        :param table: target Hive table, use dot notation to target a
            specific database
        :param delimiter: field delimiter in the file
        :param field_dict: A dictionary of the fields name in the file
            as keys and their Hive types as values.
            Note that it must be OrderedDict so as to keep columns' order.
        :param create: whether to create the table if it doesn't exist
        :param overwrite: whether to overwrite the data in table or partition
        :param partition: target partition as a dict of partition columns
            and values
        :param recreate: whether to drop and recreate the table at every
            execution
        :param tblproperties: TBLPROPERTIES of the hive table being created
        """
        hql = ""
        if recreate:
            hql += f"DROP TABLE IF EXISTS {table};\n"
        if create or recreate:
            if field_dict is None:
                raise ValueError("Must provide a field dict when creating a table")
            fields = ",\n    ".join(f"`{k.strip('`')}` {v}" for k, v in field_dict.items())
            hql += f"CREATE TABLE IF NOT EXISTS {table} (\n{fields})\n"
            if partition:
                pfields = ",\n    ".join(p + " STRING" for p in partition)
                hql += f"PARTITIONED BY ({pfields})\n"
            hql += "ROW FORMAT DELIMITED\n"
            hql += f"FIELDS TERMINATED BY '{delimiter}'\n"
            hql += "STORED AS textfile\n"
            if tblproperties is not None:
                tprops = ", ".join(f"'{k}'='{v}'" for k, v in tblproperties.items())
                hql += f"TBLPROPERTIES({tprops})\n"
            hql += ";"
            self.log.info(hql)
            self.run_cli(hql)
        hql = f"LOAD DATA LOCAL INPATH '{filepath}' "
        if overwrite:
            hql += "OVERWRITE "
        hql += f"INTO TABLE {table} "
        if partition:
            pvals = ", ".join(f"{k}='{v}'" for k, v in partition.items())
            hql += f"PARTITION ({pvals})"

        # As a workaround for HIVE-10541, add a newline character
        # at the end of hql (AIRFLOW-2412).
        hql += ";\n"

        self.log.info(hql)
        self.run_cli(hql)

    def kill(self) -> None:
        """Kill Hive cli command"""
        if hasattr(self, "sub_process"):
            if self.sub_process.poll() is None:
                print("Killing the Hive job")
                self.sub_process.terminate()
                time.sleep(60)
                self.sub_process.kill()
