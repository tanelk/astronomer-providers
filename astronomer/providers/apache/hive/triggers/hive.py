import asyncio
import logging
from typing import Any, AsyncIterator, Dict, Optional, Tuple

from airflow.triggers.base import BaseTrigger, TriggerEvent

log = logging.getLogger(__name__)


class HiveTrigger(BaseTrigger):
    """
    A trigger that fires and it finds the requested file or folder present in the given bucket.

    :param bucket: the bucket in the google cloud storage where the objects are residing.
    :param object_name: the file or folder present in the bucket
    :param google_cloud_conn_id: reference to the Google Connection
    :param polling_period_seconds: polling period in seconds to check for file/folder
    """

    def __init__(
        self,
        *,
        hql: str,
        hive_cli_conn_id: str = "hive_cli_default",
        schema: str = "default",
        hiveconfs: Optional[Dict[Any, Any]] = None,
        hiveconf_jinja_translate: bool = False,
        script_begin_tag: Optional[str] = None,
        run_as_owner: bool = False,
        mapred_queue: Optional[str] = None,
        mapred_queue_priority: Optional[str] = None,
        mapred_job_name: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.hql = hql
        self.hive_cli_conn_id = hive_cli_conn_id
        self.schema = schema
        self.hiveconfs = hiveconfs or {}
        self.hiveconf_jinja_translate = hiveconf_jinja_translate
        self.script_begin_tag = script_begin_tag
        self.run_as = None
        if run_as_owner:
            self.run_as = self.dag.owner
        self.mapred_queue = mapred_queue
        self.mapred_queue_priority = mapred_queue_priority
        self.mapred_job_name = mapred_job_name
        # self.mapred_job_name_template = conf.get(
        #     "hive",
        #     "mapred_job_name_template",
        #     fallback="Airflow HiveOperator task for {hostname}.{dag_id}.{task_id}.{execution_date}",
        # )

        # assigned lazily - just for consistency we can create the attribute with a
        # `None` initial value, later it will be populated by the execute method.
        # This also makes `on_kill` implementation consistent since it assumes `self.hook`
        # is defined.
        # self.hook: Optional[HiveCliHook] = None

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Serializes GCSBlobTrigger arguments and classpath."""
        return (
            "astronomer.providers.google.cloud.triggers.gcs.GCSBlobTrigger",
            {
                "bucket": self.bucket,
                "object_name": self.object_name,
                "polling_period_seconds": self.polling_period_seconds,
                "google_cloud_conn_id": self.google_cloud_conn_id,
                "hook_params": self.hook_params,
            },
        )

    async def run(self) -> AsyncIterator["TriggerEvent"]:  # type: ignore[override]
        """Simple loop until the relevant file/folder is found."""
        try:
            hook = self._get_async_hook()
            while True:
                res = await self._object_exists(
                    hook=hook, bucket_name=self.bucket, object_name=self.object_name
                )
                if res == "success":
                    yield TriggerEvent({"status": "success", "message": res})
                    return
                await asyncio.sleep(self.polling_period_seconds)
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})
            return

    # def get_hook(self) -> HiveCliHook:
    #     """Get Hive cli hook"""
    #     return HiveCliHook(
    #         hive_cli_conn_id=self.hive_cli_conn_id,
    #         run_as=self.run_as,
    #         mapred_queue=self.mapred_queue,
    #         mapred_queue_priority=self.mapred_queue_priority,
    #         mapred_job_name=self.mapred_job_name,
    #     )
