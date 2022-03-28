import asyncio
import logging
from typing import Any, AsyncIterator, Dict, Tuple

from airflow.triggers.base import BaseTrigger, TriggerEvent
from pyhive.hive import Cursor
from TCLIService.ttypes import TOperationState

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
        cursor: Cursor,
        polling_period_seconds: float = 4.0,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self.cursor = cursor
        self.polling_period_seconds = polling_period_seconds

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Serializes HiveTrigger arguments and classpath."""
        return (
            "astronomer.providers.apache.hive.triggers.hive.HiveTrigger",
            {
                "cursor": self.cursor,
                "polling_period_seconds": self.polling_period_seconds,
            },
        )

    async def run(self) -> AsyncIterator["TriggerEvent"]:  # type: ignore[override]
        """Simple loop until the relevant file/folder is found."""
        try:
            status = self.cursor.poll().operationState
            while True:
                if status in (TOperationState.INITIALIZED_STATE, TOperationState.RUNNING_STATE):
                    logs = self.cursor.fetch_logs()
                    for message in logs:
                        print(message)
                        # If needed, an asynchronous query can be cancelled at any time with:
                        # cursor.cancel()
                    await asyncio.sleep(self.polling_period_seconds)
                    status = self.cursor.poll().operationState
                    print(self.cursor.fetchall())
                    yield TriggerEvent({"status": "error", "message": self.cursor.fetchall()})
                    return
                elif status == TOperationState.FINISHED_STATE:
                    logs = self.cursor.fetch_logs()
                    for message in logs:
                        print(message)
                        # If needed, an asynchronous query can be cancelled at any time with:
                        # cursor.cancel()
                    await asyncio.sleep(self.polling_period_seconds)
                    status = self.cursor.poll().operationState
                    print(self.cursor.fetchall())
                    yield TriggerEvent({"status": "error", "message": self.cursor.fetchall()})
                    return
                else:
                    yield TriggerEvent(
                        {"status": "error", "message": "An error has occurred while executing the query"}
                    )
                    return
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})
            return
