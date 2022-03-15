import warnings
from typing import Any, Dict, Optional

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.sensors.bigquery import (
    BigQueryTablePartitionExistenceSensor,
)

from astronomer.providers.google.cloud.triggers.bigquery import (
    BigQueryTablePartitionExistenceTrigger,
)


class BigQueryTablePartitionExistenceSensorAsync(BigQueryTablePartitionExistenceSensor):
    """
    Async version to Checks for the existence of a partition within a table in Google Bigquery.

    :param project_id: The Google cloud project in which to look for the table.
        The connection supplied to the hook must provide
        access to the specified project.
    :param dataset_id: The name of the dataset in which to look for the table.
        storage bucket.
    :param table_id: The name of the table to check the existence of.
    :param partition_id: The name of the partition to check the existence of.
    :param bigquery_conn_id: (Deprecated) The connection ID used to connect to Google Cloud. This
        parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must
        have domain-wide delegation enabled.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    def __init__(
        self,
        google_cloud_conn_id: str = "google_cloud_default",
        polling_interval: float = 5.0,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.polling_interval = polling_interval
        if self.bigquery_conn_id:
            warnings.warn(
                "The bigquery_conn_id parameter has been deprecated. You should pass "
                "the gcp_conn_id parameter.",
                DeprecationWarning,
                stacklevel=3,
            )
            google_cloud_conn_id = self.bigquery_conn_id
        self.google_cloud_conn_id = google_cloud_conn_id

    def execute(self, context: Dict[str, Any]) -> None:
        """Airflow runs this method on the worker and defers using the trigger."""
        print("partition_id ", self.partition_id)
        self.defer(
            timeout=self.execution_timeout,
            trigger=BigQueryTablePartitionExistenceTrigger(
                project_id=self.project_id,
                dataset_id=self.dataset_id,
                table_id=self.table_id,
                partition_id=self.partition_id,
                google_cloud_conn_id=self.google_cloud_conn_id,
                hook_params={
                    "delegate_to": self.delegate_to,
                    "impersonation_chain": self.impersonation_chain,
                },
                poll_interval=self.polling_interval,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Dict[str, Any], event: Optional[Dict[str, str]] = None) -> str:
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event:
            if event["status"] == "success":
                table_uri = f"{self.project_id}:{self.dataset_id}.{self.table_id}"
                self.log.info(
                    "Sensor Checks for the existence of a partition id %s within a table: %s.",
                    self.partition_id,
                    table_uri,
                )
                return event["message"]
            raise AirflowException(event["message"])
        raise AirflowException("No event received in trigger callback")
