from unittest import mock
from unittest.mock import MagicMock

import pytest

from astronomer_operators.google.hooks.bigquery_async import BigQueryHookAsync

PROJECT_ID = "bq-project"
CREDENTIALS = "bq-credentials"
DATASET_ID = "bq_dataset"
TABLE_ID = "bq_table"
PARTITION_ID = "20200101"
VIEW_ID = "bq_view"
JOB_ID = "1234"
LOCATION = "europe-north1"
TABLE_REFERENCE_REPR = {
    "tableId": TABLE_ID,
    "datasetId": DATASET_ID,
    "projectId": PROJECT_ID,
}


@pytest.mark.asyncio
@mock.patch("astronomer_operators.google.hooks.bigquery_async.BigQueryHookAsync.get_job_instance")
@mock.patch(
    "astronomer_operators.google.hooks.bigquery_async.BigQueryHookAsync.get_connection", new=MagicMock()
)
@mock.patch("astronomer_operators.google.hooks.bigquery_async.Session")
async def test_get_job_status(mock_session, mock_job_instance):
    print("Entered test_get_job_status")
    hook = BigQueryHookAsync()
    resp = await hook.get_job_status(job_id=JOB_ID, project_id=PROJECT_ID)
    print(resp)
    print(hook._super_init_has_run)
    print(hook.extras)
    mock_job_instance.assert_called_once_with(
        PROJECT_ID, JOB_ID, mock_session.return_value.__aenter__.return_value
    )
