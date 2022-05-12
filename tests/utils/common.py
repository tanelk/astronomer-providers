import pytest
from airflow.utils.timezone import datetime

DEFAULT_DATE = datetime(2015, 1, 1)


@pytest.fixture
def context():
    """
    Creates a context with default execution date.
    """
    context = {"execution_date": DEFAULT_DATE, "logical_date": DEFAULT_DATE}
    yield context
