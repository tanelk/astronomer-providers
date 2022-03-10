from airflow.decorators import dag, task
from airflow.utils import timezone
from airflow.utils.session import create_session


@dag(
    schedule_interval=None,
    start_date=timezone.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
)
def master_dag():
    """Master DAG to get overall status for all the example DAGs"""

    @task
    def check_overall_status():
        """Check overall status of all DagRuns and fail if one DagRun failed"""
        from airflow.models import DagRun
        from airflow.utils.state import State

        with create_session() as session:
            dag_runs = session.query(DagRun.dag_id, DagRun.state).all()

        assert len(dag_runs) == 1
        if any(dag_run.state == State.FAILED for dag_run in dag_runs):
            raise AssertionError("at least one dag run failed")
