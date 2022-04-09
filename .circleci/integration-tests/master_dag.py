import logging
import os
import time
from datetime import datetime
from typing import List

from airflow import DAG
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.models import DagBag, DagRun
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.session import create_session

SLACK_CHANNEL = os.environ.get("SLACK_CHANNEL", "#provider-alert")
SLACK_WEBHOOK_CONN = os.environ.get("SLACK_WEBHOOK_CONN", "http_slack")
SLACK_USERNAME = os.environ.get("SLACK_USERNAME", "airflow_app")


def get_report(dag_run_ids: List[str]) -> None:
    """Fetch dags run details and generate report"""
    with create_session() as session:
        last_dags_runs: List[DagRun] = session.query(DagRun).filter(DagRun.run_id.in_(dag_run_ids)).all()
        message_list: List[str] = []
        for dr in last_dags_runs:
            dr_status = f" *{dr.dag_id} : {dr.get_state()}* \n"
            message_list.append(dr_status)
            for ti in dr.get_task_instances():
                task_code = ":black_circle: "
                if not ((ti.task_id == "end") or (ti.task_id == "get_report")):
                    if ti.state == "success":
                        task_code = ":large_green_circle: "
                    elif ti.state == "failed":
                        task_code = ":red_circle: "
                    elif ti.state == "upstream_failed":
                        task_code = ":large_orange_circle: "
                    task_message_str = f"{task_code} {ti.task_id} : {ti.state} \n"
                    message_list.append(task_message_str)

        logging.info("%s", "".join(message_list))
        # Send dag run report on Slack
        try:
            SlackWebhookOperator(
                task_id="slack_alert",
                http_conn_id=SLACK_WEBHOOK_CONN,
                message="".join(message_list),
                channel=SLACK_CHANNEL,
                username=SLACK_USERNAME,
            ).execute(context=None)
        except Exception:
            logging.exception("Error occur while sending slack alert.")


def get_dag_ids() -> List[str]:
    """Return DAG id list of DagBag"""
    dag_bag = DagBag()
    return dag_bag.dag_ids


# DAG registered here will not be part of integration test
IGNORE_DAG = ["example_master_dag"]

with DAG(
    dag_id="example_master_dag",
    schedule_interval="@daily",
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=["master_dag"],
) as dag:
    # Sleep for 30 seconds so that all the example dag will be available before master dag trigger them
    start = PythonOperator(
        task_id="start",
        python_callable=lambda: time.sleep(30),
    )

    example_dag_list = []
    dag_run_ids = []
    for _dag_id in get_dag_ids():
        if _dag_id not in IGNORE_DAG:
            _run_id = f"{_dag_id}_" + "{{ ds }}"
            dag_run_ids.append(_run_id)
            example_dag_list.append(
                TriggerDagRunOperator(
                    task_id=_dag_id,
                    trigger_dag_id=_dag_id,
                    trigger_run_id=_run_id,
                    wait_for_completion=True,
                    reset_dag_run=True,
                    execution_date="{{ ds }}",
                    allowed_states=["success", "failed", "skipped"],
                )
            )

    report = PythonOperator(
        task_id="get_report",
        python_callable=get_report,
        op_kwargs={"dag_run_ids": dag_run_ids},
        trigger_rule="all_done",
    )

    end = DummyOperator(
        task_id="end",
        trigger_rule="all_success",
    )

    start >> example_dag_list >> end
    example_dag_list >> report
