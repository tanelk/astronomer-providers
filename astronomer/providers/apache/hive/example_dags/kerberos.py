import os
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator

EXECUTION_TIMEOUT = int(os.getenv("EXECUTION_TIMEOUT", 6))
EMR_CLUSTER_SECURITY_CONFIG = os.getenv(
    "EMR_CLUSTER_SECURITY_CONFIG", "provider-team-kerberos-security-config"
)
JOB_FLOW_ROLE = os.getenv("EMR_JOB_FLOW_ROLE", "EMR_EC2_DefaultRole")
SERVICE_ROLE = os.getenv("EMR_SERVICE_ROLE", "EMR_DefaultRole")
PEM_FILENAME = os.getenv("PEM_FILENAME", "providers_team_keypair")


default_args = {
    "execution_timeout": timedelta(hours=EXECUTION_TIMEOUT),
}


# This example uses an emr-5.34.0 cluster, apache-hive-2.3.9 and hadoop-2.10.1.
# If you would like to use a different version of the EMR cluster, then we need to
# match the hive and hadoop versions same as specified in the integration tests `Dockefile`.
JOB_FLOW_OVERRIDES = {
    "Name": "example_hive_sensor_cluster",
    "ReleaseLabel": "emr-5.34.0",
    "Applications": [
        {"Name": "Spark"},
        {
            "Name": "Hive",
        },
        {
            "Name": "Hadoop",
        },
    ],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Primary node",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m4.large",
                "InstanceCount": 1,
            },
        ],
        "Ec2KeyName": PEM_FILENAME,
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },
    "Steps": [],
    "JobFlowRole": JOB_FLOW_ROLE,
    "ServiceRole": SERVICE_ROLE,
    "SecurityConfiguration": EMR_CLUSTER_SECURITY_CONFIG,
}


def create_security_configuration():
    """Create security config"""
    import boto3

    client = boto3.client("emr")
    SecurityConfiguration = {
        "EnableKerberosAuthentication": {
            "Provider": "Cluster dedicated KDC",
            "TicketLifetime": 240,
            "CrossRealmTrust": "Disabled",
        }
    }
    client.create_security_configuration(
        Name=EMR_CLUSTER_SECURITY_CONFIG, SecurityConfiguration=SecurityConfiguration
    )


def get_cluster_details():
    """Cluster details"""
    pass


def add_cluster_inbound_rule(task_instance: Any):
    """Rule"""
    pass


with DAG(
    dag_id="example_hive_kerberos_dag",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["example", "hive", "kerberos"],
):
    # Create security configuration
    create_security_config = PythonOperator(
        task_id="create_security_config",
        python_callable=create_security_configuration,
    )

    # Create Cluster
    cluster_creator = EmrCreateJobFlowOperator(
        task_id="cluster_creator",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
    )

    describe_created_cluster = PythonOperator(
        task_id="describe_created_cluster", python_callable=get_cluster_details
    )

    # Add Inbound rule
    add_inbound_rule = PythonOperator(
        task_id="add_inbound_rule",
        python_callable=add_cluster_inbound_rule,
    )

    # SSH to machine and run kerberos command
    # Get public ip private dns name
    # Add public ip and private dns in airflow host file
    pass
