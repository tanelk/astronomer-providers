import json
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,
    EmrTerminateJobFlowOperator,
)
from airflow.providers.apache.hive.operators.hive import HiveOperator
from requests import get

from astronomer.providers.apache.hive.sensors.hive_partition import (
    HivePartitionSensorAsync,
)
from astronomer.providers.apache.hive.sensors.named_hive_partition import (
    NamedHivePartitionSensorAsync,
)

EXECUTION_TIMEOUT = int(os.getenv("EXECUTION_TIMEOUT", 6))
EMR_CLUSTER_SECURITY_CONFIG = os.getenv(
    "EMR_CLUSTER_SECURITY_CONFIG", "provider-team-kerberos-security-config"
)
EMR_CLUSTER_NAME = os.getenv("EMR_CLUSTER_NAME", "example_hive_sensor_cluster")
JOB_FLOW_ROLE = os.getenv("EMR_JOB_FLOW_ROLE", "EMR_EC2_DefaultRole")
SERVICE_ROLE = os.getenv("EMR_SERVICE_ROLE", "EMR_DefaultRole")
PEM_FILENAME = os.getenv("PEM_FILENAME", "providers_team_keypair")
HIVE_OPERATOR_INGRESS_PORT = int(os.getenv("HIVE_OPERATOR_INGRESS_PORT", 10000))
HIVE_SCHEMA = os.getenv("HIVE_SCHEMA", "default")
HIVE_TABLE = os.getenv("HIVE_TABLE", "zipcode")
HIVE_PARTITION = os.getenv("HIVE_PARTITION", "state='FL'")

default_args = {
    "execution_timeout": timedelta(hours=EXECUTION_TIMEOUT),
}

AWS_CREDENTIAL = {
    "aws_access_key_id": "",
    "aws_secret_access_key": "",
    "region_name": "",
}

JOB_FLOW_OVERRIDES = {
    "Name": EMR_CLUSTER_NAME,
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
    "KerberosAttributes": {"KdcAdminPassword": "astro123", "Realm": "EC2.INTERNAL"},
}


def create_security_configuration():
    """Create security config with cluster dedicated provider having ticket lifetime 240 hrs"""
    import boto3

    client = boto3.client("emr", **AWS_CREDENTIAL)
    SecurityConfiguration = {
        "AuthenticationConfiguration": {
            "KerberosConfiguration": {
                "Provider": "ClusterDedicatedKdc",
                "ClusterDedicatedKdcConfiguration": {"TicketLifetimeInHours": 240},
            }
        }
    }

    client.create_security_configuration(
        Name=EMR_CLUSTER_SECURITY_CONFIG, SecurityConfiguration=json.dumps(SecurityConfiguration)
    )


def get_emr_cluster_statue(cluster_id) -> str:
    """Get EMR cluster status"""
    import boto3

    client = boto3.client("emr", **AWS_CREDENTIAL)
    response = client.describe_cluster(ClusterId=cluster_id)
    return response["Cluster"]["Status"]["State"]


def wait_for_cluster(ti):
    """Wait for EMR cluster to reach RUNNING or WAITING state"""
    while True:
        status: str = get_emr_cluster_statue(
            str(ti.xcom_pull(key="return_value", task_ids=["create_cluster"])[0])
        )
        if status in ["RUNNING", "WAITING"]:
            return
        else:
            logging.info("Cluster status is %s. Sleeping for 60 seconds", status)
            time.sleep(60)


def cache_cluster_details(task_instance: Any) -> None:
    """
    Fetches the cluster details and stores EmrManagedMasterSecurityGroup and
    MasterPublicDnsName in the XCOM.
    """
    import boto3

    client = boto3.client("emr", **AWS_CREDENTIAL)
    response = client.describe_cluster(
        ClusterId=str(task_instance.xcom_pull(key="return_value", task_ids=["create_cluster"])[0])
    )
    logging.info("Cluster configuration : %s", str(response))
    task_instance.xcom_push(
        key="cluster_response_master_public_dns", value=response["Cluster"]["MasterPublicDnsName"]
    )
    task_instance.xcom_push(
        key="cluster_response_master_security_group",
        value=response["Cluster"]["Ec2InstanceAttributes"]["EmrManagedMasterSecurityGroup"],
    )
    # MasterPrivateDnsName
    # PublicIpAddress


def add_inbound_rule(ti):
    """
    Add inbound rule in security group

        1. Tcp, port 22 for ssh
        2. Tcp, port 88 for Kerberos
        3. Udp, port 88 for Kerberos
        4. Tcp, port 10000 for Hive
    """
    import boto3
    from botocore.exceptions import ClientError

    container_ip = get("https://api.ipify.org").text
    logging.info("The container ip address is: %s", str(container_ip))

    inbound_rules = [
        {
            "IpProtocol": "tcp",
            "FromPort": 22,
            "ToPort": 22,
            "IpRanges": [{"CidrIp": str(container_ip) + "/32"}],
        },
        {
            "IpProtocol": "tcp",
            "FromPort": 88,
            "ToPort": 88,
            "IpRanges": [{"CidrIp": str(container_ip) + "/32"}],
        },
        {
            "IpProtocol": "udp",
            "FromPort": 88,
            "ToPort": 88,
            "IpRanges": [{"CidrIp": str(container_ip) + "/32"}],
        },
        {
            "IpProtocol": "tcp",
            "FromPort": 10000,
            "ToPort": 10000,
            "IpRanges": [{"CidrIp": str(container_ip) + "/32"}],
        },
    ]

    client = boto3.client("ec2", **AWS_CREDENTIAL)
    for _inbound_rule in inbound_rules:
        try:
            client.authorize_security_group_ingress(
                GroupId=ti.xcom_pull(
                    key="cluster_response_master_security_group", task_ids=["describe_created_cluster"]
                )[0],
                IpPermissions=[_inbound_rule],
            )
        except ClientError as error:
            if error.response.get("Error", {}).get("Code", "") == "InvalidPermission.Duplicate":
                logging.error(
                    "Ingress for port %s already authorized. Error message is: %s",
                    _inbound_rule.get("FromPort"),
                    error.response["Error"]["Message"],
                )
            else:
                raise error


def gen_kerberos_credential(ti):
    """Generate keytab on EMR master node"""
    import paramiko

    key = paramiko.RSAKey.from_private_key_file(f"/tmp/{PEM_FILENAME}.pem")
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    # Connect/ssh to an instance
    cluster_response_master_public_dns = ti.xcom_pull(
        key="cluster_response_master_public_dns", task_ids=["describe_created_cluster"]
    )[0]
    client.connect(hostname=cluster_response_master_public_dns, username="hadoop", pkey=key)

    # Execute a command(cmd) after connecting/ssh to an instance
    commands = [
        "sudo kadmin.local",
        "addprinc - randkey airflow",
        "xst -norandkey -k airflow.keytab airflow",
        "q" "chmod 777 airflow.keytab",
    ]
    for command in commands:
        stdin, stdout, stderr = client.exec_command(command)
        stdout.read()

    # close the client connection once the job is done
    client.close()


def get_kerberos_credential(ti):
    """Copy kerberos configuration and keytab from EMR master node to airflow container"""
    import subprocess

    master_public_dns = ti.xcom_pull(
        key="cluster_response_master_public_dns", task_ids=["describe_created_cluster"]
    )[0]

    copy_krb5 = f"scp - i {PEM_FILENAME} hadoop@{master_public_dns}:/etc/krb5.conf /etc/krb5.conf"
    subprocess.run(copy_krb5, shell=True, check=True)
    copy_keytab = f"scp - i {PEM_FILENAME} hadoop@{master_public_dns}:airflow.keytab airflow.keytab"
    subprocess.run(copy_keytab, shell=True, check=True)


def update_krb5_and_host():
    """
    Add a host entry for EMR cluster in Airflow host file
    Update kdc and admin_server in krb5.conf
    """
    pass


def run_kerberos_init():
    """Run airflow kerberos command in airflow container"""
    pass


def create_airflow_connection_for_hive_cli():
    """Create airflow connection"""
    pass


def delete_security_configuration():
    """Delete security configuration"""
    import boto3

    client = boto3.client("emr", **AWS_CREDENTIAL)
    client.delete_security_configuration(Name=EMR_CLUSTER_SECURITY_CONFIG)


with DAG(
    dag_id="kerberos_dag",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["hive", "kerberos"],
) as dag:
    create_security_config = PythonOperator(
        task_id="create_security_config",
        python_callable=create_security_configuration,
    )

    create_cluster = EmrCreateJobFlowOperator(
        task_id="create_cluster",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
    )

    wait = PythonOperator(task_id="wait_for_cluster", python_callable=wait_for_cluster)

    # Add Inbound rule
    inbound_rule = PythonOperator(
        task_id="add_inbound_rule",
        python_callable=add_inbound_rule,
    )

    # TODO
    cache_cluster_details = PythonOperator(
        task_id="cache_cluster_details", python_callable=cache_cluster_details
    )

    gen_kerberos_credential = PythonOperator(
        task_id="gen_kerberos_credential", python_callable=gen_kerberos_credential
    )

    get_kerberos_credential = PythonOperator(
        task_id="get_kerberos_credential", python_callable=get_kerberos_credential
    )

    # Update krb5 file & update host file
    update_krb5_and_host = PythonOperator(
        task_id="update_krb5_and_host", python_callable=update_krb5_and_host
    )

    kerberos_init = PythonOperator(task_id="run_kerberos_init", python_callable=run_kerberos_init)

    create_hive_cli_conn = PythonOperator(
        task_id="create_hive_cli_conn", python_callable=create_airflow_connection_for_hive_cli
    )

    load_to_hive = HiveOperator(
        task_id="hive_query",
        hql=(
            "CREATE TABLE zipcode(RecordNumber int,Country string,City string,Zipcode int) "
            "PARTITIONED BY(state string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';"
            "CREATE TABLE zipcodes_tmp(RecordNumber int,Country string,City string,Zipcode int,state string)"
            "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';"
            "LOAD DATA INPATH '/user/root/zipcodes.csv' INTO TABLE zipcodes_tmp;"
            "SET hive.exec.dynamic.partition.mode = nonstrict;"
            "INSERT into zipcode PARTITION(state) SELECT * from  zipcodes_tmp;"
        ),
    )

    hive_sensor = HivePartitionSensorAsync(
        task_id="hive_partition_check",
        table=HIVE_TABLE,
        partition=HIVE_PARTITION,
        poke_interval=5,
    )

    wait_for_partition = NamedHivePartitionSensorAsync(
        task_id="wait_for_partition",
        partition_names=[f"{HIVE_SCHEMA}.{HIVE_TABLE}/{HIVE_PARTITION}"],
        poke_interval=5,
    )

    remove_cluster = EmrTerminateJobFlowOperator(
        task_id="remove_cluster",
        job_flow_id=create_cluster.output,
        trigger_rule="all_done",
    )

    delete_security_config = PythonOperator(
        task_id="delete_security_config",
        python_callable=delete_security_configuration,
        trigger_rule="all_done",
    )

    (
        create_security_config
        >> create_cluster
        >> wait
        >> inbound_rule
        >> cache_cluster_details
        >> gen_kerberos_credential
        >> get_kerberos_credential
        >> update_krb5_and_host
        >> kerberos_init
        >> create_hive_cli_conn
        >> remove_cluster
        >> delete_security_config
    )
