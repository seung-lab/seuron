"""Operator functions for synaptor DAGs."""
from __future__ import annotations
import os
from typing import Optional

from airflow import DAG
from airflow.utils.weight_rule import WeightRule
from airflow.operators.python import PythonOperator
from airflow.models import Variable, BaseOperator as Operator

from worker_op import worker_op
from igneous_and_cloudvolume import check_queue
from param_default import default_synaptor_image

from slack_message import task_failure_alert, task_done_alert
from kombu_helper import drain_messages


# hard-coding these for now
MOUNT_POINT = "/root/.cloudvolume/secrets/"
TASK_QUEUE_NAME = "synaptor"


# Python callables (for PythonOperators)
def generate_ngl_link_op() -> None:
    """Generates a neuroglancer link to view the results."""
    pass


# Op functions
def drain_op(
    dag: DAG,
    task_queue_name: Optional[str] = TASK_QUEUE_NAME,
    queue: Optional[str] = "manager",
) -> Operator:
    """Drains leftover messages from the RabbitMQ."""
    from airflow import configuration as conf

    broker_url = conf.get("celery", "broker_url")

    return PythonOperator(
        task_id="drain_messages",
        python_callable=drain_messages,
        priority_weight=100_000,
        op_args=(broker_url, task_queue_name),
        weight_rule=WeightRule.ABSOLUTE,
        on_failure_callback=task_failure_alert,
        on_success_callback=task_done_alert,
        queue="manager",
        dag=dag,
    )


def manager_op(
    dag: DAG,
    synaptor_task_name: str,
    queue: str = "manager",
    image: str = default_synaptor_image,
) -> Operator:
    """An operator fn for running synaptor tasks on the airflow node."""
    config_path = os.path.join(MOUNT_POINT, "synaptor_param.json")
    command = f"{synaptor_task_name} {config_path}"

    # these variables will be mounted in the containers
    variables = ["synaptor_param.json"]

    return worker_op(
        variables=variables,
        mount_point=MOUNT_POINT,
        task_id=synaptor_task_name,
        command=command,
        force_pull=True,
        on_failure_callback=task_failure_alert,
        on_success_callback=task_done_alert,
        image=image,
        priority_weight=100_000,
        weight_rule=WeightRule.ABSOLUTE,
        queue=queue,
        dag=dag,
    )


def generate_op(
    dag: DAG,
    taskname: str,
    op_queue_name: Optional[str] = "manager",
    task_queue_name: Optional[str] = TASK_QUEUE_NAME,
    tag: Optional[str] = None,
    image: str = default_synaptor_image,
) -> Operator:
    """Generates tasks to run and adds them to the RabbitMQ."""
    from airflow import configuration as conf

    broker_url = conf.get("celery", "broker_url")
    config_path = os.path.join(MOUNT_POINT, "synaptor_param.json")

    command = (
        f"generate {taskname} {config_path}"
        f" --queueurl {broker_url}"
        f" --queuename {task_queue_name}"
    )

    # these variables will be mounted in the containers
    variables = add_secrets_if_defined(["synaptor_param.json"])

    task_id = f"generate_{taskname}" if tag is None else f"generate_{taskname}_{tag}"

    return worker_op(
        variables=variables,
        mount_point=MOUNT_POINT,
        task_id=task_id,
        command=command,
        force_pull=True,
        on_failure_callback=task_failure_alert,
        on_success_callback=task_done_alert,
        image=image,
        priority_weight=100_000,
        weight_rule=WeightRule.ABSOLUTE,
        queue=op_queue_name,
        dag=dag,
    )


def synaptor_op(
    dag: DAG,
    i: int,
    op_queue_name: Optional[str] = "synaptor-cpu",
    task_queue_name: Optional[str] = TASK_QUEUE_NAME,
    tag: Optional[str] = None,
    image: str = default_synaptor_image,
) -> Operator:
    """Runs a synaptor worker until it receives a self-destruct task."""
    from airflow import configuration as conf

    broker_url = conf.get("celery", "broker_url")
    config_path = os.path.join(MOUNT_POINT, "synaptor_param.json")

    command = (
        f"worker --configfilename {config_path}"
        f" --queueurl {broker_url} "
        f" --queuename {task_queue_name}"
        " --lease_seconds 300"
    )

    # these variables will be mounted in the containers
    variables = add_secrets_if_defined(["synaptor_param.json"])

    task_id = f"worker_{i}" if tag is None else f"worker_{tag}_{i}"

    return worker_op(
        variables=variables,
        mount_point=MOUNT_POINT,
        task_id=task_id,
        command=command,
        force_pull=True,
        image=image,
        priority_weight=100_000,
        weight_rule=WeightRule.ABSOLUTE,
        queue=op_queue_name,
        dag=dag,
        # qos='quality of service'
        # this turns of a 5-minute failure timer that can kill nodes between
        # task waves or during database tasks
        qos=False,
        retries=100,
        retry_exponential_backoff=False,
    )


def wait_op(dag: DAG, taskname: str) -> Operator:
    """Waits for a task to finish."""
    return PythonOperator(
        task_id=f"wait_for_queue_{taskname}",
        python_callable=check_queue,
        op_args=(TASK_QUEUE_NAME,),
        priority_weight=100_000,
        weight_rule=WeightRule.ABSOLUTE,
        on_success_callback=task_done_alert,
        queue="manager",
        dag=dag,
    )


# Helper functions
def add_secrets_if_defined(variables: list[str]) -> list[str]:
    """Adds CloudVolume secret files to the mounted variables if defined.

    Synaptor still needs to store the google-secret.json file sometimes
    bc it currently uses an old version of gsutil.
    """
    maybe_aws = Variable.get("aws-secret.json", None)
    maybe_gcp = Variable.get("google-secret.json", None)

    if maybe_aws is not None:
        variables.append("aws-secret.json")
    if maybe_gcp is not None:
        variables.append("google-secret.json")

    return variables
