"""A simple corgie DAG for cluster management.

The overall structure is to start a cluster of failure-tolerant worker nodes
and a single manager node. The manager node will control which tasks are run
on the workers through the corgie command, and will succeed or fail depending
on how the workers process their tasks.

Regardless of whether the manager succeeds or fails, we always run a scale down
op to turn off the the worker cluster. This means that the worker nodes will always
"fail" eventually, but a following operation then marks these task instances with
the success or failure of the manager node.
"""
import json
from typing import Optional
from datetime import datetime

from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.utils.weight_rule import WeightRule
from airflow.operators.python import PythonOperator
from airflow.models import Variable, DagRun, DagBag, BaseOperator as Operator

from worker_op import worker_op
from slack_message import task_failure_alert, task_done_alert
from helper_ops import (
    scale_up_cluster_op,
    scale_down_cluster_op,
    mark_success_or_failure_op,
)


CORGIE_IMAGE = "gcr.io/zetta-lee-fly-vnc-001/test-worm-alignment-redo"
CORGIE_CLUSTERS = ["corgie-cpu", "corgie_gpu"]
CLUSTER = Variable.get("active_corgie_cluster", "corgie-gpu")
COMMAND = Variable.get("corgie_command", "")


default_args = {
    "owner": "seuronbot",
    "depends_on_past": False,
    "start_date": datetime(2022, 4, 5),
    "catchup": False,
    "retries": 0,
}


# Op functions
def sanity_check_op(
    dag: DAG, command: Optional[str] = "", queue: Optional[str] = "manager"
) -> Operator:
    """An operator fn for sanity checking a corgie command on the airflow node."""
    command = f"sanity_check {command}"
    # temporary while we use SQS
    variables = {
        "aws-secret.json": "/root/.cloudvolume/secrets",
        "credentials": "/root/.aws",
    }

    return worker_op(
        task_id="sanity_check",
        command=command,
        variables=variables,
        mount_point=None,
        force_pull=True,
        on_failure_callback=task_failure_alert,
        on_success_callback=task_done_alert,
        image=CORGIE_IMAGE,
        priority_weight=100_000,
        weight_rule=WeightRule.ABSOLUTE,
        queue=queue,
        dag=dag,
    )


def corgie_manager_op(
    dag: DAG, command: Optional[str] = "", queue: Optional[str] = "manager"
) -> Operator:
    """An operator fn for running a corgie command on the airflow node."""
    # temporary while we use SQS
    variables = {
        "aws-secret.json": "/root/.cloudvolume/secrets",
        "credentials": "/root/.aws",
    }

    return worker_op(
        task_id="corgie_manager",
        command=command,
        variables=variables,
        mount_point=None,
        force_pull=True,
        on_failure_callback=task_failure_alert,
        on_success_callback=task_done_alert,
        image=CORGIE_IMAGE,
        priority_weight=100_000,
        weight_rule=WeightRule.ABSOLUTE,
        queue=queue,
        dag=dag,
        qos=False,
    )


def corgie_worker_op(
    dag: DAG, worker_id: int, queue: Optional[str] = CLUSTER
) -> Operator:
    """An operator fn for running the corgie workers."""
    # temporary for testing
    queueurl = "seuron-corgie-testing"
    # temporary while we use SQS
    variables = {
        "aws-secret.json": "/root/.cloudvolume/secrets",
        "credentials": "/root/.aws",
    }

    command = f"corgie-worker --queue_name {queueurl} --lease_seconds 600 --verbose"
    return worker_op(
        task_id=f"corgie_worker_{worker_id}",
        command=command,
        force_pull=True,
        variables=variables,
        mount_point=None,
        image=CORGIE_IMAGE,
        priority_weight=100_000,
        weight_rule=WeightRule.ABSOLUTE,
        queue=queue,
        dag=dag,
        qos=False,
        retries=1000,
        retry_exponential_backoff=False,
    )


# Helper fn
def max_cluster_size() -> int:
    """Reads the max cluster size from the InstanceGroups connection."""
    cluster_info = json.loads(BaseHook.get_connection("InstanceGroups").extra)

    try:
        return cluster_info[CLUSTER][0]["max_size"]

    except KeyError:
        raise KeyError(f"cluster {CLUSTER} not found in InstanceGroups connection")


# DAG Definition
# DAG 1: A sanity-check dry run
dry_run = DAG(
    "corgie_dry_run",
    default_args=default_args,
    schedule_interval=None,
    tags=["corgie"],
)

sanity_check_op(dry_run, COMMAND)


# DAG 2: Actually running a corgie command with workers
MAX_CLUSTER_SIZE = max_cluster_size()

corgie_dag = DAG(
    "corgie", default_args=default_args, schedule_interval=None, tags=["corgie"],
)

# skipping sanity checks until the function is implemented
# sanity_check = sanity_check_op(corgie_dag, COMMAND)
scale_up_cluster = scale_up_cluster_op(
    corgie_dag, "corgie", CLUSTER, 1, MAX_CLUSTER_SIZE, "cluster"
)
workers = [corgie_worker_op(corgie_dag, i) for i in range(MAX_CLUSTER_SIZE)]
run_corgie = corgie_manager_op(corgie_dag, COMMAND)
scale_down_cluster = scale_down_cluster_op(
    corgie_dag, "corgie", CLUSTER, 0, "cluster", trigger_rule="all_done"
)
mark_op = mark_success_or_failure_op(corgie_dag, "corgie", "corgie_manager")

# Dependencies

# skipping sanity checks until the function is implemented
# sanity_check >> scale_up_cluster >> run_corgie >> scale_down_cluster >> mark_op
scale_up_cluster >> run_corgie >> scale_down_cluster >> mark_op
scale_up_cluster >> workers
