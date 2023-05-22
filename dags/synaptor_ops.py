"""Operator functions for synaptor DAGs."""
from __future__ import annotations
import os
from typing import Optional

from airflow import DAG
from airflow.utils.weight_rule import WeightRule
from airflow.operators.python import PythonOperator
from airflow.models import Variable, BaseOperator

from worker_op import worker_op
from param_default import default_synaptor_image
from igneous_and_cloudvolume import check_queue, upload_json, read_single_file
from slack_message import task_failure_alert, task_done_alert, slack_message
from nglinks import ImageLayer, SegLayer, generate_ng_payload, wrap_payload
from kombu_helper import drain_messages


# hard-coding these for now
MOUNT_POINT = "/root/.cloudvolume/secrets/"
TASK_QUEUE_NAME = "synaptor"


# Op functions
def generate_nglink(
    net_output_path: str,
    seg_path: str,
    workflowtype: str,
    storagedir: str,
    add_synapse_points: str,
    img_path: Optional[str] = None,
    voxelres: Optional[tuple[int, int, int]] = None,
) -> None:
    """Generates a neuroglancer link to view the results."""
    layers = [
        ImageLayer("network output", net_output_path),
        SegLayer("synaptor segmentation", seg_path),
    ]

    if img_path:
        layers = [ImageLayer("image", img_path)] + layers

    payload = generate_ng_payload(layers)

    if "Assignment" in workflowtype and getboolean(add_synapse_points):
        presyn_pts, postsyn_pts = read_pts(storagedir)
        payload = add_annotation_layer(payload, presyn_pts, postsyn_pts, voxelres)

    upload_json(storagedir, "ng.json", payload)
    slack_message(wrap_payload(os.path.join(storagedir, "ng.json")), broadcast=True)


def getboolean(rawvalue: str) -> bool:
    """Simulating configparser.getboolean"""
    value = rawvalue.lower()
    if value in [True, 1, "yes", "y", "true", "t", "on"]:
        return True
    elif value in [False, 0, "no", "n", "false", "f", "off"]:
        return False
    else:
        raise ValueError(f"unrecognized boolean value: {rawvalue}")


def read_pts(storagedir: str) -> tuple[list, list]:
    maybe_content = read_single_file(storagedir, "final_edgelist.df")

    if maybe_content:
        content = maybe_content.decode("utf-8")
    else:
        raise ValueError("no edge list found")

    lines = content.split("\n")
    header, rows = lines[0], lines[1:]

    # indices for the columns we want
    colnames = header.split(",")
    pre_x_i = colnames.index("presyn_x")
    pre_y_i = colnames.index("presyn_y")
    pre_z_i = colnames.index("presyn_z")
    post_x_i = colnames.index("postsyn_x")
    post_y_i = colnames.index("postsyn_y")
    post_z_i = colnames.index("postsyn_z")

    # extracting points
    presyn_pts = list()
    postsyn_pts = list()
    for row in rows:
        if "," not in row:
            continue

        fields = row.split(",")
        presyn_pt = fields[pre_x_i], fields[pre_y_i], fields[pre_z_i]
        postsyn_pt = fields[post_x_i], fields[post_y_i], fields[post_z_i]

        presyn_pts.append(list(map(int, presyn_pt)))
        postsyn_pts.append(list(map(int, postsyn_pt)))

    return presyn_pts, postsyn_pts


def add_annotation_layer(
    payload: dict, presyn_pts: list, postsyn_pts: list, voxel_res: tuple
) -> dict:
    annotations = [
        {
            "pointA": list(presyn_pt),
            "pointB": list(postsyn_pt),
            "type": "line",
            "id": str(index),
        }
        for (index, (presyn_pt, postsyn_pt)) in enumerate(zip(presyn_pts, postsyn_pts))
    ]

    annotation_layer = {
        "type": "annotation",
        "tool": "annotateLine",
        "tab": "annotations",
        "source": {
            "url": "local://annotations",
            "transform" : {
                "outputDimensions": {
                    "x": [f"{voxel_res[0]}e-9", "m"],
                    "y": [f"{voxel_res[1]}e-9", "m"],
                    "z": [f"{voxel_res[2]}e-9", "m"],
                }
            }
        },
        "annotations": annotations,
    }

    payload["layers"]["synapses"] = annotation_layer

    return payload


def nglink_op(
    dag: DAG,
    net_output_path: str,
    seg_path: str,
    workflowtype: str,
    storagedir: str,
    add_synapse_points: str,
    img_path: str,
    voxelres: tuple[int, int, int],
) -> PythonOperator:
    return PythonOperator(
        task_id="nglink",
        python_callable=generate_nglink,
        op_args=(
            net_output_path, seg_path, workflowtype, storagedir, add_synapse_points
        ),
        op_kwargs=dict(img_path=img_path, voxelres=voxelres),
        priority_weight=100000,
        on_failure_callback=task_failure_alert,
        weight_rule=WeightRule.ABSOLUTE,
        queue="manager",
        dag=dag,
    )


def drain_op(
    dag: DAG,
    task_queue_name: Optional[str] = TASK_QUEUE_NAME,
    queue: Optional[str] = "manager",
) -> PythonOperator:
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
) -> BaseOperator:
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
) -> BaseOperator:
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
) -> BaseOperator:
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


def wait_op(dag: DAG, taskname: str) -> PythonOperator:
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
