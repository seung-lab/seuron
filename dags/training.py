"""Training DAGs."""
from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.utils.weight_rule import WeightRule
from airflow.operators.python import PythonOperator
from airflow.models import Variable, BaseOperator as Operator

from worker_op import worker_op
from helper_ops import scale_up_cluster_op, scale_down_cluster_op
from slack_message import task_failure_alert, task_done_alert
from webknossos import export_op, report_export


PARAM = Variable.get("training_param", {}, deserialize_json=True)
DEEPEM_IMAGE = PARAM.get("deepem_image", "zettaai/deepem")


default_args = dict(
    owner="seuronbot",
    depends_on_path=False,
    start_date=datetime(2022, 1, 23),
    catchup=False,
    retries=0,
)


def prep_parameters() -> dict:
    """Modify the user-supplied parameters to be used as a command for DeepEM."""
    param = PARAM.copy()

    # Removing unused args and storing some
    exp_name = param.get("exp_name", "empty")
    while exp_name.endswith("/"):
        exp_name = exp_name[:-1]
    remote_dir = param.pop("remote_dir", "")
    annotation_ids = param.pop("annotation_ids", [])
    param.pop("deepem_image", DEEPEM_IMAGE)

    # Adding experiment directory to samwise map
    param["samwise_map"] = (
        param.get('samwise_map', []) + 
        [f"{os.path.join(remote_dir, exp_name)}/::/workspace/experiments/{exp_name}/"]
    )

    param["train_ids"] = annotation_ids
    param["val_ids"] = param.get("validation_ids", annotation_ids[-1:])

    # Replace the remote pretraining chkpt location with a samwise directory
    if "pretrain" in param:
        pretrain_dir = "/workspace/pretrain"
        remote_dir = os.path.split(param["pretrain"])

        param["samwise_map"] = (
            param.get("samwise_map", []) + [f"{remote_dir}::{pretrain_dir}"]
        )

    # Default samwise period
    if "samwise_period" not in param:
        param["samwise_period"] = 1800

    return param


def make_argstr(param: dict) -> str:

    def format_arg(item) -> str:
        k, v = item
        if v is None:
            return f"--{k}"
        elif isinstance(v, list):
            return f"--{k} " + " ".join(f"{elem}" for elem in v)
        elif isinstance(v, dict):
            return (
                f"--{k}"
                + " '{"
                + ", ".join(f"\"{vk}\":\"{vv}\"" for vk, vv in v.items())
                + "}'"
            )
        else:
            return f"--{k} {v}"

    return " ".join(map(format_arg, param.items()))


def training_op(dag: DAG, queue="gpu") -> Operator:
    param = prep_parameters()
    return worker_op(
        variables={},
        task_id="training",
        command=make_argstr(param),
        force_pull=True,
        on_failure_callback=task_failure_alert,
        on_success_callback=task_done_alert,
        image=DEEPEM_IMAGE,
        priority_weight=100_000,
        weight_rule=WeightRule.ABSOLUTE,
        queue=queue,
        dag=dag,
    )


training_dag = DAG(
    "training",
    default_args=default_args,
    schedule_interval=None,
    tags=["training"],
)

#export = export_op(training_dag)
#report_export_task = PythonOperator(
#    task_id="report_export",
#    provide_context=True,
#    python_callable=report_export,
#    priority_weight=100000,
#    on_failure_callback=task_failure_alert,
#    weight_rule=WeightRule.ABSOLUTE,
#    queue="manager",
#    dag=training_dag,
#)

scale_up = scale_up_cluster_op(training_dag, "training", "gpu", 1, 1, "cluster")
scale_down = scale_down_cluster_op(
    training_dag, "training", "gpu", 0, "cluster", trigger_rule="all_done"
)
training = training_op(training_dag)

(
    #export
    #>> report_export_task
    scale_up
    >> training
    >> scale_down
    # >> report_training_task
)
