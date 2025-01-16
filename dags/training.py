"""Training DAGs."""
from __future__ import annotations

import json
import os
import uuid
from datetime import datetime

from airflow import DAG
from airflow.utils.weight_rule import WeightRule
from airflow.operators.python import PythonOperator
from airflow.models import Variable, BaseOperator as Operator

from worker_op import worker_op
from helper_ops import scale_up_cluster_op, scale_down_cluster_op, collect_metrics_op
from param_default import default_mount_path
from slack_message import slack_message, task_failure_alert, task_done_alert
from webknossos import export_op, report_export


PARAM = Variable.get("training_param", {}, deserialize_json=True)
DEEPEM_IMAGE = PARAM.get("deepem_image", "zettaai/deepem")

if "rdzv_id" not in PARAM:
    PARAM["rdzv_id"] = str(uuid.uuid4())
    Variable.set("training_param", PARAM, serialize_json=True)

SKIP_EXPORT = PARAM.pop("skip_export", False)

default_args = dict(
    owner="seuronbot",
    depends_on_path=False,
    start_date=datetime(2022, 1, 23),
    catchup=False,
    retries=10,
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
    param["val_ids"] = param.pop("validation_ids", annotation_ids[-1:])

    # Default samwise period
    if "samwise_period" not in param:
        param["samwise_period"] = 1800

    return param


def make_argstr(param: dict, num_trainers: int, rank: int, rdzv_id: str) -> str:

    launch_command = ["torchrun", f"--nproc_per_node={len(param['gpu_ids'])}",
                      f"--nnodes={num_trainers}", f"--node_rank={rank}", f"--rdzv_id={rdzv_id}",
                      "--rdzv_backend=etcd-v2", f"--rdzv_endpoint={os.environ['REDIS_SERVER']}:2379",
                      "/DeepEM/deepem/train/run.py"]

    def format_arg(item) -> str:
        k, v = item
        if v is None:
            return f"--{k}"
        elif isinstance(v, list):
            return f"--{k} " + " ".join(f"{elem}" for elem in v)
        elif isinstance(v, dict):
            return f"--{k} '{json.dumps(v)}'"
        else:
            return f"--{k} {v}"

    return " ".join(launch_command + list(map(format_arg, param.items())))


def training_op(dag: DAG, rank=0, queue="deepem-gpu") -> Operator:
    param = prep_parameters()

    wandb_api_key = param.pop("WANDB_API_KEY", None)
    environment = {"WANDB_API_KEY": wandb_api_key} if wandb_api_key else None

    num_trainers = param.pop("NUM_TRAINERS", 1)
    rdzv_id = param.pop("rdzv_id", None)
    # these variables will be mounted in the containers
    mount_secrets = param.pop("MOUNT_SECRETS", [])
    variables = []
    for key in mount_secrets:
        variables.append(Variable.get(key))

    return worker_op(
        variables=variables,
        mount_point=param.pop("MOUNT_PATH", default_mount_path),
        task_id=f"training_{rank}",
        command=make_argstr(param, num_trainers, rank, rdzv_id),
        use_gpus=True,
        environment=environment,
        force_pull=True,
        on_failure_callback=task_failure_alert,
        on_success_callback=task_done_alert,
        image=DEEPEM_IMAGE,
        priority_weight=100_000,
        weight_rule=WeightRule.ABSOLUTE,
        queue=queue,
        dag=dag,
        qos=False,
        shm_size=4 * (2 ** 30),  # 4 GB
        network_mode="host",
    )


def report_model() -> None:
    """Tells the user where the last model checkpoint lives."""
    import re
    import operator
    from cloudfiles import CloudFiles

    model_dir = os.path.join(PARAM["remote_dir"], PARAM["exp_name"], "models")
    cf = CloudFiles(model_dir)
    number_regexp = re.compile("model([0-9]+).onnx")

    try:
        onnx_files = [f for f in cf.list() if number_regexp.match(f)]
        assert len(onnx_files) > 0, "No onnx checkpoints found"
        numbers = [int(number_regexp.match(f).groups()[0]) for f in onnx_files]

        latest = sorted(zip(onnx_files, numbers), key=operator.itemgetter(1))[-1][0]

    except Exception as e:
        slack_message(
            f"Error finding onnx checkpoints for experiment directory: `{model_dir}`"
            f" - {e}"
        )
        return

    slack_message(f"Latest export: `{os.path.join(model_dir, latest)}`")


training_dag = DAG(
    "training",
    default_args=default_args,
    schedule_interval=None,
    tags=["training"],
)

if not SKIP_EXPORT:
    export = export_op(training_dag)
    report_export_task = PythonOperator(
        task_id="report_export",
        provide_context=True,
        python_callable=report_export,
        priority_weight=100000,
        on_failure_callback=task_failure_alert,
        weight_rule=WeightRule.ABSOLUTE,
        queue="manager",
        dag=training_dag,
    )

collect_metrics = collect_metrics_op(training_dag)
num_trainers = PARAM["NUM_TRAINERS"]
scale_up = scale_up_cluster_op(training_dag, "training", "deepem-gpu", num_trainers, num_trainers, "cluster")
scale_down = scale_down_cluster_op(
    training_dag, "training", "deepem-gpu", 0, "cluster", trigger_rule="all_done"
)
training = [training_op(training_dag, i) for i in range(num_trainers)]
report_training = PythonOperator(
    task_id="report_model",
    python_callable=report_model,
    priority_weight=100000,
    on_failure_callback=task_failure_alert,
    weight_rule=WeightRule.ABSOLUTE,
    queue="manager",
    dag=training_dag,
)

(
    collect_metrics
    >> scale_up
    >> training
    >> report_training
    >> scale_down
)
if not SKIP_EXPORT:
    (
        export
        >> report_export_task
        >> collect_metrics
    )
