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
from airflow.hooks.base_hook import BaseHook
from airflow.utils.state import State
from airflow.models import TaskInstance

from worker_op import worker_op
from helper_ops import scale_up_cluster_op, scale_down_cluster_op, collect_metrics_op
from param_default import default_mount_path
from slack_message import slack_message, task_failure_alert, task_done_alert
from webknossos import export_op, report_export


PARAM = Variable.get("training_param", {}, deserialize_json=True)
DEEPEM_IMAGE = PARAM.get("deepem_image", "zettaai/deepem")
cluster_info = json.loads(BaseHook.get_connection("InstanceGroups").extra)
training_cluster = "deepem-gpu"

max_trainers = sum(c['max_size'] for c in cluster_info[training_cluster])

if "rdzv_id" not in PARAM:
    PARAM["rdzv_id"] = str(uuid.uuid4())
    Variable.set("training_param", PARAM, serialize_json=True)

if "gpu_ids" not in PARAM:
    num_gpus = cluster_info[training_cluster][0]['gpuWorkerAcceleratorCount']
    PARAM["gpu_ids"] = list(range(num_gpus))
    Variable.set("training_param", PARAM, serialize_json=True)


SKIP_EXPORT = PARAM.pop("skip_export", False)

default_args = dict(
    owner="seuronbot",
    depends_on_path=False,
    start_date=datetime(2022, 1, 23),
    catchup=False,
    retries=10,
)


def skip_parallel_tasks(context):
    task_failure_alert(context)

    slack_message(":exclamation: Stop the rest of training nodes...")

    task_instance = context['task_instance']
    dag_run = context['dag_run']

    # Get all tasks in the parallel_tasks group
    parallel_task_ids = [
        t.task_id for t in dag_run.dag.tasks
        if t.task_id.startswith('training_') and t.task_id != task_instance.task_id
    ]

    # Mark all other running parallel tasks as skipped
    for task_id in parallel_task_ids:
        ti = TaskInstance.get_task_instance(
            task_id=task_id,
            dag_id=dag_run.dag_id,
            run_id=dag_run.run_id,
            map_index=-1,
        )

        # Only modify tasks that aren't already in a terminal state
        if ti and ti.state not in State.finished:
            ti.set_state(State.SKIPPED)

    slack_message(":exclamation: Training cluster stopped")


def reset_rdzv_id(context):
    from airflow.models import Variable
    param = Variable.get("training_param", {}, deserialize_json=True)
    param["rdzv_id"] = str(uuid.uuid4())
    Variable.set("training_param", param, serialize_json=True)


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

    torchrun_launcher =  param.pop("TORCHRUN_LAUNCHER", None)
    if torchrun_launcher:
        launch_command = ["torchrun", f"--nproc_per_node={len(param['gpu_ids'])}",
                          f"--nnodes={num_trainers}", f"--node_rank={rank}", f"--rdzv_id={rdzv_id}",
                          "--rdzv_backend=etcd-v2", f"--rdzv_endpoint={os.environ['REDIS_SERVER']}:2379",
                          "/DeepEM/deepem/train/run.py"]
    else:
        launch_command = []

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


def training_op(dag: DAG, rank=0, queue=training_cluster) -> Operator:
    param = prep_parameters()

    wandb_api_key = param.pop("WANDB_API_KEY", None)
    environment = {"WANDB_API_KEY": wandb_api_key} if wandb_api_key else None

    num_trainers = min(param.pop("NUM_TRAINERS", 1), max_trainers)
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
        on_retry_callback=reset_rdzv_id if rank == 0 else None,
        on_failure_callback=skip_parallel_tasks,
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
num_trainers = min(PARAM.get("NUM_TRAINERS", 1), max_trainers)

scale_up = scale_up_cluster_op(training_dag, "training", training_cluster, num_trainers, num_trainers, "cluster")
scale_down = scale_down_cluster_op(
    training_dag, "training", training_cluster, 0, "cluster", trigger_rule="all_done"
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
