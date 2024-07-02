"""DAG definition for synaptor workflows."""
from typing import Optional
from datetime import datetime, timedelta
from dataclasses import dataclass

from airflow import DAG
from airflow.models import Variable, BaseOperator

from helper_ops import placeholder_op, scale_up_cluster_op, scale_down_cluster_op, collect_metrics_op, toggle_nfs_server_op
from param_default import synaptor_param_default, default_synaptor_image
from synaptor_ops import manager_op, drain_op, self_destruct_op
from synaptor_ops import synaptor_op, wait_op, generate_op, nglink_op


# Processing parameters
# We need to set a default so any airflow container can parse the dag before we
# pass in a configuration file
param = Variable.get(
    "synaptor_param.json", synaptor_param_default, deserialize_json=True
)
WORKFLOW_PARAMS = param.get("Workflow", {})
MAX_CLUSTER_SIZE = int(WORKFLOW_PARAMS.get("maxclustersize", 1))
SYNAPTOR_IMAGE = WORKFLOW_PARAMS.get("synaptor_image", default_synaptor_image)

default_args = {
    "owner": "seuronbot",
    "depends_on_past": False,
    "start_date": datetime(2022, 2, 22),
    "catchup": False,
    "retries": 0,
    'retry_delay': timedelta(seconds=10),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(seconds=600),
}


# =========================================
# Sanity check DAG
# "update synaptor params"

dag_sanity = DAG(
    "synaptor_sanity_check",
    default_args=default_args,
    schedule_interval=None,
    tags=["synaptor"],
)

manager_op(dag_sanity, "sanity_check", image=SYNAPTOR_IMAGE)


# =========================================
# Workflow DAGs

@dataclass
class Task:
    name: str
    cluster_key: str
    use_gpus: bool


class CPUTask(Task):

    def __init__(self, name):
        self.name = name
        self.cluster_key = "synaptor-cpu"
        self.use_gpus = False


class GPUTask(Task):

    def __init__(self, name):
        self.name = name
        self.cluster_key = "synaptor-gpu"
        self.use_gpus = True


class GraphTask(Task):

    def __init__(self, name):
        self.name = name
        self.cluster_key = "synaptor-seggraph"
        self.use_gpus = False


class ManagerTask(Task):

    def __init__(self, name):
        self.name = name
        self.cluster_key = "manager"
        self.use_gpus = False


def fill_dag(dag: DAG, tasklist: list[Task], collect_metrics: bool = True) -> DAG:
    """Fills a synaptor DAG from a list of Tasks."""
    start_nfs_server = toggle_nfs_server_op(dag, on=True)
    stop_nfs_server = toggle_nfs_server_op(dag, on=False)
    drain_tasks = [drain_op(dag, task_queue_name=f"synaptor-{t}-tasks") for t in ["cpu", "gpu", "seggraph"]]
    init_cloudvols = manager_op(dag, "init_cloudvols", image=SYNAPTOR_IMAGE)

    start_nfs_server >> drain_tasks >> init_cloudvols

    curr_operator = init_cloudvols
    if WORKFLOW_PARAMS.get("workspacetype", "File") == "Database":
        init_db = manager_op(dag, "init_db", queue="nfs", image=SYNAPTOR_IMAGE)
        init_cloudvols >> init_db
        curr_operator = init_db

    if collect_metrics:
        metrics = collect_metrics_op(dag)
        metrics >> drain_tasks

    curr_cluster = ""
    for task in tasklist:
        curr_cluster, curr_operator = change_cluster_if_required(
            dag, curr_cluster, curr_operator, task
        )
        curr_operator = add_task(dag, task, curr_operator)

    assert curr_cluster != "", "no tasks?"
    nglink = nglink_op(
        dag,
        param["Volumes"]["descriptor"],
        param["Volumes"]["output"],
        param["Workflow"]["workflowtype"],
        param["Workflow"]["storagedir"],
        param["Workflow"].get("add_synapse_points", True),
        param["Volumes"]["image"],
        tuple(map(int, param["Dimensions"]["voxelres"].split(","))),
    )
    curr_operator >> nglink >> stop_nfs_server
    scale_down_cluster(dag, curr_cluster, stop_nfs_server)

    return dag


def change_cluster_if_required(
    dag: DAG, prev_cluster_tag: str, curr_operator: BaseOperator, next_task: Task,
) -> tuple[str, BaseOperator]:
    if next_task.cluster_key == "manager" or next_task.cluster_key in prev_cluster_tag:
        # don't need to change the cluster
        return prev_cluster_tag, curr_operator

    # scaling down previous cluster
    scale_down, curr_operator = scale_down_cluster(dag, prev_cluster_tag, curr_operator)

    # generating a new cluster tag
    guess_i = 0
    while True:
        guess_tag = f"{next_task.cluster_key}{guess_i}"
        if dag.has_task(f"resize_{guess_tag}_0"):
            guess_i += 1
        elif dag.has_task(f"dummy_resize_{guess_tag}_0"):
            guess_i += 1
        else:
            break
    new_tag = guess_tag

    # scale up and worker ops
    cluster_size = (
        MAX_CLUSTER_SIZE if next_task.cluster_key != "synaptor-seggraph" else 1
    )
    scale_up = scale_up_cluster_op(
        dag, new_tag, next_task.cluster_key, min(10, cluster_size), cluster_size, "cluster"
    )

    workers = [
        synaptor_op(
            dag,
            i,
            image=SYNAPTOR_IMAGE,
            op_queue_name=next_task.cluster_key,
            task_queue_name=f"{next_task.cluster_key}-tasks",
            tag=new_tag,
            use_gpus=next_task.use_gpus
        )
        for i in range(cluster_size)
    ]

    # dependencies
    if scale_down is not None:
        scale_down >> scale_up
    else:
        curr_operator >> scale_up

    scale_up >> workers

    return new_tag, curr_operator


def scale_down_cluster(
    dag: DAG, prev_cluster_tag: str, prev_operator: BaseOperator
) -> None:
    """Adds a self-destruct op, finds worker nodes, and chains these to a downscale."""
    if prev_cluster_tag == "":
        return None, prev_operator

    # cluster sub-dag
    cluster_key = cluster_key_from_tag(prev_cluster_tag)
    scale_down = scale_down_cluster_op(dag, prev_cluster_tag, cluster_key, 0, "cluster")
    cluster_size = 1 if prev_cluster_tag.startswith("synaptor-seggraph") else MAX_CLUSTER_SIZE
    prev_workers = [
        dag.get_task(f"worker_{prev_cluster_tag}_{i}") for i in range(cluster_size)
    ]

    prev_workers >> scale_down

    # management sub-dag
    wait_op = add_task(
        dag, CPUTask("self_destruct"), prev_operator, tag=prev_cluster_tag
    )

    wait_op >> scale_down

    return scale_down, wait_op


def cluster_key_from_tag(cluster_tag: str) -> str:
    if "graph" in cluster_tag:
        return "synaptor-seggraph"
    elif "cpu" in cluster_tag:
        return "synaptor-cpu"
    elif "gpu" in cluster_tag:
        return "synaptor-gpu"
    else:
        raise ValueError(f"unrecognized cluster tag: {cluster_tag}")


def add_task(
    dag: DAG, task: Task, prev_operator: BaseOperator, tag: Optional[str] = None
) -> BaseOperator:
    """Adds a processing step to a DAG."""

    if task.name == "self_destruct":
        cluster_key = cluster_key_from_tag(tag)
        generate = self_destruct_op(dag, queue=cluster_key, tag=tag)
    elif task.cluster_key == "manager":
        generate = manager_op(dag, task.name, image=SYNAPTOR_IMAGE)
    else:
        generate = generate_op(dag, task.name, image=SYNAPTOR_IMAGE, tag=tag, task_queue_name=f"{task.cluster_key}-tasks")

    if tag:
        wait_task_name = f"{task.name}_{tag}"
    else:
        wait_task_name = task.name

    if task.cluster_key == "manager":
        wait = placeholder_op(dag, wait_task_name)
    else:
        wait = wait_op(dag, wait_task_name, task_queue_name=f"{task.cluster_key}-tasks")

    prev_operator >> generate >> wait

    return wait


# =========================================
# Workflow definitions - which tasks are run for each workflow
file_segmentation = [
    CPUTask("chunk_ccs"),
    CPUTask("merge_ccs"),
    CPUTask("remap"),
]

db_segmentation = [
    CPUTask("chunk_ccs"),
    CPUTask("match_contins"),
    CPUTask("seg_graph_ccs"),
    CPUTask("chunk_seg_map"),
    CPUTask("merge_seginfo"),
    CPUTask("remap"),
]

file_assignment = [
    CPUTask("chunk_ccs"),
    CPUTask("merge_ccs"),
    GPUTask("chunk_edges"),
    CPUTask("merge_edges"),
    CPUTask("remap"),
]

db_assignment = [
    CPUTask("chunk_ccs"),
    CPUTask("match_contins"),
    GraphTask("seg_graph_ccs"),
    ManagerTask("index_seg_map"),
    ManagerTask("chunk_seg_map"),
    CPUTask("merge_seginfo"),
    ManagerTask("index_chunked_seg_map"),
    GPUTask("chunk_edges"),
    CPUTask("pick_edge"),
    CPUTask("merge_dups"),
    CPUTask("merge_dup_maps"),
    CPUTask("remap"),
]

# =========================================
# DAG definition
dag = DAG(
    "synaptor",
    default_args=default_args,
    schedule_interval=None,
    tags=["synaptor"],
)

workflows = {
    "Segmentation": {"File": file_segmentation, "Database": db_segmentation},
    "Segmentation+Assignment": {"File": file_assignment, "Database": db_assignment},
}

fill_dag(
    dag,
    workflows[
        WORKFLOW_PARAMS.get("workflowtype", "Segmentation")
    ][
        WORKFLOW_PARAMS.get("workspacetype", "File")
    ]
)
