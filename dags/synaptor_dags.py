"""DAG definition for synaptor workflows."""
from typing import Optional
from datetime import datetime
from dataclasses import dataclass

from airflow import DAG
from airflow.models import Variable, BaseOperator as Operator

from helper_ops import scale_up_cluster_op, scale_down_cluster_op, collect_metrics_op
from param_default import synaptor_param_default, default_synaptor_image
from synaptor_ops import manager_op, drain_op
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


class CPUTask(Task):
    pass


class GPUTask(Task):
    pass


class GraphTask(Task):
    pass


def fill_dag(dag: DAG, tasklist: list[Task], collect_metrics: bool = True) -> DAG:
    """Fills a synaptor DAG from a list of Tasks."""
    drain = drain_op(dag)
    init_cloudvols = manager_op(dag, "init_cloudvols", image=SYNAPTOR_IMAGE)

    drain >> init_cloudvols

    if collect_metrics:
        metrics = collect_metrics_op(dag)
        metrics >> drain

    curr_cluster = ""
    curr_operator = init_cloudvols
    for task in tasklist:
        if isinstance(task, CPUTask) and "cpu" not in curr_cluster:
            curr_cluster, curr_operator = change_clusters(
                dag, curr_cluster, "synaptor-cpu", curr_operator
            )
        elif isinstance(task, GPUTask) and "gpu" not in curr_cluster:
            curr_cluster, curr_operator = change_clusters(
                dag, curr_cluster, "synaptor-gpu", curr_operator
            )
        elif isinstance(task, GraphTask) and "graph" not in curr_cluster:
            curr_cluster, curr_operator = change_clusters(
                dag, curr_cluster, "synaptor-seggraph", curr_operator
            )

        curr_operator = add_task(dag, task, curr_operator)

    assert curr_cluster is not None, "no tasks?"
    nglink = nglink_op(
        dag,
        param["Volumes"]["descriptor"],
        param["Volumes"]["output"],
        param["Workflow"]["workflowtype"],
        param["Workflow"]["storagedir"],
        param["Volumes"]["image"],
        tuple(map(int, param["Dimensions"]["voxelres"].split(","))),
    )
    curr_operator >> nglink
    scale_down_cluster(dag, curr_cluster, nglink)

    return dag


def change_clusters(
    dag: DAG, prev_cluster_tag: str, next_cluster_key: str, curr_operator: Operator,
) -> tuple[str, Operator]:
    # scaling down previous cluster
    scale_down, curr_operator = scale_down_cluster(dag, prev_cluster_tag, curr_operator)

    # generating a new cluster tag
    guess_i = 0
    while True:
        guess_tag = f"{next_cluster_key.replace('synaptor-', '')}{guess_i}"
        if dag.has_task(f"resize_synaptor-{guess_tag}_{MAX_CLUSTER_SIZE}"):
            guess_i += 1
        else:
            break

        # heuristic
        if guess_i == 5:
            raise ValueError(
                "suspiciously high guess value (5), please check dag definition"
            )
    new_tag = guess_tag

    # scale up and worker ops
    scale_up = scale_up_cluster_op(
        dag, f"synaptor-{new_tag}", next_cluster_key, 1, MAX_CLUSTER_SIZE, "cluster"
    )

    queue = next_cluster_key
    workers = [
        synaptor_op(dag, i, image=SYNAPTOR_IMAGE, op_queue_name=queue, tag=new_tag)
        for i in range(MAX_CLUSTER_SIZE)
    ]

    # dependencies
    if scale_down is not None:
        scale_down >> scale_up
    else:
        curr_operator >> scale_up

    scale_up >> workers

    return new_tag, curr_operator


def scale_down_cluster(dag: DAG, prev_cluster_tag: str, prev_operator: Operator) -> None:
    """Adds a self-destruct op, finds worker nodes, and chains these to a downscale."""
    if prev_cluster_tag == "":
        return None, prev_operator

    # cluster sub-dag
    cluster_key = cluster_key_from_tag(prev_cluster_tag)
    scale_down = scale_down_cluster_op(dag, prev_cluster_tag, cluster_key, 0, "cluster")
    prev_workers = [
        dag.get_task(f"worker_{prev_cluster_tag}_{i}") for i in range(MAX_CLUSTER_SIZE)
    ]

    prev_workers >> scale_down

    # management sub-dag
    wait_op = add_task(
        dag, CPUTask("self_destruct"), prev_operator, tag=prev_cluster_tag
    )

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
    dag: DAG, task: Task, prev_operator: Operator, tag: Optional[str] = None
) -> Operator:
    """Adds a processing step to a DAG."""
    generate = generate_op(dag, task.name, image=SYNAPTOR_IMAGE, tag=tag)
    if tag:
        wait = wait_op(dag, f"{task.name}_{tag}")
    else:
        wait = wait_op(dag, task.name)

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
    CPUTask("seg_graph_ccs"),
    CPUTask("chunk_seg_map"),
    CPUTask("merge_seginfo"),
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
