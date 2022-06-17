"""DAG definition for synaptor workflows."""
from datetime import datetime

from airflow import DAG
from airflow.models import Variable

from helper_ops import scale_up_cluster_op, scale_down_cluster_op, collect_metrics_op
from param_default import synaptor_param_default, default_synaptor_image
from synaptor_ops import manager_op, drain_op
from synaptor_ops import synaptor_op, wait_op, generate_op


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
# File segmentation
# "run synaptor file segmentation"

fileseg_dag = DAG(
    "synaptor_file_seg",
    default_args=default_args,
    schedule_interval=None,
    tags=["synaptor"],
)


# OP INSTANTIATION
# Cluster management
drain = drain_op(fileseg_dag)
sanity_check = manager_op(fileseg_dag, "sanity_check", image=SYNAPTOR_IMAGE)
init_cloudvols = manager_op(fileseg_dag, "init_cloudvols", image=SYNAPTOR_IMAGE)

scale_up_cluster = scale_up_cluster_op(
    fileseg_dag, "synaptor", "synaptor-cpu", 1, MAX_CLUSTER_SIZE, "cluster"
)
scale_down_cluster = scale_down_cluster_op(
    fileseg_dag, "synaptor", "synaptor-cpu", 0, "cluster"
)

generate_self_destruct = generate_op(fileseg_dag, "self_destruct", image=SYNAPTOR_IMAGE)
wait_self_destruct = wait_op(fileseg_dag, "self_destruct")


# Ops that do actual work
workers = [
    synaptor_op(fileseg_dag, i, image=SYNAPTOR_IMAGE) for i in range(MAX_CLUSTER_SIZE)
]

generate_chunk_ccs = generate_op(fileseg_dag, "chunk_ccs", image=SYNAPTOR_IMAGE)
wait_chunk_ccs = wait_op(fileseg_dag, "chunk_ccs")

generate_merge_ccs = generate_op(fileseg_dag, "merge_ccs", image=SYNAPTOR_IMAGE)
wait_merge_ccs = wait_op(fileseg_dag, "merge_ccs")

generate_remap = generate_op(fileseg_dag, "remap", image=SYNAPTOR_IMAGE)
wait_remap = wait_op(fileseg_dag, "remap")


# DEPENDENCIES
# Drain old tasks before doing anything
collect_metrics_op(fileseg_dag) >> drain >> sanity_check >> init_cloudvols  # >> generate_ngl_link

# Worker dag
(init_cloudvols >> scale_up_cluster >> workers >> scale_down_cluster)

# Generator/task dag
(
    init_cloudvols
    # >> sanity_check_report
    >> generate_chunk_ccs
    >> wait_chunk_ccs
    >> generate_merge_ccs
    >> wait_merge_ccs
    >> generate_remap
    >> wait_remap
    >> generate_self_destruct
    >> wait_self_destruct
)


# =========================================
# DB segmentation
# "run synaptor db segmentation"
#
# This performs the "merge_ccs" task above in separate extra steps (in parallel)
#
# I will also soon add some database management commands (creating indexes)

dbseg_dag = DAG(
    "synaptor_db_seg",
    default_args=default_args,
    schedule_interval=None,
    tags=["synaptor"],
)


# OP INSTANTIATION
# Cluster management
drain = drain_op(dbseg_dag)
sanity_check = manager_op(dbseg_dag, "sanity_check", image=SYNAPTOR_IMAGE)
init_cloudvols = manager_op(dbseg_dag, "init_cloudvols", image=SYNAPTOR_IMAGE)
init_db = manager_op(dbseg_dag, "init_db", image=SYNAPTOR_IMAGE)

scale_up_cluster = scale_up_cluster_op(
    dbseg_dag, "synaptor", "synaptor-cpu", 1, MAX_CLUSTER_SIZE, "cluster"
)
scale_down_cluster = scale_down_cluster_op(
    dbseg_dag, "synaptor", "synaptor-cpu", 0, "cluster"
)

generate_self_destruct = generate_op(dbseg_dag, "self_destruct", image=SYNAPTOR_IMAGE)
wait_self_destruct = wait_op(dbseg_dag, "self_destruct")


# Ops that do actual work
workers = [
    synaptor_op(dbseg_dag, i, image=SYNAPTOR_IMAGE) for i in range(MAX_CLUSTER_SIZE)
]

generate_chunk_ccs = generate_op(dbseg_dag, "chunk_ccs", image=SYNAPTOR_IMAGE)
wait_chunk_ccs = wait_op(dbseg_dag, "chunk_ccs")

generate_match_contins = generate_op(dbseg_dag, "match_contins", image=SYNAPTOR_IMAGE)
wait_match_contins = wait_op(dbseg_dag, "match_contins")

generate_seg_graph_ccs = generate_op(dbseg_dag, "seg_graph_ccs", image=SYNAPTOR_IMAGE)
wait_seg_graph_ccs = wait_op(dbseg_dag, "seg_graph_ccs")

generate_chunk_seg_map = generate_op(dbseg_dag, "chunk_seg_map", image=SYNAPTOR_IMAGE)
wait_chunk_seg_map = wait_op(dbseg_dag, "chunk_seg_map")

generate_merge_seginfo = generate_op(dbseg_dag, "merge_seginfo", image=SYNAPTOR_IMAGE)
wait_merge_seginfo = wait_op(dbseg_dag, "merge_seginfo")

generate_remap = generate_op(dbseg_dag, "remap", image=SYNAPTOR_IMAGE)
wait_remap = wait_op(dbseg_dag, "remap")


# DEPENDENCIES
# Drain old tasks before doing anything
collect_metrics_op(dbseg_dag) >> drain >> sanity_check >> init_cloudvols >> init_db  # >> generate_ngl_link

# Worker dag
(init_db >> scale_up_cluster >> workers >> scale_down_cluster)

# Generator/task dag
(
    init_db
    # >> sanity_check_report
    >> generate_chunk_ccs
    >> wait_chunk_ccs
    >> generate_match_contins
    >> wait_match_contins
    >> generate_seg_graph_ccs
    >> wait_seg_graph_ccs
    >> generate_chunk_seg_map
    >> wait_chunk_seg_map
    >> generate_merge_seginfo
    >> wait_merge_seginfo
    >> generate_remap
    >> wait_remap
    >> generate_self_destruct
    >> wait_self_destruct
)


# =========================================
# DB assignment
# "run synaptor assignment"
#
# This adds three more parallel processing stages for synapse assignment
#
# The "chunk_edges" steps requires a GPU, so we need to add some extra
# operators to scale CPU/GPU nodes up and down in waves.
#
# I will also soon add some database management commands (creating indexes)

assign_dag = DAG(
    "synaptor_assignment",
    default_args=default_args,
    schedule_interval=None,
    tags=["synaptor"],
)


# OP INSTANTIATION
# Cluster management
drain = drain_op(assign_dag)
sanity_check = manager_op(assign_dag, "sanity_check", image=SYNAPTOR_IMAGE)
init_cloudvols = manager_op(assign_dag, "init_cloudvols", image=SYNAPTOR_IMAGE)
init_db = manager_op(assign_dag, "init_db", image=SYNAPTOR_IMAGE)

scale_up_cluster_cpu0 = scale_up_cluster_op(
    assign_dag, "synaptor-cpu0", "synaptor-cpu", 1, MAX_CLUSTER_SIZE, "cluster"
)
scale_down_cluster_cpu0 = scale_down_cluster_op(
    assign_dag, "synaptor-cpu0", "synaptor-cpu", 0, "cluster"
)
scale_up_cluster_gpu = scale_up_cluster_op(
    assign_dag, "synaptor-gpu", "synaptor-gpu", 1, MAX_CLUSTER_SIZE, "cluster"
)
scale_down_cluster_gpu = scale_down_cluster_op(
    assign_dag, "synaptor-gpu", "synaptor-gpu", 0, "cluster"
)
scale_up_cluster_cpu1 = scale_up_cluster_op(
    assign_dag, "synaptor-cpu1", "synaptor-cpu", 1, MAX_CLUSTER_SIZE, "cluster"
)
scale_down_cluster_cpu1 = scale_down_cluster_op(
    assign_dag, "synaptor-cpu1", "synaptor-cpu", 0, "cluster"
)

generate_self_destruct_cpu0 = generate_op(
    assign_dag, "self_destruct", tag="cpu0", image=SYNAPTOR_IMAGE
)
wait_self_destruct_cpu0 = wait_op(assign_dag, "self_destruct_cpu0")
generate_self_destruct_gpu = generate_op(
    assign_dag, "self_destruct", tag="gpu", image=SYNAPTOR_IMAGE
)
wait_self_destruct_gpu = wait_op(assign_dag, "self_destruct_gpu")
generate_self_destruct_cpu1 = generate_op(
    assign_dag, "self_destruct", tag="cpu1", image=SYNAPTOR_IMAGE
)
wait_self_destruct_cpu1 = wait_op(assign_dag, "self_destruct_cpu1")


# Ops that do actual work
cpu0_workers = [
    synaptor_op(assign_dag, i, tag="cpu0", image=SYNAPTOR_IMAGE)
    for i in range(MAX_CLUSTER_SIZE)
]
gpu_workers = [
    synaptor_op(
        assign_dag, i, op_queue_name="synaptor-gpu", tag="gpu", image=SYNAPTOR_IMAGE
    )
    for i in range(MAX_CLUSTER_SIZE)
]
cpu1_workers = [
    synaptor_op(assign_dag, i, tag="cpu1", image=SYNAPTOR_IMAGE)
    for i in range(MAX_CLUSTER_SIZE)
]

generate_chunk_ccs = generate_op(assign_dag, "chunk_ccs", image=SYNAPTOR_IMAGE)
wait_chunk_ccs = wait_op(assign_dag, "chunk_ccs")

generate_match_contins = generate_op(assign_dag, "match_contins", image=SYNAPTOR_IMAGE)
wait_match_contins = wait_op(assign_dag, "match_contins")

generate_seg_graph_ccs = generate_op(assign_dag, "seg_graph_ccs", image=SYNAPTOR_IMAGE)
wait_seg_graph_ccs = wait_op(assign_dag, "seg_graph_ccs")

generate_chunk_seg_map = generate_op(assign_dag, "chunk_seg_map", image=SYNAPTOR_IMAGE)
wait_chunk_seg_map = wait_op(assign_dag, "chunk_seg_map")

generate_merge_seginfo = generate_op(assign_dag, "merge_seginfo", image=SYNAPTOR_IMAGE)
wait_merge_seginfo = wait_op(assign_dag, "merge_seginfo")

generate_chunk_edges = generate_op(assign_dag, "chunk_edges", image=SYNAPTOR_IMAGE)
wait_chunk_edges = wait_op(assign_dag, "chunk_edges")

generate_pick_edge = generate_op(assign_dag, "pick_edge", image=SYNAPTOR_IMAGE)
wait_pick_edge = wait_op(assign_dag, "pick_edge")

generate_merge_dups = generate_op(assign_dag, "merge_dups", image=SYNAPTOR_IMAGE)
wait_merge_dups = wait_op(assign_dag, "merge_dups")

generate_merge_dup_maps = generate_op(
    assign_dag, "merge_dup_maps", image=SYNAPTOR_IMAGE
)
wait_merge_dup_maps = wait_op(assign_dag, "merge_dup_maps")

generate_remap = generate_op(assign_dag, "remap", image=SYNAPTOR_IMAGE)
wait_remap = wait_op(assign_dag, "remap")


# DEPENDENCIES
# Drain old tasks before doing anything
collect_metrics_op(assign_dag) >> drain >> sanity_check >> init_cloudvols >> init_db  # >> generate_ngl_link

# Worker dag
(
    init_db
    # Segmentation wave (CPU)
    >> scale_up_cluster_cpu0
    >> cpu0_workers
    >> scale_down_cluster_cpu0
    # Chunk-wise synapse assignment (GPU)
    >> scale_up_cluster_gpu
    >> gpu_workers
    >> scale_down_cluster_gpu
    # Merging synapse assignment (CPU)
    >> scale_up_cluster_cpu1
    >> cpu1_workers
    >> scale_down_cluster_cpu1
)

# Generator/task dag
(
    init_db
    # Segmentation wave (CPU)
    >> generate_chunk_ccs
    >> wait_chunk_ccs
    >> generate_match_contins
    >> wait_match_contins
    >> generate_seg_graph_ccs
    >> wait_seg_graph_ccs
    >> generate_chunk_seg_map
    >> wait_chunk_seg_map
    >> generate_merge_seginfo
    >> wait_merge_seginfo
    >> generate_self_destruct_cpu0
    >> wait_self_destruct_cpu0
)

(
    wait_self_destruct_cpu0
    # Chunk-wise synapse assignment (GPU)
    >> generate_chunk_edges
    >> wait_chunk_edges
    >> generate_self_destruct_gpu
    >> wait_self_destruct_gpu
)

(
    wait_self_destruct_gpu
    # Merging synapse assignment (CPU)
    >> generate_pick_edge
    >> wait_pick_edge
    >> generate_merge_dups
    >> wait_merge_dups
    >> generate_merge_dup_maps
    >> wait_merge_dup_maps
    >> generate_remap
    >> wait_remap
    >> generate_self_destruct_cpu1
    >> wait_self_destruct_cpu1
)
