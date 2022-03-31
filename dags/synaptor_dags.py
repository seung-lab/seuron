"""DAG definition for synaptor workflows."""
from datetime import datetime
from configparser import ConfigParser

from airflow import DAG
from airflow.models import Variable

from helper_ops import scale_up_cluster_op, scale_down_cluster_op
from param_default import default_synaptor_param
from synaptor_ops import manager_op, drain_op
from synaptor_ops import synaptor_op, wait_op, generate_op


# Processing parameters
# We need to set a default so any airflow container can parse the dag before we
# pass in a configuration file

try:
    param = Variable.get("synaptor_param", default_synaptor_param)
    cp = ConfigParser()
    cp.read_string(param)
    MAX_CLUSTER_SIZE = cp.getint("Workflow", "maxclustersize")

except:  # not sure why this doesn't work sometimes
    MAX_CLUSTER_SIZE = 1


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

manager_op(dag_sanity, "sanity_check")


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
sanity_check = manager_op(fileseg_dag, "sanity_check")
init_cloudvols = manager_op(fileseg_dag, "init_cloudvols")

scale_up_cluster = scale_up_cluster_op(
    fileseg_dag, "synaptor", "synaptor-cpu", 1, MAX_CLUSTER_SIZE, "cluster"
)
scale_down_cluster = scale_down_cluster_op(
    fileseg_dag, "synaptor", "synaptor-cpu", 0, "cluster"
)

generate_self_destruct = generate_op(fileseg_dag, "self_destruct")
wait_self_destruct = wait_op(fileseg_dag, "self_destruct")


# Ops that do actual work
workers = [synaptor_op(fileseg_dag, i) for i in range(MAX_CLUSTER_SIZE)]

generate_chunk_ccs = generate_op(fileseg_dag, "chunk_ccs")
wait_chunk_ccs = wait_op(fileseg_dag, "chunk_ccs")

generate_merge_ccs = generate_op(fileseg_dag, "merge_ccs")
wait_merge_ccs = wait_op(fileseg_dag, "merge_ccs")

generate_remap = generate_op(fileseg_dag, "remap")
wait_remap = wait_op(fileseg_dag, "remap")


# DEPENDENCIES
# Drain old tasks before doing anything
drain >> sanity_check >> init_cloudvols  # >> generate_ngl_link

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
