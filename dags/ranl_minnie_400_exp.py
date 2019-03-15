from airflow import DAG

from airflow.operators.python_operator import PythonOperator

from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.models import Variable

from cloudvolume import Storage

from chunk_iterator import ChunkIterator

from slack_message import slack_message, task_start_alert, task_done_alert
from segmentation_op import composite_chunks_batch_op, composite_chunks_wrap_op, remap_chunks_batch_op
from helper_ops import slack_message_op, resize_cluster_op, wait_op, mark_done_op, reset_flags_op

from param_default import param_default, batch_mip, high_mip, default_args, CLUSTER_1_CONN_ID, CLUSTER_2_CONN_ID
from igneous_and_cloudvolume import create_info, downsample_and_mesh
import numpy as np


dag_manager = DAG("segmentation", default_args=default_args, schedule_interval=None)

dag = dict()

dag["ws"] = DAG("watershed", default_args=default_args, schedule_interval=None)

dag["agg"] = DAG("agglomeration", default_args=default_args, schedule_interval=None)

dag_ws = dag["ws"]
dag_agg = dag["agg"]

Variable.setdefault("param", param_default, deserialize_json=True)
param = Variable.get("param", deserialize_json=True)
image = dict()
image["ws"] = param["WS_IMAGE"]
image["agg"] = param["AGG_IMAGE"]

for p in ["SCRATCH", "WS", "SEG"]:
    path = "{}_PATH".format(p)
    if path not in param:
        param[path] = param["{}_PREFIX".format(p)]+param["NAME"]

def confirm_dag_run(context, dag_run_obj):
    skip_flag = context['params']['skip_flag']
    op = context['params']['op']
    if param[skip_flag]:
        slack_message(":exclamation: Skip {op}".format(op=op))
    else:
        return dag_run_obj


def process_composite_tasks(c, top_mip):
    if c.mip_level() < batch_mip:
        return

    short_queue = "atomic"
    long_queue = "composite"

    composite_queue = short_queue if c.mip_level() < high_mip else long_queue

    top_tag = str(top_mip)+"_0_0_0"
    tag = str(c.mip_level()) + "_" + "_".join([str(i) for i in c.coordinate()])
    if c.mip_level() > batch_mip:
        for stage, op in [("ws", "ws"), ("agg", "me")]:
            generate_chunks[stage][c.mip_level()][tag]=composite_chunks_wrap_op(image[stage], dag[stage], composite_queue, c.mip_level(), tag, stage, op)
            slack_ops[stage][c.mip_level()].set_upstream(generate_chunks[stage][c.mip_level()][tag])
    elif c.mip_level() == batch_mip:
        for stage, op in [("ws", "ws"), ("agg", "me")]:
            generate_chunks[stage][c.mip_level()][tag]=composite_chunks_batch_op(image[stage], dag[stage], short_queue, c.mip_level(), tag, stage, op)
            slack_ops[stage][c.mip_level()].set_upstream(generate_chunks[stage][c.mip_level()][tag])
            remap_chunks[stage][tag]=remap_chunks_batch_op(image["ws"], dag[stage], short_queue, batch_mip, tag, stage, op)
            slack_ops[stage]["remap"].set_upstream(remap_chunks[stage][tag])
            generate_chunks[stage][top_mip][top_tag].set_downstream(remap_chunks[stage][tag])
            init[stage].set_downstream(generate_chunks[stage][c.mip_level()][tag])

    if c.mip_level() < top_mip:
        parent_coord = [i//2 for i in c.coordinate()]
        parent_tag = str(c.mip_level()+1) + "_" + "_".join([str(i) for i in parent_coord])
        for stage in ["ws", "agg"]:
            generate_chunks[stage][c.mip_level()][tag].set_downstream(generate_chunks[stage][c.mip_level()+1][parent_tag])

def get_infos(param):
#    try:
    content = b''
    with Storage(param["SCRATCH_PATH"]) as storage:
        v = ChunkIterator(param["BBOX"], param["CHUNK_SIZE"])
        for c in v:
            tag = str(c.mip_level()) + "_" + "_".join([str(i) for i in c.coordinate()])
            content += storage.get_file('agg/info/info_{}.data'.format(tag))

    return content

#    except:
#        print("Cannot read all the info files")
#        return None


def process_infos(param):
    dt_count = np.dtype([('segid', np.uint64), ('count', np.uint64)])
    content = get_infos(param)
    data = np.frombuffer(content, dtype=dt_count)
    order = np.argsort(data['count'])[::-1]
    data_sorted = data[order]
    msg = '''*Agglomeration Finished*
*{nseg}* segments (*{nsv}* supervoxels)

Largest segments:
{top20list}'''.format(
    nseg=len(data_sorted),
    nsv=np.sum(data_sorted['count']),
    top20list="\n".join("id: {} ({})".format(data_sorted[i][0], data_sorted[i][1]) for i in range(20))
    )
    slack_message(msg)


data_bbox = param["BBOX"]

chunk_size = param["CHUNK_SIZE"]


#data_bbox = [126280+256, 64280+256, 20826-200, 148720-256, 148720-256, 20993]
starting_msg ='''*Start Segmenting {name}*
Affinity map: `{aff}`
Affinity mip level: {mip}
Bounding box: [{bbox}]'''.format(
    name = param["NAME"],
    aff = param["AFF_PATH"],
    bbox = ", ".join(str(x) for x in param["BBOX"]),
    mip = param["AFF_MIP"]
)

ending_msg = '''*Finish Segmenting {name}*
Watershed layer: `{ws}`
Segmentation Layer: `{seg}`'''.format(
    name = param["NAME"],
    ws = param["WS_PATH"],
    seg = param["SEG_PATH"]
)

no_rescale_msg = ":exclamation: Cannot rescale cluster"
rescale_message = ":heavy_check_mark: Rescaled cluster {} to {} instances"

starting_op = slack_message_op(dag_manager, "start", starting_msg)
ending_op = slack_message_op(dag_manager, "end", ending_msg)

reset_flags = reset_flags_op(dag_manager, param)

init = dict()

init["ws"] = PythonOperator(
    task_id = "Init_Watershed",
    python_callable=create_info,
    op_args = ["ws", param],
    default_args=default_args,
    on_success_callback=task_start_alert,
    dag=dag["ws"],
    queue = "manager"
)

init["agg"] = PythonOperator(
    task_id = "Init_Agglomeration",
    python_callable=create_info,
    op_args = ["agg", param],
    default_args=default_args,
    on_success_callback=task_start_alert,
    dag=dag["agg"],
    queue = "manager"
)


generate_chunks = {
    "ws": {},
    "agg": {}
}

remap_chunks = {
    "ws": {},
    "agg": {}
}

slack_ops = {
    "ws": {},
    "agg": {}
}

scaling_ops = {
    "ws": {},
    "agg": {}
}

triggers = dict()
wait = dict()
mark_done = dict()

triggers["ws"] = TriggerDagRunOperator(
    task_id="trigger_ws",
    trigger_dag_id="watershed",
    python_callable=confirm_dag_run,
    params={'skip_flag': "SKIP_WS",
            'op': "watershed"},
    queue="manager",
    dag=dag_manager
)

wait["ws"] = wait_op(dag_manager, "ws_done")

mark_done["ws"] = mark_done_op(dag["ws"], "ws_done")

triggers["agg"] = TriggerDagRunOperator(
    task_id="trigger_agg",
    trigger_dag_id="agglomeration",
    python_callable=confirm_dag_run,
    params={'skip_flag': "SKIP_AGG",
            'op': "agglomeration"},
    queue="manager",
    dag=dag_manager
)

check_seg = PythonOperator(
    task_id = "Check_Segmentation",
    python_callable=process_infos,
    op_args = [param],
    default_args=default_args,
    dag=dag_manager,
    queue = "manager"
)

wait["agg"] = wait_op(dag_manager, "agg_done")

mark_done["agg"] = mark_done_op(dag["agg"], "agg_done")

v = ChunkIterator(data_bbox, chunk_size)
top_mip = v.top_mip_level()

for c in v:
    if c.mip_level() < batch_mip:
        break
    else:
        for k in ["ws","agg"]:
            if c.mip_level() not in generate_chunks[k]:
                generate_chunks[k][c.mip_level()] = {}

            if c.mip_level() not in slack_ops[k]:
                slack_ops[k][c.mip_level()] = slack_message_op(dag[k], k+str(c.mip_level()), ":heavy_check_mark: {}: MIP {} finished".format(k, c.mip_level()))
                if c.mip_level() == batch_mip:
                    slack_ops[k]["remap"] = slack_message_op(dag[k], "remap_{}".format(k), ":heavy_check_mark: {}: Remaping finished".format(k))
                    slack_ops[k]["remap"] >> mark_done[k]
        process_composite_tasks(c, top_mip)

cluster1_size = len(remap_chunks["ws"])

real_size, scaling_global_start = resize_cluster_op(image["ws"], dag_manager, "global_start", CLUSTER_1_CONN_ID, cluster1_size)

slack_scaling_global_start = slack_message_op(dag_manager, "scaling_up_global_start", no_rescale_msg) if real_size == -1 else slack_message_op(dag_manager, "scaling_up_global_start", rescale_message.format(1, real_size))

_, scaling_global_finish = resize_cluster_op(image["ws"], dag_manager, "global_finish", CLUSTER_1_CONN_ID, 0)
slack_scaling_global_finish = slack_message_op(dag_manager, "scaling_down_global_finish", rescale_message.format(1, 0))

igneous_task = PythonOperator(
    task_id = "Downsample_and_Mesh",
    python_callable=downsample_and_mesh,
    op_args = [param,],
    default_args=default_args,
    on_success_callback=task_done_alert,
    dag=dag_manager,
    queue = "manager"
)

starting_op >> reset_flags >> scaling_global_start >> slack_scaling_global_start >> triggers["ws"] >> wait["ws"] >> triggers["agg"] >> wait["agg"] >> scaling_global_finish >> slack_scaling_global_finish >> igneous_task >> ending_op
scaling_global_finish >> check_seg
if real_size > 0 and top_mip >= high_mip:
    for stage in ["ws", "agg"]:
        _, scaling_ops[stage]["down"] = resize_cluster_op(image["ws"], dag[stage], stage, CLUSTER_1_CONN_ID, 0)
        slack_ops[stage]["down"]= slack_message_op(dag[stage], "scaling_down_{}".format(stage), rescale_message.format(1,0))
        scaling_ops[stage]["down"].set_downstream(slack_ops[stage]["down"])

        _, scaling_ops[stage]["up"] = resize_cluster_op(image["ws"], dag[stage], stage, CLUSTER_1_CONN_ID, cluster1_size)
        slack_ops[stage]["up"]= slack_message_op(dag[stage], "scaling_up_{}".format(stage), rescale_message.format(1, real_size))
        scaling_ops[stage]["up"].set_downstream(slack_ops[stage]["up"])

        scaling_ops[stage]["down"].set_upstream(slack_ops[stage][high_mip-1])
        scaling_ops[stage]["up"].set_upstream(slack_ops[stage][top_mip])

        cluster2_size = max(1, len(generate_chunks[stage][high_mip])//8)

        real_size2, scaling_ops[stage]["up_long"] = resize_cluster_op(image["ws"], dag[stage], stage+"_long", CLUSTER_2_CONN_ID, cluster2_size)

        for k in generate_chunks[stage][high_mip-1]:
            scaling_ops[stage]["up_long"].set_upstream(generate_chunks[stage][high_mip-1][k])

        if real_size2 == -1:
            slack_ops[stage]["up_long"] = slack_message_op(dag[stage], "scaling_up_{}_long".format(stage), no_rescale_msg)
        else:
            slack_ops[stage]["up_long"] = slack_message_op(dag[stage], "scaling_up_{}_long".format(stage), rescale_message.format(2, real_size2))

        scaling_ops[stage]["up_long"].set_downstream(slack_ops[stage]["up_long"])

        if real_size2 != -1:
            real_size2, scaling_ops[stage]["down_long"] = resize_cluster_op(image["ws"], dag[stage], stage+"_long", CLUSTER_2_CONN_ID, 0)
            slack_ops[stage]["down_long"] = slack_message_op(dag[stage], "scaling_down_{}_long".format(stage), rescale_message.format(2, 0))
            scaling_ops[stage]["down_long"].set_downstream(slack_ops[stage]["down_long"])
            scaling_ops[stage]["down_long"].set_upstream(slack_ops[stage][top_mip])
