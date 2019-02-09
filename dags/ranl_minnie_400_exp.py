from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.docker_plugin import DockerWithVariablesOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.slack_operator import SlackAPIPostOperator
from airflow.utils.weight_rule import WeightRule
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook

from chunk_iterator import ChunkIterator
from cloudvolume import CloudVolume
from taskqueue import TaskQueue
from cloudvolume.lib import Vec
import igneous.task_creation as tc
from time import sleep
import os

SLACK_CONN_ID = 'Slack'
AWS_CONN_ID = 'AWS'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 2, 4),
    'cactchup_by_default': False,
    'retries': 100,
    'retry_delay': timedelta(seconds=10),
    'retry_exponential_backoff': True,
    }

dag = DAG(
    "ranl_minnie_seg", default_args=default_args, schedule_interval=None)

param_default = {
    "SCRATCH_PATH":"gs://ranl-scratch/minnie_367_0",

    "AFF_PATH":"gs://microns-seunglab/minnie_v0/minnie10/affinitymap/test",
    "AFF_MIP":"1",

    "WS_PATH":"gs://microns-seunglab/minnie_v0/minnie10/ws_367_0",
    "WS_MIP":"0",

    "SEG_PATH":"gs://microns-seunglab/minnie_v0/minnie10/seg_367_0",
    "SEG_MIP":"0",

    "WS_HIGH_THRESHOLD":"0.99",
    "WS_LOW_THRESHOLD":"0.01",
    "WS_SIZE_THRESHOLD":"200",

    "AGG_THRESHOLD":"0.2",
    "WS_IMAGE":"ranlu/watershed:ranl_minnie_exp",
    "AGG_IMAGE":"ranlu/agglomeration:ranl_minnie_exp",
    "BBOX": [126280+256, 64280+256, 20826-200, 148720-256, 148720-256, 20993],
    "RESOLUTION": [8,8,40]
}

Variable.setdefault("param",param_default, deserialize_json=True)
param = Variable.get("param", deserialize_json=True)
ws_image = param["WS_IMAGE"]
agg_image = param["AGG_IMAGE"]

cv_path = "/root/.cloudvolume/secrets/"
config_file = "param"
cmd_proto = '/bin/bash -c "mkdir $AIRFLOW_TMP_DIR/work && cd $AIRFLOW_TMP_DIR/work && {} && rm -rf $AIRFLOW_TMP_DIR/work || {{ rm -rf $AIRFLOW_TMP_DIR/work; exit 111; }}"'
config_mounts = ['neuroglancer-google-secret.json', 'google-secret.json', config_file]

def slack_alert(msg, context):
    text="""
        {msg}
        *Task*: {task}
        *Dag*: {dag}
        """.format(msg=msg,
        task=context.get('task_instance').task_id,
        dag=context.get('task_instance').dag_id,
        ti=context.get('task_instance'))

    slack_message = slack_message_op(dag, "slack_message", text)
    return slack_message.execute(context=context)

def task_start_alert(context):
    return slack_alert(":arrow_forward: Task Started", context)

def task_retry_alert(context):
    try_number = context.get('task_instance').try_number
    if try_number > 2:
        return slack_alert(":exclamation: Task up for retry: {}".format(try_number-1), context)

def task_done_alert(context):
    return slack_alert(":heavy_check_mark: Task Finished", context)

def composite_chunks_wrap_op(dag, queue, tag, stage, op):
    cmdlist = "export STAGE={} && /root/{}/scripts/run_wrapper.sh . composite_chunk_{} {}".format(stage, stage, op, tag)

    image = ws_image if stage == "ws" else agg_image

    return DockerWithVariablesOperator(
        config_mounts,
        mount_point=cv_path,
        task_id='composite_chunk_{}_{}'.format(stage, tag),
        command=cmd_proto.format(cmdlist),
        default_args=default_args,
        image=image,
        on_retry_callback=task_retry_alert,
        weight_rule=WeightRule.ABSOLUTE,
        execution_timeout=timedelta(minutes=2880),
        queue=queue,
        dag=dag
    )

def placeholder_op(dag, tid):
    return DummyOperator(
        task_id = "dummy_{}".format(tid),
        default_args=default_args,
        dag=dag,
        queue = "manager"
    )

def slack_message_op(dag, tid, msg):
    try:
        slack_username = BaseHook.get_connection(SLACK_CONN_ID).login
        slack_token = BaseHook.get_connection(SLACK_CONN_ID).password
        slack_channel = BaseHook.get_connection(SLACK_CONN_ID).extra
    except:
        return placeholder_op(dag, tid)

    if (not slack_username) or (not slack_token) or (not slack_channel):
        return placeholder_op(dag, tid)

    return SlackAPIPostOperator(
        task_id='slack_message_{}'.format(tid),
        username=slack_username,
        channel=slack_channel,
        token=slack_token,
        queue="manager",
        text=msg,
        dag=dag
    )


def composite_chunks_batch_op(dag, queue, mip, tag, stage, op):
    cmdlist = "export STAGE={} && /root/{}/scripts/run_batch.sh {} {} {}".format(stage, stage, op, mip, tag)

    image = ws_image if stage == "ws" else agg_image

    return DockerWithVariablesOperator(
        config_mounts,
        mount_point=cv_path,
        task_id='composite_chunk_{}_{}'.format(stage, tag),
        command=cmd_proto.format(cmdlist),
        default_args=default_args,
        image=image,
        on_retry_callback=task_retry_alert,
        weight_rule=WeightRule.ABSOLUTE,
        execution_timeout=timedelta(minutes=30),
        queue=queue,
        dag=dag
    )

def remap_chunks_batch_op(dag, queue, mip, tag, stage, op):
    cmdlist = "export STAGE={} && /root/ws/scripts/remap_batch.sh {} {} {}".format(stage, stage, mip, tag)
    return DockerWithVariablesOperator(
        config_mounts,
        mount_point=cv_path,
        task_id='remap_chunk_{}_{}'.format(stage, tag),
        command=cmd_proto.format(cmdlist),
        default_args=default_args,
        image=ws_image,
        on_retry_callback=task_retry_alert,
        weight_rule=WeightRule.ABSOLUTE,
        execution_timeout=timedelta(minutes=30),
        queue=queue,
        dag=dag
    )

def create_info(stage, param):
    cv_secrets_path = os.path.join(os.path.expanduser('~'),".cloudvolume/secrets")
    if not os.path.exists(cv_secrets_path):
        os.makedirs(cv_secrets_path)


    for k in ['neuroglancer-google-secret.json', 'google-secret.json']:
        v = Variable.get(k)
        with open(os.path.join(cv_secrets_path, k), 'w') as value_file:
            value_file.write(v)

    bbox = param["BBOX"]
    metadata_seg = CloudVolume.create_new_info(
        num_channels    = 1,
        layer_type      = 'segmentation',
        data_type       = 'uint64',
        encoding        = 'raw',
        resolution      = param["RESOLUTION"], # Pick scaling for your data!
        voxel_offset    = bbox[0:3],
        chunk_size      = [128,128,16], # This must divide evenly into image length or you won't cover the #
        volume_size     = [bbox[i+3] - bbox[i] for i in range(3)]
        )
    cv_path = param["WS_PATH"] if stage == "ws" else param["SEG_PATH"]
    vol = CloudVolume(cv_path, mip=0, info=metadata_seg, provenance=param)
    vol.commit_info()
    vol.commit_provenance()

    for k in ['neuroglancer-google-secret.json', 'google-secret.json']:
        os.remove(os.path.join(cv_secrets_path, k))

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
            generate_chunks[stage][tag]=composite_chunks_wrap_op(dag, composite_queue, tag, stage, op)
            slack_ops[stage][c.mip_level()].set_upstream(generate_chunks[stage][tag])
    elif c.mip_level() == batch_mip:
        for stage, op in [("ws", "ws"), ("agg", "me")]:
            generate_chunks[stage][tag]=composite_chunks_batch_op(dag, short_queue, batch_mip, tag, stage, op)
            slack_ops[stage][c.mip_level()].set_upstream(generate_chunks[stage][tag])
            remap_chunks[stage][tag]=remap_chunks_batch_op(dag, short_queue, batch_mip, tag, stage, op)
            slack_ops[stage]["remap"].set_upstream(remap_chunks[stage][tag])
            generate_chunks[stage][top_tag].set_downstream(remap_chunks[stage][tag])
            init[stage].set_downstream(generate_chunks[stage][tag])

        #remap_chunks_ws[tag].set_downstream(init_agg)

    if c.mip_level() < top_mip:
        parent_coord = [i//2 for i in c.coordinate()]
        parent_tag = str(c.mip_level()+1) + "_" + "_".join([str(i) for i in parent_coord])
        for stage in ["ws", "agg"]:
            generate_chunks[stage][tag].set_downstream(generate_chunks[stage][parent_tag])

def check_queue(tq):
    totalTries = 5
    nTries = totalTries
    while True:
        sleep(20)
        nTasks = tq.enqueued
        print("Tasks left: {}".format(nTasks))
        if nTasks == 0:
            nTries -= 1
        else:
            nTries = totalTries
        if nTries == 0:
            return

def downsample_and_mesh():
    url = "https://sqs.us-east-1.amazonaws.com/098703261575/ranl-iarpa-igneous"

    seg_cloudpath = param["SEG_PATH"]

    try:
        os.environ['AWS_ACCESS_KEY_ID'] = BaseHook.get_connection(AWS_CONN_ID).login
        os.environ['AWS_SECRET_ACCESS_KEY'] = BaseHook.get_connection(AWS_CONN_ID).password
    except:
        pass

    with TaskQueue(url, queue_server='sqs') as tq:
        tc.create_downsampling_tasks(tq, seg_cloudpath, mip=0, fill_missing=True, preserve_chunk_size=True)
        check_queue(tq)
        tc.create_meshing_tasks(tq, seg_cloudpath, mip=2, shape=Vec(256, 256, 256))
        check_queue(tq)
        tc.create_mesh_manifest_tasks(tq, seg_cloudpath, magnitude=4)
        check_queue(tq)
        #tc.create_downsampling_tasks(tq, seg_cloudpath, mip=5, fill_missing=True, preserve_chunk_size=True)
        #check_queue(tq)


#data_bbox = [126280+256, 64280+256, 20826-200, 148720-256, 148720-256, 20993]
init = {}

init["ws"] = PythonOperator(
    task_id = "Init_Watershed",
    python_callable=create_info,
    op_args = ["ws", param],
    default_args=default_args,
    on_success_callback=task_start_alert,
    dag=dag,
    queue = "manager"
)
init["agg"] = PythonOperator(
    task_id = "Init_Agglomeration",
    python_callable=create_info,
    op_args = ["agg", param],
    default_args=default_args,
    on_success_callback=task_start_alert,
    dag=dag,
    queue = "manager"
)
#done  = DummyOperator(
#    task_id = "Finish",
#    default_args=default_args,
#    on_success_callback=task_done_alert,
#    dag=dag,
#    queue = "manager"
#)
igneous_task = PythonOperator(
    task_id = "Downsample_and_Mesh",
    python_callable=downsample_and_mesh,
    default_args=default_args,
    on_success_callback=task_done_alert,
    dag=dag,
    queue = "manager"
)

data_bbox = param["BBOX"]

chunk_size = [512,512,128]
batch_mip = 2
high_mip = 5

v = ChunkIterator(data_bbox, chunk_size)
top_mip = v.top_mip_level()

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

for c in v:
    if c.mip_level() < batch_mip:
        break
    else:
        for k in ["ws","agg"]:
            if c.mip_level() not in slack_ops[k]:
                slack_ops[k][c.mip_level()] = slack_message_op(dag, k+str(c.mip_level()), ":heavy_check_mark: {}: MIP {} finished".format(k, c.mip_level()))
                if c.mip_level() == batch_mip:
                    slack_ops[k]["remap"] = slack_message_op(dag, "remap_{}".format(k), ":heavy_check_mark: {}: Remaping finished".format(k))
        process_composite_tasks(c, top_mip)

igneous_task.set_upstream(slack_ops["agg"]["remap"])
init["agg"].set_upstream(slack_ops["ws"]["remap"])
