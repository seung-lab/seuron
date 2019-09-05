from airflow import DAG
from airflow.operators.docker_plugin import DockerWithVariablesOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.weight_rule import WeightRule
from datetime import datetime, timedelta
from cloudvolume import CloudVolume
from cloudvolume.lib import Bbox
from airflow.models import Variable
from param_default import param_default
import os

from chunk_iterator import ChunkIterator
from helper_ops import slack_message_op, placeholder_op
from slack_message import slack_message

Variable.setdefault("param", param_default, deserialize_json=True)
param = Variable.get("param", deserialize_json=True)

DAG_ID = 'sanity_check'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 2, 8),
    'catchup': False,
    'retries': 0,
}

dag = DAG(
    dag_id=DAG_ID,
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
)

def path_exist_alert(context):
    msg = "Path already exist: {}".format(context.get('task_instance').task_id)
    slack_msg = slack_message_op(dag, "slack_message", msg)
    return slack_msg.execute(context=context)

def task_done_alert(context):
    msg = "{} passed".format(context.get('task_instance').task_id)
    slack_msg = slack_message_op(dag, "slack_message", msg)
    return slack_msg.execute(context=context)


def affinity_check_alert(context):
    msg = "Cannot check the affinity, do you have the correct secret files?"
    slack_msg = slack_message_op(dag, "slack_message", msg)
    return slack_msg.execute(context=context)

def check_affinitymap(param):
    cv_secrets_path = os.path.join(os.path.expanduser('~'),".cloudvolume/secrets")
    if not os.path.exists(cv_secrets_path):
        os.makedirs(cv_secrets_path)

    mount_secrets = param.get("MOUNT_SECRETES", [])

    supply_default_param(param)

    for k in mount_secrets:
        v = Variable.get(k)
        with open(os.path.join(cv_secrets_path, k), 'w') as value_file:
            value_file.write(v)
    try:
        vol = CloudVolume(param["AFF_PATH"],mip=int(param["AFF_MIP"]))
    except:
        raise

    aff_bbox = vol.bounds

    if "BBOX" in param:
        target_bbox = Bbox(param["BBOX"][:3],param["BBOX"][3:])
        if not aff_bbox.contains_bbox(target_bbox):
            slack_message("ERROR: Bounding box is outside of the affinity map")
            raise ValueError('Bounding box is outside of the affinity map')
    else:
        param["BBOX"] = [int(x) for x in aff_bbox.to_list()]
        Variable.set("param", param, serialize_json=True)
        slack_message(":exclamation:*Segment the whole affinity map by default*")


    for k in mount_secrets:
        os.remove(os.path.join(cv_secrets_path, k))

def check_path_exists_op(dag, tag, path):
    cmdline = '/bin/bash -c ". /root/google-cloud-sdk/path.bash.inc && (gsutil ls {} >& /dev/null && exit 1 || echo OK)"'.format(path)
    return DockerWithVariablesOperator(
        [],
        task_id='check_path_{}'.format(tag.replace("PREFIX", "PATH")),
        command=cmdline,
        default_args=default_args,
        image=param["WORKER_IMAGE"],
        weight_rule=WeightRule.ABSOLUTE,
        execution_timeout=timedelta(minutes=5),
        on_failure_callback=path_exist_alert,
        on_success_callback=task_done_alert,
        queue='manager',
        dag=dag
    )

def supply_default_param(param):
    if "CHUNK_SIZE" not in param:
        param["CHUNK_SIZE"] = [512,512,64]
        Variable.set("param", param, serialize_json=True)
        slack_message(":exclamation:*Process dataset in 512x512x64 chunks by default*")

    if "AFF_MIP" not in param:
        param["AFF_MIP"] = 0
        Variable.set("param", param, serialize_json=True)
        slack_message(":exclamation:*Use MIP 0 affinity map by default*")


def print_summary(param):
    data_bbox = param["BBOX"]

    chunk_size = param["CHUNK_SIZE"]

    v = ChunkIterator(data_bbox, chunk_size)

    bchunks = 0
    hchunks = 0
    ntasks = 0
    nnodes = 0
    top_mip = v.top_mip_level()
    local_batch_mip = param.get("BATCH_MIP", 3)
    if top_mip < local_batch_mip:
        local_batch_mip = top_mip

    for c in v:
        mip = c.mip_level()
        if nnodes % 1000 == 0:
            print("{} nodes processed".format(nnodes))
        if mip < local_batch_mip:
            break
        else:
            ntasks+=1
            nnodes += 1
            if mip == local_batch_mip:
                bchunks+=1
            elif mip >= param.get("HIGH_MIP", 5):
                hchunks+=1
    ntasks += bchunks
    ntasks *= 2


    msg = '''
:heavy_check_mark: *Sanity Check, everything looks OK*
Affinity map: `{aff}`
Affinity mip level: {mip}
Bounding box: [{bbox}]
Size: [{size}]
Watershed: `{ws}`
Segmentation: `{seg}`
Region graph and friends: `{scratch}`
Watershed parameters: {ws_param}
Agglomeration threshold: {agg_threshold}
Worker image: {worker_image}
Fundamental chunk size: {chunk_size}

{nnodes} nodes in the octree
{bchunks} bundle chunks at mip level {local_batch_mip}
{hchunks} chunks at mip level {high_mip} and above
{ntasks} tasks in total
'''.format(
        aff = param["AFF_PATH"],
        mip = param["AFF_MIP"],
        bbox = ", ".join(str(x) for x in data_bbox),
        size = ", ".join(str(data_bbox[i+3] - data_bbox[i]) for i in range(3)),
        ws = param["WS_PATH"],
        seg = param["SEG_PATH"],
        scratch = param["SCRATCH_PATH"],
        ws_param = "(high: {}, low: {}, size: {})".format(param["WS_HIGH_THRESHOLD"], param["WS_LOW_THRESHOLD"], param["WS_SIZE_THRESHOLD"]),
        agg_threshold = param["AGG_THRESHOLD"],
        worker_image = param["WORKER_IMAGE"],
        chunk_size = param["CHUNK_SIZE"],
        nnodes = nnodes,
        bchunks = bchunks,
        local_batch_mip = local_batch_mip,
        hchunks = hchunks,
        high_mip = param.get("HIGH_MIP", 5),
        ntasks = ntasks
    )

    for skip_flag, op in [("SKIP_WS", "watershed"), ("SKIP_AGG", "agglomeration"), ("SKIP_DOWNSAMPLE", "downsample"), ("SKIP_MESHING", "meshing")]:
        if param.get(skip_flag, False):
            msg += ":exclamation:Skip {op}!\n".format(op=op)

    slack_message(msg)


for p in ["SCRATCH", "WS", "SEG"]:
    path = "{}_PATH".format(p)
    if path not in param:
        param[path] = param["{}_PREFIX".format(p)]+param["NAME"]

path_checks = [check_path_exists_op(dag, "SCRATCH_PATH", param["SCRATCH_PATH"])]

for p in [("WS","WS"), ("AGG","SEG")]:
    if param.get("SKIP_"+p[0], False):
        path_checks.append(placeholder_op(dag, p[1]+"_PATH"))
    else:
        path_checks.append(check_path_exists_op(dag, p[1]+"_PATH", param[p[1]+"_PATH"]))


affinity_check = PythonOperator(
    task_id="check_affinity_layer",
    python_callable=check_affinitymap,
    op_args = (param,),
    on_failure_callback=affinity_check_alert,
    on_success_callback=task_done_alert,
    queue="manager",
    dag=dag)

summary = PythonOperator(
    task_id="summary",
    python_callable=print_summary,
    op_args = (param,),
    queue="manager",
    dag=dag)

path_checks >> affinity_check >> summary
