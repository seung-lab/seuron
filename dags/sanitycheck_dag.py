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

def cv_check_alert(context):
    msg = "*Sanity check failed* Errors in the input cloudvolume data"
    slack_msg = slack_message_op(dag, "slack_message", msg)
    return slack_msg.execute(context=context)

def path_exist_alert(context):
    msg = "*Sanity check failed* Path already exist: {}".format(context.get('task_instance').task_id)
    slack_msg = slack_message_op(dag, "slack_message", msg)
    return slack_msg.execute(context=context)

def task_done_alert(context):
    msg = "{} passed".format(context.get('task_instance').task_id)
    slack_msg = slack_message_op(dag, "slack_message", msg)
    return slack_msg.execute(context=context)


def check_cv_data():
    param = Variable.get("param", deserialize_json=True)
    cv_secrets_path = os.path.join(os.path.expanduser('~'),".cloudvolume/secrets")
    if not os.path.exists(cv_secrets_path):
        os.makedirs(cv_secrets_path)

    mount_secrets = param.get("MOUNT_SECRETES", [])

    if "AFF_RESOLUTION" in param:
        try:
            vol = CloudVolume(param["AFF_PATH"], mip=param["AFF_RESOLUTION"])
        except:
            slack_message(":u7981:*ERROR: Cannot access the affinity map* `{}` at resolution {}".format(param["AFF_PATH"], param["AFF_RESOLUTION"]))
            raise ValueError('Resolution does not exist')
        if "AFF_MIP" in param:
            slack_message(":exclamation:*AFF_RESOLUTION and AFF_MIP are both specified, Perfer AFF_RESOLUTION*")
        param["AFF_MIP"] = vol.mip
        Variable.set("param", param, serialize_json=True)

    if "AFF_MIP" not in param:
        param["AFF_MIP"] = 0
        Variable.set("param", param, serialize_json=True)
        slack_message("*Use MIP 0 affinity map by default*")

    for k in mount_secrets:
        v = Variable.get(k)
        with open(os.path.join(cv_secrets_path, k), 'w') as value_file:
            value_file.write(v)
    try:
        vol = CloudVolume(param["AFF_PATH"],mip=param["AFF_MIP"])
    except:
        slack_message(":u7981:*ERROR: Cannot access the affinity map* `{}` at MIP {}".format(param["AFF_PATH"], param["AFF_MIP"]))
        raise

    aff_bbox = vol.bounds
    if "AFF_RESOLUTION" not in param:
        param["AFF_RESOLUTION"] = [int(x) for x in vol.resolution]

    if "BBOX" in param:
        target_bbox = Bbox(param["BBOX"][:3],param["BBOX"][3:])
        if not aff_bbox.contains_bbox(target_bbox):
            slack_message(":u7981:*ERROR: Bounding box is outside of the affinity map, affinity map: {} vs bbox: {}*".format([int(x) for x in aff_bbox.to_list()], param["BBOX"]))
            raise ValueError('Bounding box is outside of the affinity map')
    else:
        param["BBOX"] = [int(x) for x in aff_bbox.to_list()]
        Variable.set("param", param, serialize_json=True)
        slack_message("*Segment the whole affinity map by default* {}".format(param["BBOX"]))

    if param.get("SKIP_WS", False):
        if "WS_PATH" not in param:
            slack_message(":u7981:*ERROR: Must specify path for existing watershed when SKIP_WS is used*")
            raise ValueError('Must specify path for existing watershed when SKIP_WS is used')
        try:
            vol_ws = CloudVolume(param["WS_PATH"])
        except:
            slack_message(":u7981:*ERROR: Cannot access the watershed layer* `{}`".format(param["WS_PATH"]))
            raise

        provenance = vol_ws.provenance
        try:
            ws_param = provenance['processing'][0]['method']
            ws_chunk_size = ws_param["CHUNK_SIZE"]
            ws_chunkmap_path = ws_param['SCRATCH_PATH']+"/chunkmap"
        except:
            raise

        if "CHUNKMAP_PATH" not in param:
            param['CHUNKMAP_PATH'] = ws_chunkmap_path
            Variable.set("param", param, serialize_json=True)
            slack_message("*Use chunkmap path derived from the watershed layer* `{}`".format(ws_chunkmap_path))

        if "CHUNK_SIZE" not in param:
            param["CHUNK_SIZE"] = ws_chunk_size
            Variable.set("param", param, serialize_json=True)
            slack_message("*Use chunk size* `{}` *to match the watershed layer*".format(ws_chunk_size))
        else:
            if any(i != j for i, j in zip(param["CHUNK_SIZE"], ws_chunk_size)):
                slack_message(":u7981:*ERROR: CHUNK_SIZE has to match the watershed layer: {} != {} *".format(param["CHUNK_SIZE"], ws_chunk_size))
                raise ValueError('CHUNK_SIZE has to match the watershed layer')

        if "CV_CHUNK_SIZE" in ws_param and "CV_CHUNK_SIZE" not in param:
            param["CV_CHUNK_SIZE"] = ws_param["CV_CHUNK_SIZE"]
            Variable.set("param", param, serialize_json=True)
            slack_message("*Use cloudvolume chunk size `{}` to match the watershed layer*".format(ws_param["CV_CHUNK_SIZE"]))


    if "CHUNK_SIZE" not in param:
        param["CHUNK_SIZE"] = [512,512,64]
        Variable.set("param", param, serialize_json=True)
        slack_message(":exclamation:*Process dataset in 512x512x64 chunks by default*")

    for k in mount_secrets:
        os.remove(os.path.join(cv_secrets_path, k))

def check_path_exists_op(dag, tag, path):
    cmdline = '/bin/bash -c ". /root/google-cloud-sdk/path.bash.inc && (gsutil ls {} >& /dev/null && exit 1 || echo OK)"'.format(path)
    return DockerWithVariablesOperator(
        [],
        task_id='check_path_{}'.format(tag.replace("PREFIX", "PATH").lower()),
        command=cmdline,
        default_args=default_args,
        image=param["WORKER_IMAGE"],
        weight_rule=WeightRule.ABSOLUTE,
        execution_timeout=timedelta(minutes=5),
        on_failure_callback=path_exist_alert,
        queue='manager',
        dag=dag
    )


def print_summary():
    param = Variable.get("param", deserialize_json=True)
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

    paths = {}

    for p in ["SCRATCH", "WS", "SEG"]:
        path = "{}_PATH".format(p)
        if path not in param:
            paths[path] = param["{}_PREFIX".format(p)]+param["NAME"]
        else:
            paths[path] = param["{}_PATH".format(p)]

    msg = '''
:heavy_check_mark: *Sanity Check, everything looks OK*
Affinity map: `{aff}`
Affinity resolution: [{resolution}]
Bounding box: [{bbox}]
Size: [{size}]
Watershed: `{ws}`
Segmentation: `{seg}`
Region graph and friends: `{scratch}`
'''.format(
        aff = param["AFF_PATH"],
        resolution = ", ".join(str(x) for x in param["AFF_RESOLUTION"]),
        bbox = ", ".join(str(x) for x in data_bbox),
        size = ", ".join(str(data_bbox[i+3] - data_bbox[i]) for i in range(3)),
        ws = paths["WS_PATH"],
        seg = paths["SEG_PATH"],
        scratch = paths["SCRATCH_PATH"],
    )

    if not param.get("SKIP_WS", False):
        msg += '''
Watershed parameters: {ws_param}
'''.format(
            ws_param = "(high: {}, low: {}, size: {})".format(param["WS_HIGH_THRESHOLD"], param["WS_LOW_THRESHOLD"], param["WS_SIZE_THRESHOLD"]),
        )

    if not param.get("SKIP_AGG", False):
        msg += '''
Agglomeration threshold: {agg_threshold}
'''.format(
            agg_threshold = param["AGG_THRESHOLD"],
        )

    msg += '''
Worker image: {worker_image}
Fundamental chunk size: {chunk_size}

{nnodes} nodes in the octree
{bchunks} bundle chunks at mip level {local_batch_mip}
{hchunks} chunks at mip level {high_mip} and above
{ntasks} tasks in total
'''.format(
        worker_image = param["WORKER_IMAGE"],
        chunk_size = param["CHUNK_SIZE"],
        nnodes = nnodes,
        bchunks = bchunks,
        local_batch_mip = local_batch_mip,
        hchunks = hchunks,
        high_mip = param.get("HIGH_MIP", 5),
        ntasks = ntasks
    )
    if param.get("OVERLAP", False) and top_mip > local_batch_mip:
        msg += ":exclamation:Agglomeration in overlaping mode at MIP {}\n".format(param.get("BATCH_MIP", 3))

    for skip_flag, op in [("SKIP_WS", "watershed"), ("SKIP_AGG", "agglomeration"), ("SKIP_DOWNSAMPLE", "downsample"), ("SKIP_MESHING", "meshing")]:
        if param.get(skip_flag, False):
            msg += ":exclamation:Skip {op}!\n".format(op=op)

    if param.get("MESH_QUALITY", "NORMAL") == "PERFECT":
        msg += ":exclamation:Meshing without any simplification requires significantly more time and resources!\n"

    slack_message(msg)

paths = {}

for p in ["SCRATCH", "WS", "SEG"]:
    path = "{}_PATH".format(p)
    if path not in param:
        paths[path] = param["{}_PREFIX".format(p)]+param["NAME"]
    else:
        paths[path] = param["{}_PATH".format(p)]

path_checks = [check_path_exists_op(dag, "SCRATCH_PATH", paths["SCRATCH_PATH"])]

for p in [("WS","WS"), ("AGG","SEG")]:
    if param.get("SKIP_"+p[0], False):
        path_checks.append(placeholder_op(dag, p[1]+"_PATH"))
    else:
        path_checks.append(check_path_exists_op(dag, p[1]+"_PATH", paths[p[1]+"_PATH"]))


affinity_check = PythonOperator(
    task_id="check_cloudvolume_data",
    python_callable=check_cv_data,
    on_failure_callback=cv_check_alert,
    queue="manager",
    dag=dag)


summary = PythonOperator(
    task_id="summary",
    python_callable=print_summary,
    queue="manager",
    dag=dag)

path_checks >> affinity_check >> summary
