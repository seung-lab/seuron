from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from custom.docker_custom import DockerWithVariablesOperator
from airflow.utils.weight_rule import WeightRule
from datetime import datetime, timedelta
from cloudvolume import CloudVolume
from cloudvolume.lib import Bbox
from airflow.models import Variable
from param_default import param_default
from igneous_and_cloudvolume import check_cloud_path_empty, cv_has_data, cv_scale_with_data
import os

from chunkiterator import ChunkIterator
from helper_ops import slack_message_op, placeholder_op
from slack_message import slack_message

Variable.setdefault("param", param_default, deserialize_json=True)
param = Variable.get("param", deserialize_json=True)

DAG_ID = 'sanity_check'

default_args = {
    'owner': 'seuronbot',
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

def worker_image_alert(context):
    msg = "*Sanity check failed* Check worker image failed: {}".format(param["WORKER_IMAGE"])
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

    for k in mount_secrets:
        try:
            v = Variable.get(k)
        except KeyError:
            webui_ip = Variable.get("webui_ip")
            slack_message(":exclamation:*{} does not exist, specify it from <https://{}/airflow/admin/variable/|the web interface>*".format(k, webui_ip))
            raise

        with open(os.path.join(cv_secrets_path, k), 'w') as value_file:
            value_file.write(v)


    # We need affinity map for watershed and agglomeration, not for meshing
    if (not param.get("SKIP_WS", False)) or (not param.get("SKIP_AGG", False)):
        if "AFF_RESOLUTION" in param:
            try:
                vol = CloudVolume(param["AFF_PATH"], mip=param["AFF_RESOLUTION"])
            except:
                slack_message(":u7981:*ERROR: Cannot access the affinity map* `{}` *at resolution {}*".format(param["AFF_PATH"], param["AFF_RESOLUTION"]))
                raise ValueError('Resolution does not exist')
            if "AFF_MIP" in param:
                slack_message(":exclamation:*AFF_RESOLUTION and AFF_MIP are both specified, Perfer AFF_RESOLUTION*")
            param["AFF_MIP"] = vol.mip
            Variable.set("param", param, serialize_json=True)

        if "AFF_MIP" not in param:
            try:
                param["AFF_MIP"], param["AFF_RESOLUTION"] = cv_scale_with_data(param["AFF_PATH"])
            except:
                slack_message(":u7981:*ERROR: Cannot access the affinity map in* `{}`".format(param["AFF_PATH"]))
                raise ValueError("No data")
            Variable.set("param", param, serialize_json=True)
            slack_message("*Use affinity map at resolution {} by default*".format(param["AFF_RESOLUTION"]))

        try:
            vol = CloudVolume(param["AFF_PATH"],mip=param["AFF_MIP"])
        except:
            slack_message(":u7981:*ERROR: Cannot access the affinity map* `{}` *at MIP {}*".format(param["AFF_PATH"], param["AFF_MIP"]))
            raise ValueError('Mip level does not exist')

        if not cv_has_data(param["AFF_PATH"], mip=param["AFF_MIP"]):
            resolution = vol.scales[param["AFF_MIP"]]['resolution']
            slack_message(":u7981:*ERROR: No data in* `{}`  *at resolution {} (mip {})*".format(param["AFF_PATH"], resolution, param["AFF_MIP"]))
            raise ValueError('No data available')

        aff_bbox = vol.bounds
        if "AFF_RESOLUTION" not in param:
            param["AFF_RESOLUTION"] = vol.resolution.tolist()
            Variable.set("param", param, serialize_json=True)

        if "BBOX" not in param:
            param["BBOX"] = [int(x) for x in aff_bbox.to_list()]
            Variable.set("param", param, serialize_json=True)
            slack_message("*Segment the whole affinity map by default* {}".format(param["BBOX"]))

        target_bbox = Bbox(param["BBOX"][:3],param["BBOX"][3:])
        if not aff_bbox.contains_bbox(target_bbox):
            slack_message(":u7981:*ERROR: Bounding box is outside of the affinity map, affinity map: {} vs bbox: {}*".format([int(x) for x in aff_bbox.to_list()], param["BBOX"]))
            raise ValueError('Bounding box is outside of the affinity map')


    if "GT_PATH" in param:
        if param.get("SKIP_AGG", False):
            slack_message(":u7981:*ERROR: Cannot compare ground truth with existing segmentation, you have to run agglomeration at least!")
        else:
            try:
                gt_vol = CloudVolume(param["GT_PATH"],mip=param["AFF_RESOLUTION"])
            except:
                slack_message(":u7981:*ERROR: Cannot access the ground truth layer* `{}` *at resolution {}*".format(param["GT_PATH"], param["AFF_RESOLUTION"]))
                raise ValueError('Ground truth layer does not exist')
            gt_bbox = gt_vol.bounds
            if not gt_bbox.contains_bbox(target_bbox):
                slack_message(":u7981:*ERROR: Bounding box is outside of the ground truth volume, gt: {} vs bbox: {}*".format([int(x) for x in gt_bbox.to_list()], param["BBOX"]))
                raise ValueError('Bounding box is outside of the ground truth volume')

    if "SEM_PATH" in param:
        if param.get("SKIP_AGG", False):
            slack_message(":u7981:*WARNING: Semantic labels will be ignored without doing agglomeration!")
        else:
            try:
                sem_vol = CloudVolume(param["SEM_PATH"], mip=param["AFF_RESOLUTION"])
            except:
                slack_message(":u7981:*ERROR: Cannot access the semantic layer* `{}` *at resolution {}*".format(param["SEM_PATH"], param["AFF_RESOLUTION"]))
                raise ValueError('Semantic layer does not exist')
            sem_bbox = sem_vol.bounds
            if not sem_bbox.contains_bbox(target_bbox):
                slack_message(":u7981:*ERROR: Bounding box is outside of the semantic label volume, sem: {} vs bbox: {}*".format([int(x) for x in sem_bbox.to_list()], param["BBOX"]))
                raise ValueError('Bounding box is outside of the semantic label volume')

            slack_message("""*Use semantic labels in* `{}`""".format(param["SEM_PATH"]))

    if param.get("SKIP_AGG", False):
        if "SEG_PATH" not in param:
            slack_message(":u7981:*ERROR: Must specify path for a existing segmentation when SKIP_AGG is used*")
            raise ValueError('Must specify path for existing watershed when SKIP_AGG is used')
        try:
            vol_seg = CloudVolume(param["SEG_PATH"])
        except:
            slack_message(":u7981:*ERROR: Cannot access the segmentation layer* `{}`".format(param["SEG_PATH"]))
            raise

        if param.get("SKIP_WS", False):
            param["AFF_PATH"] = "N/A" if "AFF_PATH" not in param else param["AFF_PATH"]
            param["WS_PATH"] = "N/A" if "WS_PATH" not in param else param["WS_PATH"]
        param["AFF_MIP"] = 0
        param["AFF_RESOLUTION"] = vol_seg.resolution.tolist()
        Variable.set("param", param, serialize_json=True)
        if "BBOX" not in param:
            seg_bbox = vol_seg.bounds
            param["BBOX"] = [int(x) for x in seg_bbox.to_list()]
            Variable.set("param", param, serialize_json=True)
            slack_message("*Process the whole segmentation by default* {}".format(param["BBOX"]))

    else:
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
                ws_chunkmap_path = os.path.join(ws_param['SCRATCH_PATH'], "chunkmap") if "CHUNKMAP_OUTPUT" not in ws_param else ws_param["CHUNKMAP_OUTPUT"]
            except:
                raise

            if "CHUNKMAP_INPUT" not in param:
                param['CHUNKMAP_INPUT'] = ws_chunkmap_path
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
                slack_message("*Use cloudvolume chunk size* `{}` *to match the watershed layer*".format(ws_param["CV_CHUNK_SIZE"]))

#        else:
    if "CHUNK_SIZE" not in param:
        param["CHUNK_SIZE"] = [512,512,64]
        Variable.set("param", param, serialize_json=True)
        slack_message(":exclamation:*Process dataset in 512x512x64 chunks by default*")

    cv_chunk_size = param.get("CV_CHUNK_SIZE", [256,256,64])
    if any( x%y != 0 for x, y in zip(param["CHUNK_SIZE"], cv_chunk_size) ):
        slack_message(":u7981:*ERROR: CHUNK_SIZE must be multiples of CV_CHUNK_SIZE in each dimension: {} vs {}*".format(param["CHUNK_SIZE"], cv_chunk_size))
        raise ValueError('CHUNK_SIZE must be multiples of CV_CHUNK_SIZE')

    for k in mount_secrets:
        os.remove(os.path.join(cv_secrets_path, k))

def check_path_exists_op(dag, tag, path):
    return PythonOperator(
        task_id='check_path_{}'.format(tag.replace("PREFIX", "PATH").lower()),
        python_callable=check_cloud_path_empty,
        op_args = (path,),
        on_failure_callback=cv_check_alert,
        weight_rule=WeightRule.ABSOLUTE,
        queue="manager",
        dag=dag
    )

def check_worker_image_op(dag):
    cmdline = '/bin/bash -c "ls /root/seg/scripts/init.sh"'
    return DockerWithVariablesOperator(
        [],
        task_id='check_worker_image',
        command=cmdline,
        default_args=default_args,
        image=param["WORKER_IMAGE"],
        weight_rule=WeightRule.ABSOLUTE,
        execution_timeout=timedelta(minutes=5),
        on_failure_callback=worker_image_alert,
        queue='manager',
        dag=dag
)

def print_summary():
    param = Variable.get("param", deserialize_json=True)
    data_bbox = param["BBOX"]

    if not (param.get("SKIP_WS", False) and param.get("SKIP_AGG", False)):
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
Resolution: [{resolution}]
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

    if not (param.get("SKIP_WS", False) and param.get("SKIP_AGG", False)):
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

    for skip_flag, op in [("SKIP_WS", "watershed"), ("SKIP_AGG", "agglomeration"), ("SKIP_DOWNSAMPLE", "downsample"), ("SKIP_MESHING", "meshing"), ("SKIP_SKELETON", "skeletonization")]:
        if param.get(skip_flag, False):
            msg += ":exclamation:Skip {op}!\n".format(op=op)

    if param.get("MESH_QUALITY", "NORMAL") == "PERFECT":
        msg += ":exclamation:Meshing without any simplification requires significantly more time and resources!\n"

    if "GT_PATH" in param:
        msg += """:vs: Evaluate the output against ground truth `{}`\n""".format(param["GT_PATH"])

    slack_message(msg, broadcast=True)

paths = {}

for p in ["SCRATCH", "WS", "SEG"]:
    path = "{}_PATH".format(p)
    if path not in param:
        paths[path] = param["{}_PREFIX".format(p)]+param["NAME"]
    else:
        paths[path] = param["{}_PATH".format(p)]

path_checks = [check_path_exists_op(dag, "SCRATCH_PATH", paths["SCRATCH_PATH"])]
image_check = check_worker_image_op(dag)

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
    on_failure_callback=cv_check_alert,
    queue="manager",
    dag=dag)

image_check >> path_checks >> affinity_check >> summary
