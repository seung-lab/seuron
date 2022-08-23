from airflow import DAG
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook
from worker_op import worker_op
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.weight_rule import WeightRule
from param_default import default_args, default_mount_path, default_chunkflow_workspace, check_worker_image_labels
from datetime import datetime
from igneous_and_cloudvolume import check_queue, cv_has_data, cv_scale_with_data

from slack_message import slack_message, task_retry_alert, task_failure_alert

from helper_ops import slack_message_op, mark_done_op, scale_up_cluster_op, scale_down_cluster_op, setup_redis_op

from cloudvolume import CloudVolume
from cloudvolume.lib import Bbox
import os
import json
import urllib
from collections import OrderedDict

param = Variable.get("inference_param", deserialize_json=True)

def generate_ng_link():
    param = Variable.get("inference_param", deserialize_json=True)
    ng_host = "https://neuromancer-seung-import.appspot.com"

    layers = OrderedDict()

    layers["img"] = {
        "source": "precomputed://"+param["IMAGE_PATH"],
        "type": "image"
    }
    if "IMAGE_SHADER" in param:
        layers["img"]["shader"] = param["IMAGE_SHADER"]

    layers["out"] = {
        "source": "precomputed://"+param["OUTPUT_PATH"],
        "shader": param.get("OUTPUT_SHADER", "void main() {\n  float r = toNormalized(getDataValue(0));\n  float g = toNormalized(getDataValue(1));\n  float b = toNormalized(getDataValue(2)); \n  emitRGB(vec3(r,g,b));\n}"),
        "type": "image"
    }

    bbox = param["BBOX"]

    center = [(bbox[i]+bbox[i+3])/2 for i in range(3)]

    navigation = {
        "pose": {
            "position": {
                "voxelSize": param["IMAGE_RESOLUTION"],
                "voxelCoordinates": center
            }
        },
        "zoomFactor": 4
    }
    payload = OrderedDict([("layers", layers),("navigation", navigation),("showSlices", False),("layout", "xy-3d")])
    url = "<{host}/#!{payload}|*view the results in neuroglancer*>".format(
        host=ng_host,
        payload=urllib.parse.quote(json.dumps(payload)))
    slack_message(url, broadcast=True)


def check_patch_parameters(param):
    if "INPUT_PATCH_SIZE" not in param:
        slack_message("Use default input patch size: `[256,256,20]`")
        param["INPUT_PATCH_SIZE"] = [256,256,20]
    if "OUTPUT_PATCH_SIZE" not in param:
        slack_message("No cropping by default, output patch size: `{}`".format(param["INPUT_PATCH_SIZE"]))
        param["OUTPUT_PATCH_SIZE"] = param["INPUT_PATCH_SIZE"][:]

    input_patch_size = param["INPUT_PATCH_SIZE"]
    output_patch_size = param["OUTPUT_PATCH_SIZE"]

    if (any(x < y for x, y in zip(input_patch_size, output_patch_size))):
        slack_message("""input patch size smaller than output patch size""")
        raise ValueError('Parameter mismatch')

    if "OUTPUT_PATCH_OVERLAP" not in param:
        if "INPUT_PATCH_OVERLAP_RATIO" not in param:
            slack_message("""Use 50% overlap between input patches""")

    overlap = param.get("INPUT_PATCH_OVERLAP_RATIO", 0.5)
    output_patch_overlap = [ int(i*overlap - (i-o)) for i, o in zip(input_patch_size, output_patch_size)  ]
    output_patch_overlap = [o+o%2 for o in output_patch_overlap]


    if "OUTPUT_PATCH_OVERLAP" in param and "INPUT_PATCH_OVERLAP_RATIO" in param:
        if any(x != y for x, y in zip(output_patch_overlap, param["OUTPUT_PATCH_OVERLAP"])):
            slack_message(f":u7981:*ERROR: Output patch overlap* `f{param['OUTPUT_PATCH_OVERLAP']}` *is inconsistent with input patch overlap ratio* `f{overlap}`")
            raise ValueError('Parameter mismatch')

    if "OUTPUT_PATCH_OVERLAP" not in param:
        if any(x < 0 for x in output_patch_overlap):
            slack_message(":u7981:*ERROR: The output patches have gaps after cropping, not enough overlap between input patches")
            raise ValueError('Input patch overlap too small')
        param["OUTPUT_PATCH_OVERLAP"] = output_patch_overlap
        slack_message(f'Output patch overlap: `{param["OUTPUT_PATCH_OVERLAP"]}`')


    if "CHUNK_CROP_MARGIN" not in param:
        param["CHUNK_CROP_MARGIN"] = [o + (ip - op)//2 for o, ip, op in zip(param["OUTPUT_PATCH_OVERLAP"], input_patch_size, output_patch_size)]
        slack_message(f'Chunk crop margin: `{param["CHUNK_CROP_MARGIN"]}`')

    return param


def supply_default_parameters():
    from docker_helper import health_check_info
    from airflow import configuration as conf
    param = Variable.get("inference_param", deserialize_json=True)

    statsd_host = conf.get('metrics', 'statsd_host')
    statsd_port = conf.get('metrics', 'statsd_port')

    if not param.get("STATSD_HOST", ""):
        param["STATSD_HOST"] = statsd_host

    if not param.get("STATSD_PORT", ""):
        param["STATSD_PORT"] = statsd_port

    cv_secrets_path = os.path.join(os.path.expanduser('~'),".cloudvolume/secrets")
    if not os.path.exists(cv_secrets_path):
        os.makedirs(cv_secrets_path)

    mount_secrets = param.get("MOUNT_SECRETS", [])

    for k in mount_secrets:
        v = Variable.get(k)
        with open(os.path.join(cv_secrets_path, k), 'w') as value_file:
            value_file.write(v)


    def check_matching_mip(path1, path2):
        vol1 = CloudVolume(path1, mip=0)
        vol2 = CloudVolume(path2, mip=0)
        if all(x == y for x, y in zip(vol1.resolution, vol2.resolution)):
            return True
        else:
            slack_message(":u7981:*ERROR: mip0 resolutions mismatch* {} *vs* {}".format(
                vol1.resolution, vol2.resolution))
            return False

    if "ONNX_MODEL_PATH" in param and "PYTORCH_MODEL_PATH" in param:
        slack_message(":u7981:*ERROR: Cannot specify pytorch model and onnx model at the same time*")
        raise ValueError('Can only use one backend')

    if param.get("ENABLE_FP16", False):
        slack_message(":exclamation:*Enable FP16 inference for TensorRT*")

    if "CHUNKFLOW_IMAGE" not in param:
        slack_message(":u7981:*ERROR: You have to specify a chunkflow image")
        raise ValueError('chunkflow image missing')


    if param.get("IMAGE_MASK_PATH", "N/A") != "N/A" and (not check_matching_mip(param["IMAGE_PATH"], param["IMAGE_MASK_PATH"])):
        raise ValueError('Resolution mismatch')


    if param.get("OUTPUT_MASK_PATH", "N/A") != "N/A" and (not check_matching_mip(param["IMAGE_PATH"], param["OUTPUT_MASK_PATH"])):
        raise ValueError('Resolution mismatch')


    if "IMAGE_RESOLUTION" in param:
        try:
            vol = CloudVolume(param["IMAGE_PATH"], mip=param["IMAGE_RESOLUTION"])
        except:
            slack_message(":u7981:*ERROR: Cannot access image * `{}` *at resolution {}*".format(param["IMAGE_PATH"], param["IMAGE_RESOLUTION"]))
            raise ValueError('Resolution does not exist')
        if "IMAGE_MIP" in param:
            slack_message(":exclamation:*IMAGE_RESOLUTION and IMAGE_MIP are both specified, Perfer IMAGE_RESOLUTION*")
        param["IMAGE_MIP"] = vol.mip

    if "IMAGE_MASK_RESOLUTION" in param and param.get("IMAGE_MASK_PATH", "N/A") != "N/A":
        try:
            vol = CloudVolume(param["IMAGE_MASK_PATH"], mip=param["IMAGE_MASK_RESOLUTION"])
        except:
            slack_message(":u7981:*ERROR: Cannot access image mask * `{}` *at resolution {}*".format(param["IMAGE_MASK_PATH"], param["IMAGE_MASK_RESOLUTION"]))
            raise ValueError('Resolution does not exist')

        if "IMAGE_MASK_MIP" in param:
            slack_message(":exclamation:*IMAGE_MASK_RESOLUTION and IMAGE_MASK_MIP are both specified, Perfer IMAGE_MASK_RESOLUTION*")
        param["IMAGE_MASK_MIP"] = vol.mip

        slack_message(":exclamation:*Use image mask * `{}` *at resolution {}*".format(param["IMAGE_MASK_PATH"], param["IMAGE_MASK_RESOLUTION"]))

    if "OUTPUT_MASK_RESOLUTION" in param and param.get("OUTPUT_MASK_PATH", "N/A") != "N/A":
        try:
            vol = CloudVolume(param["OUTPUT_MASK_PATH"], mip=param["OUTPUT_MASK_RESOLUTION"])
        except:
            slack_message(":u7981:*ERROR: Cannot access output mask * `{}` *at resolution {}*".format(param["OUTPUT_MASK_PATH"], param["OUTPUT_MASK_RESOLUTION"]))
            raise ValueError('Resolution does not exist')

        if "OUTPUT_MASK_MIP" in param:
            slack_message(":exclamation:*OUTPUT_MASK_RESOLUTION and OUTPUT_MASK_MIP are both specified, Perfer OUTPUT_MASK_RESOLUTION*")
        param["OUTPUT_MASK_MIP"] = vol.mip

        slack_message(":exclamation:*Use output mask * `{}` *at resolution {}*".format(param["OUTPUT_MASK_PATH"], param["OUTPUT_MASK_RESOLUTION"]))

    if "IMAGE_MIP" not in param:
        try:
            param["IMAGE_MIP"], param["IMAGE_RESOLUTION"] = cv_scale_with_data(param["IMAGE_PATH"])
        except:
            slack_message(":u7981:*ERROR: Cannot access the images in* `{}`".format(param["IMAGE_PATH"]))
            raise ValueError("No data")

        slack_message("*Use images at resolution {}*".format(param["IMAGE_RESOLUTION"]))
        Variable.set("inference_param", param, serialize_json=True)

    print("check image done")
    try:
        vol = CloudVolume(param["IMAGE_PATH"],mip=param["IMAGE_MIP"])
    except:
        slack_message(":u7981:*ERROR: Cannot access the image layer* `{}` *at MIP {}*".format(param["IMAGE_PATH"], param["IMAGE_MIP"]))
        raise ValueError('Mip level does not exist')

    if not cv_has_data(param["IMAGE_PATH"], mip=param["IMAGE_MIP"]):
        resolution = vol.scales[param["IMAGE_MIP"]]['resolution']
        slack_message(":u7981:*ERROR: No data in* `{}`  *at resolution {} (mip {})*".format(param["IMAGE_PATH"], resolution, param["IMAGE_MIP"]))
        raise ValueError('No data available')


    image_bbox = vol.bounds
    if "IMAGE_RESOLUTION" not in param:
        param["IMAGE_RESOLUTION"] = vol.resolution.tolist()
        Variable.set("inference_param", param, serialize_json=True)

    if "BBOX" not in param:
        param["BBOX"] = [int(x) for x in image_bbox.to_list()]
        Variable.set("inference_param", param, serialize_json=True)
        slack_message("*inference the whole image by default* {}".format(param["BBOX"]))

    if "OUTPUT_PATH" not in param:
        if "OUTPUT_PREFIX" in param:
            param["OUTPUT_PATH"] = param["OUTPUT_PREFIX"]+param["NAME"]
        else:
            slack_message(":u7981:*ERROR: Either OUTPUT_PATH or OUTPUT_PREFIX has to be specified")
            raise ValueError('No output output path')

    if "OUTPUT_MIP" not in param:
        param["OUTPUT_MIP"] = param["IMAGE_MIP"]
        Variable.set("inference_param", param, serialize_json=True)
        slack_message("*Assume output resolution is the same as the image resolution* {}".format(param["IMAGE_RESOLUTION"]))

    if param.get("IMAGE_HISTOGRAM_PATH", "N/A") != "N/A":
        slack_message("*Normalize images with histograms in* `{}`, *lower threshold: {}, upper threshold: {}*".format(param["IMAGE_HISTOGRAM_PATH"], param.get("CONTRAST_NORMALIZATION_LOWER_THRESHOLD", 0.01), param.get("CONTRAST_NORMALIZATION_UPPER_THRESHOLD", 0.99)))

    if param.get("IMAGE_MASK_PATH", "N/A") != "N/A" and "IMAGE_MASK_MIP" not in param:
        param["IMAGE_MASK_MIP"] = param["IMAGE_MIP"]
        slack_message("*Assume image mask resolution is the same as the image resolution* {}".format(param["IMAGE_RESOLUTION"]))

    if param.get("OUTPUT_MASK_PATH", "N/A") != "N/A" and "OUTPUT_MASK_MIP" not in param:
        param["OUTPUT_MASK_MIP"] = param["IMAGE_MIP"]
        slack_message("*Assume output mask resolution is the same as the image resolution* {}".format(param["IMAGE_RESOLUTION"]))

    if "MAX_RAM" not in param:
        param["MAX_RAM"] = 4
        slack_message("*Set memory limit of each task to 4 GB by default*")

    if "MAX_MIP" not in param:
        param["MAX_MIP"] = max(param.get("IMAGE_MASK_MIP",0), param.get("OUTPUT_MASK_MIP",0), param["IMAGE_MIP"], param["OUTPUT_MIP"])
        slack_message("*Max mip level set to {}*".format(param["MAX_MIP"]))

    if param.get("INFERENCE_FRAMEWORK", "pytorch") != "pytorch":
        slack_message("*Use {} backend for inference*".format(param["INFERENCE_FRAMEWORK"]))

    if "OUTPUT_CHANNELS" in param and "INFERENCE_OUTPUT_CHANNELS" not in param:
        param["INFERENCE_OUTPUT_CHANNELS"] = param["OUTPUT_CHANNELS"]
    elif "OUTPUT_CHANNELS" not in param and "INFERENCE_OUTPUT_CHANNELS" in param:
        param["OUTPUT_CHANNELS"] = param["INFERENCE_OUTPUT_CHANNELS"]

    if param.get("MYELIN_MASK_THRESHOLD", "N/A") != "N/A":
        if param.get("OUTPUT_CHANNELS", 3) != 3 or param.get("INFERENCE_OUTPUT_CHANNELS", 3) != 4:
            slack_message(":u7981:*ERROR: Myelin mask threshold requires 3 OUTPUT_CHANNELS and 4 INFERENCE_OUTPUT_CHANNELS*")
        else:
            slack_message("*Apply myelin mask with threshold {}*".format(float(param["MYELIN_MASK_THRESHOLD"])))

    if param.get("POSTPROC", "N/A") != "N/A":
        slack_message("*Post process the inference output with operator* `{}`".format(param["POSTPROC"]))

    if param.get("OUTPUT_DTYPE", "float32") != "float32":
        slack_message("*Write the output as {}*".format(param.get("OUTPUT_DTYPE", "float32")))

    if param.get("OUTPUT_CHANNELS", 3) != 3 or param.get("INFERENCE_OUTPUT_CHANNELS", 3) != 3:
        slack_message("*Inference the input into {} channels and output {} channels*".format(param.get("INFERENCE_OUTPUT_CHANNELS", 3),param.get("OUTPUT_CHANNELS", 3)))

    target_bbox = Bbox(param["BBOX"][:3],param["BBOX"][3:])
    if not image_bbox.contains_bbox(target_bbox):
        slack_message(":u7981:*ERROR: Bounding box is outside of the image, image: {} vs bbox: {}*".format([int(x) for x in image_bbox.to_list()], param["BBOX"]))
        raise ValueError('Bounding box is outside of the image')

    Variable.set("inference_param", check_patch_parameters(param), serialize_json=True)

    health_check_info(param["CHUNKFLOW_IMAGE"])

    for k in mount_secrets:
        os.remove(os.path.join(cv_secrets_path, k))

def drain_tasks_op(dag, param, queue):
    from airflow import configuration as conf
    broker_url = conf.get('celery', 'broker_url')
    workspace_path = param.get("WORKSPACE_PATH", default_chunkflow_workspace)
    cmdlist = f'bash -c "{os.path.join(workspace_path, "scripts/drain_tasks.sh")} {broker_url}"'

    cm = ['inference_param']
    if "MOUNT_SECRETS" in param:
        cm += param["MOUNT_SECRETS"]

    return worker_op(
        variables=cm,
        mount_point=param.get("MOUNT_PATH", default_mount_path),
        task_id='drain_tasks',
        command=cmdlist,
        force_pull=True,
        on_failure_callback=task_failure_alert,
        image=param["CHUNKFLOW_IMAGE"],
        priority_weight=100000,
        weight_rule=WeightRule.ABSOLUTE,
        queue=queue,
        dag=dag
    )

def setup_env_op(dag, param, queue):
    from airflow import configuration as conf
    broker_url = conf.get('celery', 'broker_url')
    workspace_path = param.get("WORKSPACE_PATH", default_chunkflow_workspace)
    cmdlist = f'bash -c "{os.path.join(workspace_path, "scripts/setup_env.sh")} {broker_url}"'

    cm = ['inference_param']
    if "MOUNT_SECRETS" in param:
        cm += param["MOUNT_SECRETS"]

    return worker_op(
        variables=cm,
        mount_point=param.get("MOUNT_PATH", default_mount_path),
        task_id='setup_env',
        command=cmdlist,
        do_xcom_push=True,
        xcom_all=True,
        force_pull=True,
        on_failure_callback=task_failure_alert,
        image=param["CHUNKFLOW_IMAGE"],
        priority_weight=100000,
        weight_rule=WeightRule.ABSOLUTE,
        queue=queue,
        dag=dag
    )

def chunkflow_running():
    cf_done = Variable.get("chunkflow_done")
    if (cf_done == "yes"):
        return False
    else:
        return True


def skip_worker_op(dag, queue, wid):
    return ShortCircuitOperator(
        task_id="skip_worker_{}".format(wid),
        python_callable=chunkflow_running,
        priority_weight=1,
        weight_rule=WeightRule.ABSOLUTE,
        queue=queue,
        dag=dag
    )


def inference_op(dag, param, queue, wid):
    from airflow import configuration as conf
    broker_url = conf.get('celery', 'broker_url')
    workspace_path = param.get("WORKSPACE_PATH", default_chunkflow_workspace)
    cmdlist = f'bash -c "{os.path.join(workspace_path, "scripts/inference.sh")} {broker_url}"'

    cm = ['inference_param']
    if "MOUNT_SECRETS" in param:
        cm += param["MOUNT_SECRETS"]

    return worker_op(
        variables=cm,
        mount_point=param.get("MOUNT_PATH", default_mount_path),
        task_id='worker_{}'.format(wid),
        command=cmdlist,
        force_pull=True,
        image=param["CHUNKFLOW_IMAGE"],
        priority_weight=1,
        weight_rule=WeightRule.ABSOLUTE,
        on_retry_callback=task_retry_alert,
        queue=queue,
        dag=dag
    )


def process_output(**kwargs):
    from igneous_and_cloudvolume import upload_json
    from airflow import configuration as conf
    import re
    param = Variable.get("inference_param", deserialize_json=True)
    ti = kwargs['ti']
    output = ti.xcom_pull(task_ids="setup_env")
    for l in output:
        if l.startswith("patch number:"):
            m = re.search("\((\d+),\s*(\d+),\s*(\d+)\)", l)
            patch_number = list(m.group(1,2,3))
            param["PATCH_NUM"] = " ".join(patch_number)
            slack_message("Suggested patch number: [{}]".format(",".join(patch_number[::-1])))

        if l.startswith("cutout expand margin size:"):
            m = re.search("\((\d+),\s*(\d+),\s*(\d+)\)", l)
            expand_margin_size = list(m.group(1,2,3))
            param["EXPAND_MARGIN_SIZE"] = " ".join(expand_margin_size)
            slack_message("Suggested expand margin size: [{}]".format(",".join(expand_margin_size[::-1])))

        if l.startswith("total number of tasks:"):
            m = re.search("\d+", l)
            task_number = int(m.group(0))
            param["TASK_NUM"] = task_number

    for k in ['PATCH_NUM', 'EXPAND_MARGIN_SIZE', 'TASK_NUM']:
        if k not in param:
            slack_message(":u7981:*ERROR: Fail to capture {} from the chunkflow output, bail*".format(k))
            raise ValueError('Chunkflow output error')

    Variable.set("inference_param", param, serialize_json=True)

    gs_log_path = conf.get('logging', 'remote_base_log_folder')

    upload_json(os.path.join(gs_log_path,"param"), "{}.json".format(param["NAME"]), param)

    slack_message('chunkflow setup-env output: ```{}```'.format("\n".join(output)))

    Variable.set("chunkflow_done", "no")

    slack_message('chunkflow set_env finished')
    slack_message('Output path: `{}`'.format(param["OUTPUT_PATH"]), broadcast=True)

generator_default_args = {
    'owner': 'seuronbot',
    'depends_on_past': False,
    'start_date': datetime(2019, 2, 8),
    'catchup': False,
    'retries': 0,
}


dag_generator = DAG("chunkflow_generator", default_args=generator_default_args, schedule_interval=None, tags=['inference'])
dag_worker = DAG("chunkflow_worker", default_args=default_args, schedule_interval=None, tags=['inference'])

image_parameters = PythonOperator(
    task_id="setup_image_parameters",
    python_callable=check_worker_image_labels,
    op_args = ("inference_param",),
    on_failure_callback=task_failure_alert,
    weight_rule=WeightRule.ABSOLUTE,
    queue="manager",
    dag=dag_generator)


sanity_check_task = PythonOperator(
    task_id="supply_default_parameters",
    python_callable=supply_default_parameters,
    priority_weight=100000,
    on_failure_callback=task_failure_alert,
    weight_rule=WeightRule.ABSOLUTE,
    queue="manager",
    dag=dag_generator
)

process_output_task = PythonOperator(
    task_id="process_output",
    provide_context=True,
    python_callable=process_output,
    priority_weight=100000,
    on_failure_callback=task_failure_alert,
    weight_rule=WeightRule.ABSOLUTE,
    queue="manager",
    dag=dag_generator
)

cluster_info = json.loads(BaseHook.get_connection("InstanceGroups").extra)
total_gpus = sum(c['max_size'] for c in cluster_info['gpu'])

scale_up_cluster_task = scale_up_cluster_op(dag_worker, "chunkflow", "gpu", min(param.get("TASK_NUM",1), 20), min(total_gpus, param.get("TASK_NUM",1)), "cluster")
scale_down_cluster_task = scale_down_cluster_op(dag_worker, "chunkflow", "gpu", 0, "cluster")

wait_for_chunkflow_task = PythonOperator(
    task_id="wait_for_chunkflow",
    python_callable=check_queue,
    op_args=("chunkflow",),
    priority_weight=100000,
    weight_rule=WeightRule.ABSOLUTE,
    queue="manager",
    dag=dag_worker
)

generate_ng_link_task = PythonOperator(
    task_id="generate_ng_link",
    python_callable=generate_ng_link,
    priority_weight=100000,
    weight_rule=WeightRule.ABSOLUTE,
    queue="manager",
    dag=dag_worker
)

mark_done_task = mark_done_op(dag_worker, "chunkflow_done")

setup_redis_task = setup_redis_op(dag_generator, "inference_param", "CHUNKFLOW")

set_env_task = setup_env_op(dag_generator, param, "manager")
drain_tasks = drain_tasks_op(dag_generator, param, "manager")

workers = []
queue = 'gpu'

for i in range(min(param.get("TASK_NUM", 1), total_gpus)):
    workers.append(inference_op(dag_worker, param, queue, i))

scale_up_cluster_task >> workers >> scale_down_cluster_task

setup_redis_task >> sanity_check_task >> image_parameters >> drain_tasks >> set_env_task >> process_output_task

scale_up_cluster_task >> wait_for_chunkflow_task >> mark_done_task >> generate_ng_link_task >> scale_down_cluster_task
