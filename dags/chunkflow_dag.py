from airflow import DAG
from airflow.models import Variable
from airflow.operators.docker_plugin import DockerWithVariablesOperator
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.utils.weight_rule import WeightRule
from param_default import inference_param_default, default_args
from igneous_and_cloudvolume import check_queue

from slack_message import slack_message, task_retry_alert, task_failure_alert

from helper_ops import slack_message_op, mark_done_op, scale_up_cluster_op, scale_down_cluster_op

from cloudvolume import CloudVolume
from cloudvolume.lib import Bbox
import os
import json
import urllib
from collections import OrderedDict

Variable.setdefault("inference_param", inference_param_default, deserialize_json=True)
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

    layers["aff"] = {
        "source": "precomputed://"+param["AFF_PATH"],
        "shader": param.get("AFF_SHADER", "void main() {\n  float r = toNormalized(getDataValue(0));\n  float g = toNormalized(getDataValue(1));\n  float b = toNormalized(getDataValue(2)); \n  emitRGB(vec3(r,g,b));\n}"),
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
    url = "neuroglancer link: {host}/#!{payload}".format(
        host=ng_host,
        payload=urllib.parse.quote(json.dumps(payload)))
    slack_message(url, broadcast=True)

def supply_default_parameters():
    param = Variable.get("inference_param", deserialize_json=True)

    cv_secrets_path = os.path.join(os.path.expanduser('~'),".cloudvolume/secrets")
    if not os.path.exists(cv_secrets_path):
        os.makedirs(cv_secrets_path)

    mount_secrets = param.get("MOUNT_SECRETES", [])

    for k in mount_secrets:
        v = Variable.get(k)
        with open(os.path.join(cv_secrets_path, k), 'w') as value_file:
            value_file.write(v)


    if "IMAGE_RESOLUTION" in param:
        try:
            vol = CloudVolume(param["IMAGE_PATH"], mip=param["IMAGE_RESOLUTION"])
        except:
            slack_message(":u7981:*ERROR: Cannot access image * `{}` at resolution {}".format(param["IMAGE_PATH"], param["IMAGE_RESOLUTION"]))
            raise ValueError('Resolution does not exist')
        if "IMAGE_MIP" in param:
            slack_message(":exclamation:*IMAGE_RESOLUTION and IMAGE_MIP are both specified, Perfer IMAGE_RESOLUTION*")
        param["IMAGE_MIP"] = vol.mip

    if "IMAGE_MIP" not in param:
        param["IMAGE_MIP"] = 0
        slack_message("*Use MIP 0 image by default*")

    print("check image done")
    try:
        vol = CloudVolume(param["IMAGE_PATH"],mip=param["IMAGE_MIP"])
    except:
        slack_message(":u7981:*ERROR: Cannot access the image layer* `{}` at MIP {}".format(param["IMAGE_PATH"], param["IMAGE_MIP"]))
        raise ValueError('Mip level does not exist')

    image_bbox = vol.bounds
    if "IMAGE_RESOLUTION" not in param:
        param["IMAGE_RESOLUTION"] = [int(x) for x in vol.resolution]
        Variable.set("inference_param", param, serialize_json=True)

    if "BBOX" not in param:
        param["BBOX"] = [int(x) for x in image_bbox.to_list()]
        Variable.set("inference_param", param, serialize_json=True)
        slack_message("*inference the whole image by default* {}".format(param["BBOX"]))

    if "AFF_PATH" not in param:
        if "AFF_PREFIX" in param:
            param["AFF_PATH"] = param["AFF_PREFIX"]+param["NAME"]
        else:
            slack_message(":u7981:*ERROR: Either AFF_PATH or AFF_PREFIX has to be specified")
            raise ValueError('No output affinity map path')

    if "AFF_MIP" not in param:
        param["AFF_MIP"] = param["IMAGE_MIP"]
        Variable.set("inference_param", param, serialize_json=True)
        slack_message("*Assume affinity resolution is the same as the image resolution* {}".format(param["IMAGE_RESOLUTION"]))

    if "IMAGE_MASK_PATH" in param and "IMAGE_MASK_MIP" not in param:
        param["IMAGE_MASK_MIP"] = param["IMAGE_MIP"]
        slack_message("*Assume image mask resolution is the same as the image resolution* {}".format(param["IMAGE_RESOLUTION"]))

    if "AFF_MASK_PATH" in param and "AFF_MASK_MIP" not in param:
        param["AFF_MASK_MIP"] = param["IMAGE_MIP"]
        slack_message("*Assume affinity mask resolution is the same as the image resolution* {}".format(param["IMAGE_RESOLUTION"]))

    if "MAX_RAM" not in param:
        param["MAX_RAM"] = 12
        slack_message("*Set memory limit of each task to 12 GB by default*")


    if "MAX_MIP" not in param:
        param["MAX_MIP"] = max(5, param.get("IMAGE_MASK_MIP",0), param.get("AFF_MASK_MIP",0), param["IMAGE_MIP"], param["AFF_MIP"])
        slack_message("*Max mip level set to {}*".format(param["MAX_MIP"]))

    target_bbox = Bbox(param["BBOX"][:3],param["BBOX"][3:])
    if not image_bbox.contains_bbox(target_bbox):
        slack_message(":u7981:*ERROR: Bounding box is outside of the image, image: {} vs bbox: {}*".format([int(x) for x in image_bbox.to_list()], param["BBOX"]))
        raise ValueError('Bounding box is outside of the image')

    Variable.set("inference_param", param, serialize_json=True)

    for k in mount_secrets:
        os.remove(os.path.join(cv_secrets_path, k))

def drain_tasks_op(dag, queue):
    param = Variable.get("inference_param", deserialize_json=True)
    cmdlist = 'bash -c "chunkflow/scripts/drain_tasks.sh"'

    return DockerWithVariablesOperator(
        ["inference_param"],
        task_id='drain_tasks',
        command=cmdlist,
        xcom_push=True,
        xcom_all=True,
        force_pull=True,
        on_failure_callback=task_failure_alert,
        default_args=default_args,
        image=param["CHUNKFLOW_IMAGE"],
        priority_weight=100000,
        weight_rule=WeightRule.ABSOLUTE,
        queue=queue,
        dag=dag
    )

def setup_env_op(dag, queue):
    param = Variable.get("inference_param", deserialize_json=True)
    cmdlist = 'bash -c "chunkflow/scripts/setup_env.sh"'

    return DockerWithVariablesOperator(
        ["inference_param"],
        task_id='setup_env',
        command=cmdlist,
        xcom_push=True,
        xcom_all=True,
        force_pull=True,
        default_args=default_args,
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


def worker_op(dag, queue, wid):
    param = Variable.get("inference_param", deserialize_json=True)
    cmdlist = 'bash -c "chunkflow/scripts/inference.sh"'

    return DockerWithVariablesOperator(
        ["inference_param"],
        task_id='worker_{}'.format(wid),
        command=cmdlist,
        force_pull=True,
        default_args=default_args,
        image=param["CHUNKFLOW_IMAGE"],
        host_args={'runtime': 'nvidia'},
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
    output = ti.xcom_pull(task_ids="setup_env").decode("utf-8")
    for l in output.split("\n"):
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

    Variable.set("inference_param", param, serialize_json=True)

    gs_log_path = conf.get('core', 'remote_log_folder')

    upload_json(os.path.join(gs_log_path,"param"), "{}.json".format(param["NAME"]), param)

    slack_message('chunkflow setup-env output: ```{}```'.format(output))

    Variable.set("chunkflow_done", "no")

    slack_message('chunkflow set_env finished')
    slack_message('Affinitymap: `{}`'.format(param["AFF_PATH"]))



dag_generator = DAG("chunkflow_generator", default_args=default_args, schedule_interval=None)
dag_worker = DAG("chunkflow_worker", default_args=default_args, schedule_interval=None)

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
scale_up_cluster_task = scale_up_cluster_op(dag_worker, "chunkflow", "gpu", min(param.get("TASK_NUM",1), 20), param.get("TASK_NUM",1), "manager")
scale_down_cluster_task = scale_down_cluster_op(dag_worker, "chunkflow", "gpu", 0, "manager")

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

set_env_task = setup_env_op(dag_generator, "manager")
drain_tasks = drain_tasks_op(dag_generator, "manager")

workers = []
skips = []


for i in range(min(param.get("TASK_NUM", 1), 100)):
    workers.append(worker_op(dag_worker, "gpu", i))
    scale_up_cluster_task >> workers[i] >> scale_down_cluster_task

sanity_check_task >> drain_tasks >> set_env_task >> process_output_task

scale_up_cluster_task >> wait_for_chunkflow_task >> mark_done_task >> generate_ng_link_task >> scale_down_cluster_task
