from airflow.utils import db as db_utils
from airflow.models import Variable
from airflow import models
from google_metadata import get_project_data, get_instance_data, get_instance_metadata, set_instance_metadata
from param_default import param_default, inference_param_default, synaptor_param_default
import os
import json
import docker
import socket
from collections import defaultdict


def get_image_info():
    client = docker.APIClient(base_url='unix://var/run/docker.sock')
    container_id = socket.gethostname()
    container_info = client.inspect_container(container=container_id)
    image_info = container_info["Config"]['Image'].split("@")
    if len(image_info) == 2:
        return image_info
    elif len(image_info) == 1:
        image_data = client.inspect_image(image=image_info[0])
        image_sha256 = image_data['RepoDigests'][0].split("@")[1]
        return image_info[0], image_sha256
    return None


def parse_metadata():
    project_id = get_project_data("project-id")
    vm_name = get_instance_data("name")
    vm_zone = get_instance_data("zone").split('/')[-1]
    data = get_instance_metadata(project_id, vm_zone, vm_name)
    instance_groups = defaultdict(list)
    metadata = {}
    for item in data['items']:
        if item['key'] == "cluster-info":
            clusters = json.loads(item['value'])
            for c in clusters:
                worker_setting = {
                        'name': c['name'],
                        'zone': c['zone'],
                        'max_size': int(c['sizeLimit']),
                }
                if c['type'] == 'composite':
                    worker_setting['workerConcurrencies'] = c['workerConcurrencies']
                else:
                    worker_setting['concurrency'] = c.get('concurrency', 1)
                if c['type'] == 'deepem-gpu':
                    worker_setting['gpuWorkerAcceleratorCount'] = c.get('gpuWorkerAcceleratorCount', 1)
                instance_groups[c['type']].append(worker_setting)
        elif item["key"] == "easyseg-worker":
            worker = json.loads(item["value"])
            metadata["easyseg-worker"] = {
                    'zone': worker['zone'],
            }
        elif item["key"] == "nfs-server":
            worker = json.loads(item["value"])
            metadata["nfs-server"] = {
                    'hostname': worker['hostname'],
                    'zone': worker['zone'],
            }

        metadata['cluster-info'] = instance_groups
    return metadata



target_sizes = {
    'gpu': 0,
    'atomic': 0,
    'composite': 0,
    'igneous': 0,
    'custom-cpu': 0,
    'custom-gpu': 0,
}

Variable.setdefault("cluster_target_size", target_sizes, deserialize_json=True)
Variable.setdefault("param", param_default, deserialize_json=True)
Variable.setdefault("inference_param", inference_param_default, deserialize_json=True)
Variable.setdefault("vendor", os.environ.get("VENDOR", "Google"))
Variable.setdefault("synaptor_param.json", synaptor_param_default, deserialize_json=True)

db_utils.merge_conn(
        models.Connection(
            conn_id='Slack', conn_type='http',
            host='localhost', extra=json.dumps({"notification_channel": os.environ.get("SLACK_NOTIFICATION_CHANNEL", "seuron-alerts")})))

db_utils.merge_conn(
        models.Connection(
            conn_id='LLMServer', conn_type='openai',
            host=os.environ.get('LLM_HOST', 'https://api.openai.com/v1'),
            password="",
            extra=json.dumps({
                                "model": os.environ.get("LLM_MODEL", "gpt-4.1"),
                                "temperature": 0.5,
                              })))

if os.environ.get("VENDOR", None) == "Google":
    deployment = os.environ.get("DEPLOYMENT", None)
    zone = os.environ.get("ZONE", None)
    metadata = parse_metadata()
    db_utils.merge_conn(
            models.Connection(
                conn_id='GCSConn', conn_type='google_cloud_platform',
                schema='default',))

    db_utils.merge_conn(
            models.Connection(
                conn_id='InstanceGroups', conn_type='http',
                host=deployment, login=zone, extra=json.dumps(metadata["cluster-info"], indent=4)))

    image_info = get_image_info()

    if image_info and len(image_info) == 2:
        Variable.set("image_info",
                            {
                                "name": image_info[0],
                                "checksum": image_info[1]
                            }, serialize_json=True)

    if "easyseg-worker" in metadata:
        db_utils.merge_conn(
                models.Connection(
                    conn_id='EasysegWorker', conn_type='http',
                    host=deployment, login=metadata["easyseg-worker"]["zone"], extra=json.dumps(metadata["easyseg-worker"], indent=4)))
    if "nfs-server" in metadata:
        db_utils.merge_conn(
                models.Connection(
                    conn_id='NFSServer', conn_type='http',
                    host=deployment, login=metadata["nfs-server"]["zone"], extra=json.dumps(metadata["nfs-server"], indent=4)))
else:
    db_utils.merge_conn(
            models.Connection(
                conn_id='InstanceGroups', conn_type='http',
                host="localhost", login="local", extra="{}"))
