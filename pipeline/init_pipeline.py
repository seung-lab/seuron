from airflow.utils import db as db_utils
from airflow.models import Variable
from airflow import models
import os
import requests
import json


def gcloud_metadata(key):
    metadata_url = "http://169.254.169.254/computeMetadata/v1/{}".format(key)
    response = requests.get(metadata_url, headers={"Metadata-Flavor": "Google"})

    if response.status_code == 200:
        return response.content.decode("ascii", "ignore")
    else:
        return None


deployment = os.environ["DEPLOYMENT"]
zone = os.environ["ZONE"]

project_id = gcloud_metadata("project/project-id")
if project_id is None:
    project_id = "unknown"

host_ip = gcloud_metadata("instance/network-interfaces/0/access-configs/0/external-ip")
if host_ip is None:
    host_ip = "unknown"

gpu_worker_group = "{deployment}-gpu-workers-{zone}".format(deployment=deployment, zone=zone)
atomic_worker_group = "{deployment}-atomic-workers-{zone}".format(deployment=deployment, zone=zone)
composite_worker_group = "{deployment}-composite-workers-{zone}".format(deployment=deployment, zone=zone)
igneous_worker_group = "{deployment}-igneous-workers-{zone}".format(deployment=deployment, zone=zone)
custom_worker_group = "{deployment}-custom-workers-{zone}".format(deployment=deployment, zone=zone)

instance_groups = {
    'gpu': [{
        'name': gpu_worker_group,
        'zone': zone,
        'max_size': 100
    }],
    'atomic': [{
        'name': atomic_worker_group,
        'zone': zone,
        'max_size': 100
    }],
    'composite': [{
        'name': composite_worker_group,
        'zone': zone,
        'max_size': 1
    }],
    'igneous': [{
        'name': igneous_worker_group,
        'zone': zone,
        'max_size': 50
    }],
    'custom': [{
        'name': custom_worker_group,
        'zone': zone,
        'max_size': 50
    }]
}

target_sizes = {
    'gpu': 0,
    'atomic': 0,
    'composite': 0,
    'igneous': 0,
    'custom': 0,
}

Variable.setdefault("cluster_target_size", target_sizes, deserialize_json=True)

db_utils.merge_conn(
        models.Connection(
            conn_id='GCSConn', conn_type='google_cloud_platform',
            schema='default',))
db_utils.merge_conn(
        models.Connection(
            conn_id='InstanceGroups', conn_type='http',
            host=deployment, login=zone, extra=json.dumps(instance_groups, indent=4)))
db_utils.merge_conn(
        models.Connection(
            conn_id='Slack', conn_type='http',
            host='localhost'))
