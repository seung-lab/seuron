from airflow.utils import db as db_utils
from airflow.models import Variable
from airflow import models
from google_metadata import get_project_data, get_instance_data, get_instance_metadata, set_instance_metadata
from param_default import param_default, inference_param_default, synaptor_param_default
import os
import requests
import json
from collections import defaultdict

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
                instance_groups[c['type']].append(worker_setting)
        elif item["key"] == "easyseg-worker":
            worker = json.loads(item["value"])
            metadata["easyseg-worker"] = {
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
    if "easyseg-worker" in metadata:
        db_utils.merge_conn(
                models.Connection(
                    conn_id='EasysegWorker', conn_type='http',
                    host=deployment, login=zone, extra=json.dumps(metadata["easyseg-worker"], indent=4)))
else:
    db_utils.merge_conn(
            models.Connection(
                conn_id='InstanceGroups', conn_type='http',
                host="localhost", login="local", extra="{}"))
