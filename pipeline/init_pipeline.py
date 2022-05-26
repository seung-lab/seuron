from airflow.utils import db as db_utils
from airflow.models import Variable
from airflow import models
from google_metadata import get_project_data, get_instance_data, get_instance_metadata, set_instance_metadata
import os
import requests
import re
import json
from collections import defaultdict


def get_clusters(deployment):
    re_ig = deployment+r'''-([\w-]+)-workers-([\w-]+)'''

    project_id = get_project_data("project-id")
    vm_name = get_instance_data("name")
    vm_zone = get_instance_data("zone").split('/')[-1]
    data = get_instance_metadata(project_id, vm_zone, vm_name)
    instance_groups = defaultdict(list)
    for item in data['items']:
        m = re.match(re_ig, item['key'])
        if m:
            cluster = m[1]
            zone = m[2]
            instance_groups[cluster].append(
                {
                    'name': item['key'],
                    'zone': zone,
                    'max_size': int(item['value']),
                },
            )

    return instance_groups


deployment = os.environ["DEPLOYMENT"]
zone = os.environ["ZONE"]

instance_groups = get_clusters(deployment)

target_sizes = {
    'gpu': 0,
    'atomic': 0,
    'composite': 0,
    'igneous': 0,
    'custom-cpu': 0,
    'custom-gpu': 0,
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
            host='localhost', extra=json.dumps({"notification_channel": os.environ["SLACK_NOTIFICATION_CHANNEL"]})))
