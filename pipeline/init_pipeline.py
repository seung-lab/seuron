from airflow.utils import db as db_utils
from airflow import models
import os
import requests


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

atomic_worker_group = "{deployment}-atomic-workers-{zone}".format(deployment=deployment, zone=zone)
composite_worker_group = "{deployment}-composite-workers-{zone}".format(deployment=deployment, zone=zone)

db_utils.merge_conn(
        models.Connection(
            conn_id='GCSConn', conn_type='google_cloud_platform',
            schema='default',))
db_utils.merge_conn(
        models.Connection(
            conn_id='InstanceGroup1', conn_type='http',
            host=atomic_worker_group, login=zone, extra='100'))
db_utils.merge_conn(
        models.Connection(
            conn_id='InstanceGroup2', conn_type='http',
            host=composite_worker_group, login=zone, extra='1'))
db_utils.merge_conn(
        models.Connection(
            conn_id='Slack', conn_type='http',
            host='localhost'))
