from airflow.hooks.base_hook import BaseHook
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
from slack_message import slack_message
import requests
import json

def get_project_id():
    apiurl = "http://metadata.google.internal/computeMetadata/v1/project/project-id"
    response = requests.get(apiurl, headers={"Metadata-Flavor": "Google"})
    response.raise_for_status()
    return response.text


def instance_group_info(connection):
    try:
        zone = BaseHook.get_connection(connection).login
        instance_group = BaseHook.get_connection(connection).host
        project_id = get_project_id()
    except:
        return None
    credentials = GoogleCredentials.get_application_default()
    service = discovery.build('compute', 'v1', credentials=credentials)
    request = service.instanceGroupManagers().get(project=project_id, zone=zone, instanceGroupManager=instance_group)
    return request.execute()


def resize_instance_group(connection, size):
    try:
        zone = BaseHook.get_connection(connection).login
        instance_group = BaseHook.get_connection(connection).host
        max_size = int(BaseHook.get_connection(connection).extra)
        project_id = get_project_id()
    except:
        return

    if size > max_size:
        slack_message(":information_source:Limit the number of instances to {} instead of {}".format(max_size, size))
    credentials = GoogleCredentials.get_application_default()
    service = discovery.build('compute', 'v1', credentials=credentials)
    request = service.instanceGroupManagers().resize(project=project_id, zone=zone, instanceGroupManager=instance_group, size=min(size,max_size))

    response = request.execute()
    print(json.dumps(response, indent=2))
    return min(size, max_size)


def increase_instance_group_size(connection, size):
    info = instance_group_info(connection)
    if not info:
        slack_message(":exclamation:Failed to load the cluster information from connection {}".format(connection))
        slack_message(":exclamation:Cannot increase the size of the cluster to {} instances".format(size))
        return

    targetSize = info['targetSize']
    if targetSize > size:
        slack_message(":arrow_up: No need to scale up the cluster ({} instances requested, {} instances running)".format(size, targetSize))
        return
    else:
        real_size = resize_instance_group(connection, size)
        slack_message(":arrow_up: Scale up cluster {} to {} instances".format(connection, real_size))


def reduce_instance_group_size(connection, size):
    info = instance_group_info(connection)
    if not info:
        slack_message(":exclamation:Failed to load the cluster information fron connection {}".format(connection))
        slack_message(":exclamation:Cannot decrease the size of the cluster to {} instances".format(size))
        return

    targetSize = info['targetSize']
    if targetSize < size:
        slack_message(":arrow_down: No need to scale down the cluster ({} instances requested, {} instances running)".format(size, targetSize))
        return
    else:
        real_size = resize_instance_group(connection, size)
        slack_message(":arrow_down: Scale down cluster {} to {} instances".format(connection, real_size))
