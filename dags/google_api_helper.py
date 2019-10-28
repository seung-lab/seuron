from airflow.hooks.base_hook import BaseHook
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
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

    credentials = GoogleCredentials.get_application_default()
    service = discovery.build('compute', 'v1', credentials=credentials)
    request = service.instanceGroupManagers().resize(project=project_id, zone=zone, instanceGroupManager=instance_group, size=min(size,max_size))

    response = request.execute()
    print(json.dumps(response, indent=2))


def increase_instance_group_size(connection, size):
    info = instance_group_info(connection)
    if not info:
        return

    targetSize = info['targetSize']
    if targetSize > size:
        return
    else:
        resize_instance_group(connection, size)


def reduce_instance_group_size(connection, size):
    info = instance_group_info(connection)
    if not info:
        return

    targetSize = info['targetSize']
    if targetSize < size:
        return
    else:
        resize_instance_group(connection, size)
