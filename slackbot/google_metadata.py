from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
import requests
import json

def get_project_data(key):
    apiurl = f"http://metadata/computeMetadata/v1/project/{key}"
    response = requests.get(apiurl, headers={"Metadata-Flavor": "Google"})
    response.raise_for_status()
    return response.text

def get_instance_data(key):
    apiurl = f"http://metadata/computeMetadata/v1/instance/{key}"
    response = requests.get(apiurl, headers={"Metadata-Flavor": "Google"})
    response.raise_for_status()
    return response.text

def get_instance_metadata(project, zone, instance):
    credentials = GoogleCredentials.get_application_default()
    service = discovery.build('compute', 'v1', credentials=credentials)
    request = service.instances().get(project=project, zone=zone, instance=instance)
    info = request.execute()
    return info['metadata']

def set_instance_metadata(project, zone, instance, data):
    credentials = GoogleCredentials.get_application_default()
    service = discovery.build('compute', 'v1', credentials=credentials)
    request = service.instances().setMetadata(body=data, project=project, zone=zone, instance=instance)
    return request.execute()

def set_redeploy_flag(value):
    project_id = get_project_data("project-id")
    vm_name = get_instance_data("name")
    vm_zone = get_instance_data("zone").split('/')[-1]
    data = get_instance_metadata(project_id, vm_zone, vm_name)
    key_exist = False
    for item in data['items']:
        if item['key'] == 'redeploy':
            item['value'] = value
            key_exist = True

    if not key_exist:
        data['items'].append({'key': 'redeploy', 'value':value})
    set_instance_metadata(project_id, vm_zone, vm_name, data)
