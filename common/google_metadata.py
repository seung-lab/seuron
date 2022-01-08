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

def gce_external_ip():
    return get_instance_data("network-interfaces/0/access-configs/0/external-ip")

def gce_internal_ip():
    return get_instance_data("network-interfaces/0/access-configs/0/internal-ip")
