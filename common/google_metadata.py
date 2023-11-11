from googleapiclient import discovery
import requests

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
    service = discovery.build('compute', 'v1')
    request = service.instances().get(project=project, zone=zone, instance=instance)
    info = request.execute()
    return info['metadata']

def set_instance_metadata(project, zone, instance, data):
    service = discovery.build('compute', 'v1')
    request = service.instances().setMetadata(body=data, project=project, zone=zone, instance=instance)
    return request.execute()

def gce_external_ip():
    return get_instance_data("network-interfaces/0/access-configs/0/external-ip")

def gce_internal_ip():
    return get_instance_data("network-interfaces/0/ip")

def gce_hostname():
    return get_instance_data("hostname")
