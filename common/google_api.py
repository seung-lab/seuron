from googleapiclient import discovery
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

def get_project_id():
    apiurl = "http://metadata.google.internal/computeMetadata/v1/project/project-id"
    response = requests.get(apiurl, headers={"Metadata-Flavor": "Google"})
    response.raise_for_status()
    return response.text

def get_zone():
    apiurl = "http://metadata.google.internal/computeMetadata/v1/instance/zone"
    response = requests.get(apiurl, headers={"Metadata-Flavor": "Google"})
    response.raise_for_status()
    return response.text.split('/')[-1]

def delete_instances(ig, instances):
    project_id = get_project_id()
    request_body = {
        "instances": instances,
        "skipInstancesOnValidationError": True,
    }
    service = discovery.build('compute', 'v1')
    request = service.instanceGroupManagers().deleteInstances(project=project_id, zone=ig["zone"], instanceGroupManager=ig["name"], body=request_body)
    return request.execute()

def get_created_by():
    apiurl = "http://metadata.google.internal/computeMetadata/v1/instance/attributes/created-by"
    response = requests.get(apiurl, headers={"Metadata-Flavor": "Google"})
    response.raise_for_status()
    return response.text

def instance_group_manager_info(project_id, instance_group):
    service = discovery.build('compute', 'v1')
    request = service.instanceGroupManagers().get(project=project_id, zone=instance_group['zone'], instanceGroupManager=instance_group['name'])
    return request.execute()

def instance_group_manager_error(project_id, instance_group):
    service = discovery.build('compute', 'v1')
    request = service.instanceGroupManagers().listErrors(project=project_id, zone=instance_group['zone'], instanceGroupManager=instance_group['name'], orderBy="creationTimestamp desc")
    return request.execute()

def instance_group_info(project_id, instance_group):
    service = discovery.build('compute', 'v1')
    request = service.instanceGroups().get(project=project_id, zone=instance_group['zone'], instanceGroup=instance_group['name'])
    return request.execute()

def list_managed_instances(instance_group):
    project_id = get_project_id()
    service = discovery.build("compute", "v1")
    page_token = None
    instances = []
    while True:
        request = service.instanceGroupManagers().listManagedInstances(project=project_id, zone=instance_group["zone"], instanceGroupManager=instance_group["name"], pageToken=page_token, maxResults=20)
        ret = request.execute()
        if not ret:
            return instances
        instances += [r["instance"] for r in ret['managedInstances']]
        page_token = ret.get("nextPageToken", None)
        if not page_token:
            break

    return instances

def get_instance_property(instance_zone, instance, key):
    project_id = get_project_id()
    service = discovery.build("compute", "v1")
    request = service.instances().get(project=project_id, zone=instance_zone, instance=instance)
    ret = request.execute()
    return ret[key]

def resize_instance_group(ig, size):
    project_id = get_project_id()
    service = discovery.build('compute', 'v1')
    request = service.instanceGroupManagers().resize(project=project_id, zone=ig['zone'], instanceGroupManager=ig['name'], size=size)
    return request.execute()

def start_instance(instance_name, zone):
    service = discovery.build('compute', 'v1')
    request = service.instances().start(
        project=get_project_id(),
        zone=zone,
        instance=instance_name
    )
    response = request.execute()
    return response


def stop_instance(instance_name, zone):
    service = discovery.build('compute', 'v1')
    request = service.instances().stop(
        discardLocalSsd=True,
        project=get_project_id(),
        zone=zone,
        instance=instance_name
    )
    response = request.execute()
    return response