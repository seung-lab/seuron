from time import sleep
from airflow.models import Variable
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


def instance_group_manager_info(project_id, instance_group):
    credentials = GoogleCredentials.get_application_default()
    service = discovery.build('compute', 'v1', credentials=credentials)
    request = service.instanceGroupManagers().get(project=project_id, zone=instance_group['zone'], instanceGroupManager=instance_group['name'])
    return request.execute()

def instance_group_info(project_id, instance_group):
    credentials = GoogleCredentials.get_application_default()
    service = discovery.build('compute', 'v1', credentials=credentials)
    request = service.instanceGroups().get(project=project_id, zone=instance_group['zone'], instanceGroup=instance_group['name'])
    return request.execute()

def get_cluster_target_size(project_id, instance_groups):
    total_size = 0
    for ig in instance_groups:
        info = instance_group_manager_info(project_id, ig)
        total_size += info['targetSize']
    return total_size

def get_cluster_size(project_id, instance_groups):
    total_size = 0
    for ig in instance_groups:
        info = instance_group_info(project_id, ig)
        total_size += info['size']
    return total_size

def reset_cluster(key, initial_size):
    try:
        project_id = get_project_id()
        cluster_info = json.loads(BaseHook.get_connection("InstanceGroups").extra)
    except:
        slack_message(":exclamation:Failed to load the cluster information from connection {}".format("InstanceGroups"))
        slack_message(":exclamation:Cannot reset cluster {}".format(key))
        return

    if key not in cluster_info:
        slack_message(":exclamation:Cannot find the cluster information for key {}".format(key))
        slack_message(":exclamation:Cannot reset cluster {}".format(key))
        return

    total_size = get_cluster_target_size(project_id, cluster_info[key])

    try:
        target_sizes = Variable.get("cluster_target_size", deserialize_json=True)
        target_size = target_sizes[key]
        total_size = target_size
    except:
        slack_message(":information_source: Cannot obtain the target size of cluster {}".format(key))

    slack_message(":information_source:Start reseting {} instances in cluster {}".format(total_size, key))
    ramp_down_cluster(key, 0)
    slack_message(":information_source:Reduce the number of instances to 0, wait 5 min to spin them up again")
    sleep(300)
    ramp_up_cluster(key, initial_size, total_size)
    slack_message(":information_source:{} instances in cluster {} restarted".format(total_size, key))


def resize_instance_group(project_id, instance_group, size):

    total_size = get_cluster_target_size(project_id, instance_group)

    max_size = 0
    for ig in instance_group:
        max_size += ig['max_size']

    if size > max_size:
        slack_message(":information_source:Limit the number of instances to {} instead of {}".format(max_size, size))

    downsize = False
    if size < total_size:
        downsize = True

    target_size = size
    for ig in instance_group:
        info_group_manager = instance_group_manager_info(project_id, ig)
        info_group = instance_group_info(project_id, ig)
        ig_size = min(target_size, ig['max_size'])
        if info_group_manager["targetSize"] > info_group["size"]:
            ig_size = min(ig_size, info_group["size"]+1)
        credentials = GoogleCredentials.get_application_default()
        service = discovery.build('compute', 'v1', credentials=credentials)
        request = service.instanceGroupManagers().resize(project=project_id, zone=ig['zone'], instanceGroupManager=ig['name'], size=ig_size)
        response = request.execute()
        print(json.dumps(response, indent=2))
        slack_message(":information_source: resize instance group {} to {} instances".format(ig['name'], ig_size), channel="#seuron-alerts")
        target_size -= ig_size
        if not downsize and target_size == 0:
            break
        if downsize and target_size <= 0:
            target_size = 0

    return min(size, max_size)


def ramp_up_cluster(key, initial_size, total_size):
    try:
        target_sizes = Variable.get("cluster_target_size", deserialize_json=True)
        target_sizes[key] = total_size
        Variable.set("cluster_target_size", target_sizes, serialize_json=True)
        slack_message(":information_source: ramping up cluster {} to {} instances, starting from {} instances".format(key, total_size, min(initial_size, total_size)))
        increase_instance_group_size(key, min(initial_size, total_size))
    except:
        increase_instance_group_size(key, total_size)


def ramp_down_cluster(key, total_size):
    try:
        target_sizes = Variable.get("cluster_target_size", deserialize_json=True)
        target_sizes[key] = total_size
        Variable.set("cluster_target_size", target_sizes, serialize_json=True)
        reduce_instance_group_size(key, total_size)
    except:
        reduce_instance_group_size(key, total_size)


def increase_instance_group_size(key, size):
    try:
        project_id = get_project_id()
        cluster_info = json.loads(BaseHook.get_connection("InstanceGroups").extra)
    except:
        slack_message(":exclamation:Failed to load the cluster information from connection {}".format("InstanceGroups"))
        slack_message(":exclamation:Cannot increase the size of the cluster to {} instances".format(size))
        return

    if key not in cluster_info:
        slack_message(":exclamation:Cannot find the cluster information for key {}".format(key))
        slack_message(":exclamation:Cannot increase the size of the cluster to {} instances".format(size))
        return

    total_size = get_cluster_target_size(project_id, cluster_info[key])
    if total_size > size:
        slack_message(":arrow_up: No need to scale up the cluster ({} instances requested, {} instances running)".format(size, total_size))
        return
    else:
        real_size = resize_instance_group(project_id, cluster_info[key], size)
        slack_message(":arrow_up: Scale up cluster {} to {} instances".format(key, real_size))


def reduce_instance_group_size(key, size):
    try:
        project_id = get_project_id()
        cluster_info = json.loads(BaseHook.get_connection("InstanceGroups").extra)
    except:
        slack_message(":exclamation:Failed to load the cluster information from connection {}".format("InstanceGroups"))
        slack_message(":exclamation:Cannot reduce the size of the cluster to {} instances".format(size))
        return

    if key not in cluster_info:
        slack_message(":exclamation:Cannot find the cluster information for key {}".format(key))
        slack_message(":exclamation:Cannot reduce the size of the cluster to {} instances".format(size))
        return

    total_size = get_cluster_target_size(project_id, cluster_info[key])
    if total_size < size:
        slack_message(":arrow_down: No need to scale down the cluster ({} instances requested, {} instances running)".format(size, total_size))
        return
    else:
        real_size = resize_instance_group(project_id, cluster_info[key], size)
        slack_message(":arrow_down: Scale down cluster {} to {} instances, sleep for one minute to let it stablize".format(key, real_size))
        sleep(60)
