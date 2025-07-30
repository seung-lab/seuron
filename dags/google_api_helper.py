from time import sleep
from datetime import datetime, timedelta, timezone
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook
from googleapiclient import discovery
from slack_message import slack_message
import requests
import json


def get_project_id():
    apiurl = "http://metadata.google.internal/computeMetadata/v1/project/project-id"
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

def delete_instances(ig, instances):
    project_id = get_project_id()
    request_body = {
        "instances": instances,
        "skipInstancesOnValidationError": True,
    }
    service = discovery.build('compute', 'v1')
    request = service.instanceGroupManagers().deleteInstances(project=project_id, zone=ig["zone"], instanceGroupManager=ig["name"], body=request_body)
    ret = request.execute()
    print(ret)

def get_cluster_target_size(project_id, instance_groups):
    total_size = 0
    for ig in instance_groups:
        info = instance_group_manager_info(project_id, ig)
        total_size += info['targetSize']
    return total_size

def get_cluster_errors(project_id, instance_groups):
    msg = []
    for ig in instance_groups:
        items = instance_group_manager_error(project_id, ig)

        if len(items["items"]) == 0:
            continue

        item = items["items"][0]

        dt = datetime.fromisoformat(item.get('timestamp'))
        now = datetime.now(timezone.utc)

        if now - timedelta(minutes=10) <= dt <= now:
            msg.append(f"Instance Group: {ig['name']}")
            msg.append(f"Error: {item['error']['code']}")
            msg.append(f"Message: {item['error']['message']}")
    return msg

def get_cluster_size(project_id, instance_groups):
    total_size = 0
    for ig in instance_groups:
        info = instance_group_info(project_id, ig)
        total_size += info['size']
    return total_size

def reset_cluster(key, initial_size):
    run_metadata = Variable.get("run_metadata", deserialize_json=True, default_var={})
    if not run_metadata.get("manage_clusters", True):
        return
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

    slack_message(":information_source:Start resetting {} instances in cluster {}".format(total_size, key))
    ramp_down_cluster(key, 0)
    slack_message(":information_source:Reduce the number of instances to 0, wait 5 min to spin them up again")
    sleep(300)
    ramp_up_cluster(key, initial_size, total_size)
    slack_message(":information_source:{} instances in cluster {} restarted".format(total_size, key))


def resize_instance_group(instance_group, size):
    project_id = get_project_id()

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
        if ig_size < info_group["size"] and not downsize:
            continue
        if info_group_manager["targetSize"] > info_group["size"]:
            ig_size = min(ig_size, info_group["size"]+1)
        service = discovery.build('compute', 'v1')
        request = service.instanceGroupManagers().resize(project=project_id, zone=ig['zone'], instanceGroupManager=ig['name'], size=ig_size)
        response = request.execute()
        print(json.dumps(response, indent=2))
        slack_message(":information_source: resize instance group {} to {} instances".format(ig['name'], ig_size), notification=True)
        target_size -= ig_size
        if not downsize and target_size == 0:
            break
        if downsize and target_size <= 0:
            target_size = 0
        sleep(30)

    return min(size, max_size)


def ramp_up_cluster(key, initial_size, total_size):
    run_metadata = Variable.get("run_metadata", deserialize_json=True, default_var={})
    if not run_metadata.get("manage_clusters", True):
        return
    try:
        target_sizes = Variable.get("cluster_target_size", deserialize_json=True)
        target_sizes[key] = total_size
        Variable.set("cluster_target_size", target_sizes, serialize_json=True)
        slack_message(":information_source: ramping up cluster {} to {} instances, starting from {} instances".format(key, total_size, min(initial_size, total_size)))
        increase_instance_group_size(key, min(initial_size, total_size))
    except:
        increase_instance_group_size(key, total_size)


def ramp_down_cluster(key, total_size):
    run_metadata = Variable.get("run_metadata", deserialize_json=True, default_var={})
    if not run_metadata.get("manage_clusters", True):
        return
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
        real_size = resize_instance_group(cluster_info[key], size)
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
        real_size = resize_instance_group(cluster_info[key], size)
        slack_message(":arrow_down: Scale down cluster {} to {} instances, sleep for one minute to let it stabilize".format(key, real_size))
        sleep(60)


def cluster_status(name, cluster):
    project_id = get_project_id()
    current_size = get_cluster_size(project_id, cluster)
    requested_size = get_cluster_target_size(project_id, cluster)
    errors = get_cluster_errors(project_id, cluster)
    stable = True
    if requested_size > 0:
        slack_message(":information_source: status of cluster {}: {} out of {} instances up and running".format(name, current_size, requested_size), notification=True)

    if (requested_size - current_size) > 0.1 * requested_size:
        slack_message(":exclamation: cluster {} is still stabilizing, {} of {} instances created".format(name, current_size, requested_size))
        stable = False

    if errors:
        error_messages = "\n".join(errors)
        slack_message(f":exclamation: Cluster error: {error_messages}")

    return stable, requested_size


def collect_resource_metrics(start_time, end_time):
    import pendulum
    from google.cloud import monitoring_v3

    project_id = get_project_id()
    cluster_info = json.loads(BaseHook.get_connection("InstanceGroups").extra)

    resources = {}

    for k in cluster_info:
        resources |= {ig['name'] : {} for ig in cluster_info[k]}

    alignment_period = 60

    client = monitoring_v3.MetricServiceClient()
    project_name = f"projects/{project_id}"

    interval = monitoring_v3.TimeInterval(
        {
            "end_time": {"seconds": int(end_time.timestamp()), "nanos": 0},
            "start_time": {"seconds": int(start_time.timestamp()), "nanos": 0},
        }
    )
    aggregation_sum = monitoring_v3.Aggregation(
        {
            # Use SUM for DELTA metrics
            "alignment_period": {"seconds": alignment_period},
            "per_series_aligner": monitoring_v3.Aggregation.Aligner.ALIGN_SUM,
            "cross_series_reducer": monitoring_v3.Aggregation.Reducer.REDUCE_SUM,
            "group_by_fields": ["metadata.user_labels.vmrole", "metadata.user_labels.location", "metadata.system_labels.instance_group"],
        }
    )

    aggregation_sum_gcs = monitoring_v3.Aggregation(
        {
            # Use SUM for DELTA metrics
            "alignment_period": {"seconds": alignment_period},
            "per_series_aligner": monitoring_v3.Aggregation.Aligner.ALIGN_SUM,
            "cross_series_reducer": monitoring_v3.Aggregation.Reducer.REDUCE_SUM,
            "group_by_fields": ["metric.label.method", "resource.label.bucket_name"],
        }
    )

    aggregation_mean = monitoring_v3.Aggregation(
        {
            # SUM over series so we can average by uptime
            "alignment_period": {"seconds": alignment_period},
            "per_series_aligner": monitoring_v3.Aggregation.Aligner.ALIGN_MEAN,
            "cross_series_reducer": monitoring_v3.Aggregation.Reducer.REDUCE_SUM,
            "group_by_fields": ["metadata.user_labels.vmrole", "metadata.user_labels.location", "metadata.system_labels.instance_group"],
        }
    )

    def query_metric(metric, aggregation):
        try:
            return client.list_time_series(
               request={
                   "name": project_name,
                   "filter": f'metric.type = "{metric}"',
                   "interval": interval,
                   "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
                   "aggregation": aggregation,
               }
            )
        except:
            print(f"Cannot fetch metric {metric}")
            return []

    for result in query_metric("compute.googleapis.com/instance/uptime", aggregation_sum):
        group_name = result.metadata.system_labels.fields['instance_group'].string_value
        if group_name in resources:
            resources[group_name]["uptime"] = pendulum.duration(seconds=sum(p.value.double_value for p in result.points))

    for result in query_metric("compute.googleapis.com/instance/cpu/usage_time", aggregation_sum):
        group_name = result.metadata.system_labels.fields['instance_group'].string_value
        if group_name in resources and "uptime" in resources[group_name]:
            resources[group_name]["cputime"] = pendulum.duration(seconds=sum(p.value.double_value for p in result.points))
            resources[group_name]["cpu_utilization"] = resources[group_name]["cputime"].total_seconds()/resources[group_name]["uptime"].total_seconds()*100

    for result in query_metric("compute.googleapis.com/instance/network/received_bytes_count", aggregation_sum):
        group_name = result.metadata.system_labels.fields['instance_group'].string_value
        if group_name in resources and "uptime" in resources[group_name]:
            resources[group_name]["received_bytes"] = sum(p.value.int64_value for p in result.points)

    for result in query_metric("compute.googleapis.com/instance/network/sent_bytes_count", aggregation_sum):
        group_name = result.metadata.system_labels.fields['instance_group'].string_value
        if group_name in resources and "uptime" in resources[group_name]:
            resources[group_name]["sent_bytes"] = sum(p.value.int64_value for p in result.points)

    for result in query_metric("custom.googleapis.com/instance/gpu/utilization", aggregation_mean):
        group_name = result.metadata.system_labels.fields['instance_group'].string_value
        if group_name in resources and "uptime" in resources[group_name]:
            resources[group_name]["gputime"] = pendulum.duration(seconds=sum(p.value.double_value*alignment_period/100 for p in result.points))
            resources[group_name]["gpu_utilization"] = resources[group_name]["gputime"].total_seconds()/resources[group_name]["uptime"].total_seconds()*100

    buckets = Variable.get("gcs_buckets", deserialize_json=True, default_var=[])
    resources["GCS"] = {}

    for result in query_metric("storage.googleapis.com/api/request_count", aggregation_sum_gcs):
        bucket = result.resource.labels['bucket_name']
        if bucket in buckets:
            if bucket not in resources["GCS"]:
                resources["GCS"][bucket] = {}

            resources["GCS"][bucket][result.metric.labels['method']] = resources["GCS"][bucket].get(result.metric.labels['method'], 0) + sum(p.value.int64_value for p in result.points)


    return resources


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


def toggle_easyseg_worker(on=False):
    from dag_utils import get_connection
    ig_conn = get_connection("EasysegWorker")
    if not ig_conn:
        return
    deployment = ig_conn.host
    zone = ig_conn.login
    easyseg_worker = f"{deployment}-easyseg-worker"
    if on:
        start_instance(easyseg_worker, zone)
    else:
        stop_instance(easyseg_worker, zone)


def toggle_nfs_server(on=False):
    from dag_utils import get_connection
    nfs_conn = get_connection("NFSServer")
    if not nfs_conn:
        return
    deployment = nfs_conn.host
    zone = nfs_conn.login
    nfs_server = f"{deployment}-nfs-server"
    if on:
        start_instance(nfs_server, zone)
        wait_for_instance(nfs_server, zone, "RUNNING")
    else:
        stop_instance(nfs_server, zone)
        wait_for_instance(nfs_server, zone, "TERMINATED")

    slack_message(f'*Turning {"on" if on else "off"} the nfs server*')


def wait_for_instance(instance, zone, target, retries=5):
    for _ in range(retries):
        status = get_instance_property(zone, instance, "status")
        if status == target:
            return
        sleep(60)

    slack_message(f'*Timeout while waiting {instance}*')
    raise RuntimeError(f"Timeout waiting {instance}")
