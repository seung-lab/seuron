"""
This dag autoscales your cluster. This only works with docker-compose (local)
and Infrakit (swarm).

For Infrakit, the following environment variables must be set:
    - INFRAKIT_IMAGE - what docker image to use for infrakit
    i.e.infrakit/devbundle:latest
    - INFRAKIT_GROUPS_URL - the location of the groups json file that defines
    the groups definition,
    i.e. https://github.com/wongwill86/examples/blob/master/latest/swarm/groups.json
""" # noqa
from airflow import DAG
from datetime import datetime, timedelta
from airflow.utils.weight_rule import WeightRule
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python import PythonOperator
from airflow.operators.latest_only import LatestOnlyOperator

from slack_message import slack_message
import google_api_helper as gapi
import json

DAG_ID = 'cluster_management'

default_args = {
    'owner': 'seuronbot',
    'depends_on_past': False,
    'start_date': datetime(2019, 2, 8),
    'catchup': False,
    'retry_delay': timedelta(seconds=10),
    'retries': 3
}

SCHEDULE_INTERVAL = '*/20 * * * *'

dag = DAG(
    dag_id=DAG_ID,
    schedule_interval=SCHEDULE_INTERVAL,
    default_args=default_args,
    catchup=False,
    tags=['maintenance'],
)

def check_queue(queue):
    import requests
    ret = requests.get("http://rabbitmq:15672/api/queues/%2f/{}".format(queue), auth=('guest', 'guest'))
    if not ret.ok:
        slack_message(f"Cannot get info for queue {queue}, assume 0 tasks", notification=True)
        return 0
    queue_status = ret.json()
    return queue_status["messages"]


def get_num_task(cluster):
    from dag_utils import get_composite_worker_limits

    if cluster == "composite":
        min_layer, max_layer = get_composite_worker_limits()
        tasks = [check_queue(f"{cluster}_{layer}") for layer in range(min_layer, max_layer+1)]
        num_tasks = sum(tasks)
    else:
        num_tasks = check_queue(cluster)
    return num_tasks


def cluster_control():
    from dag_utils import get_composite_worker_limits

    try:
        project_id = gapi.get_project_id()
        cluster_info = json.loads(BaseHook.get_connection("InstanceGroups").extra)
        target_sizes = Variable.get("cluster_target_size", deserialize_json=True)
    except:
        slack_message(":exclamation:Failed to load the cluster information from connection {}".format("InstanceGroups"), notification=True)
        return

    for key in cluster_info:
        if key not in target_sizes:
            continue

        print(f"processing cluster: {key}")
        try:
            num_tasks = get_num_task(key)
            current_size = gapi.get_cluster_size(project_id, cluster_info[key])
            requested_size = gapi.get_cluster_target_size(project_id, cluster_info[key])
        except:
            slack_message(":exclamation:Failed to get the {} cluster information from google.".format(key), notification=True)
            continue

        if num_tasks < target_sizes[key]:
            target_sizes[key] = max(num_tasks, 1)

        if target_sizes[key] == 0:
            if requested_size != 0:
                gapi.resize_instance_group(project_id, cluster_info[key], 0)
            continue

        if num_tasks < current_size:
            continue

        if requested_size == 0:
            if num_tasks != 0:
                gapi.resize_instance_group(project_id, cluster_info[key], 1)
            continue

        if (requested_size - current_size) > 0.1 * requested_size:
            slack_message(":exclamation: cluster {} is still stabilizing, {} of {} instances created".format(key, current_size, requested_size))
            continue

        if requested_size < target_sizes[key]:
            max_size = sum(ig['max_size'] for ig in cluster_info[key])
            updated_size = min([target_sizes[key], requested_size*2, max_size])
            if requested_size != updated_size:
                gapi.resize_instance_group(project_id, cluster_info[key], updated_size)
                slack_message(":arrow_up: ramping up cluster {} from {} to {} instances".format(key, requested_size, updated_size))
        else:
            slack_message(":information_source: status of cluster {}: {} out of {} instances up and running".format(key, current_size, requested_size), notification=True)

    Variable.set("cluster_target_size", target_sizes, serialize_json=True)

latest = LatestOnlyOperator(
    task_id='latest_only',
    priority_weight=100000,
    weight_rule=WeightRule.ABSOLUTE,
    queue='cluster',
    dag=dag)

queue_sizes_task = PythonOperator(
    task_id="check_cluster_status",
    python_callable=cluster_control,
    priority_weight=100000,
    weight_rule=WeightRule.ABSOLUTE,
    queue="cluster",
    dag=dag)

latest.set_downstream(queue_sizes_task)

