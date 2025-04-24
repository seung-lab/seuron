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
    schedule=SCHEDULE_INTERVAL,
    default_args=default_args,
    catchup=False,
    tags=['maintenance'],
)

def check_queue(queue):
    import requests
    ret = requests.get("http://rabbitmq:15672/api/queues/%2f/{}".format(queue), auth=('guest', 'guest'))
    if not ret.ok:
        print(f"Cannot get info for queue {queue}, assume 0 tasks")
        return 0
    queue_status = ret.json()
    return queue_status["messages"]


def estimate_optimal_number_of_workers(cluster, cluster_info):
    from dag_utils import get_composite_worker_capacities, estimate_worker_instances

    if cluster == "composite":
        layers = get_composite_worker_capacities()
        tasks = [check_queue(f"{cluster}_{layer}") for layer in layers]
        num_workers = sum(tasks)
    else:
        if cluster == "gpu":
            num_tasks = check_queue(queue="chunkflow")
        elif cluster.startswith("synaptor"):
            num_tasks = check_queue(queue=f"{cluster}-tasks")
        else:
            num_tasks = check_queue(queue=cluster)
        num_workers = estimate_worker_instances(num_tasks, cluster_info)

    return num_workers


def cluster_control():
    if Variable.get("vendor") == "Google":
        import google_api_helper as cluster_api
    else:
        cluster_api = None

    if cluster_api is None:
        return

    run_metadata = Variable.get("run_metadata", deserialize_json=True, default_var={})
    if not run_metadata.get("manage_clusters", True):
        return

    try:
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
            num_workers = estimate_optimal_number_of_workers(key, cluster_info[key])
            stable, requested_size = cluster_api.cluster_status(key, cluster_info[key])
        except:
            slack_message(":exclamation:Failed to get the {} cluster information from google.".format(key), notification=True)
            continue

        if target_sizes[key] == 0:
            if requested_size != 0:
                cluster_api.resize_instance_group(cluster_info[key], 0)
            continue

        if num_workers != target_sizes[key] and key != "deepem-gpu":
            target_sizes[key] = max(num_workers, 1)

        if stable and requested_size < target_sizes[key]:
            max_size = sum(ig['max_size'] for ig in cluster_info[key])
            updated_size = min([target_sizes[key], requested_size*2, max_size])
            if requested_size != updated_size:
                cluster_api.resize_instance_group(cluster_info[key], updated_size)
                slack_message(":arrow_up: ramping up cluster {} from {} to {} instances".format(key, requested_size, updated_size))

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
