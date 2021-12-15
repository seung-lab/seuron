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
from airflow.operators.python_operator import PythonOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator

from slack_message import slack_message
import google_api_helper as gapi
import json

DAG_ID = 'cluster_management'

default_args = {
    'owner': 'airflow',
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
)

def check_queue(queue):
    import requests
    ret = requests.get("http://rabbitmq:15672/api/queues/%2f/{}".format(queue), auth=('guest', 'guest'))
    if not ret.ok:
        slack_message(f"Cannot get info for queue {queue}, assume 0 tasks", channel="#seuron-alerts")
        return 0
    queue_status = ret.json()
    return queue_status["messages"]

def cluster_control():
    try:
        project_id = gapi.get_project_id()
        cluster_info = json.loads(BaseHook.get_connection("InstanceGroups").extra)
        target_sizes = Variable.get("cluster_target_size", deserialize_json=True)
    except:
        slack_message(":exclamation:Failed to load the cluster information from connection {}".format("InstanceGroups"), channel="#seuron-alerts")
        return
    for key in cluster_info:
        if key in target_sizes:
            print(f"processing cluster: {key}")
            if target_sizes[key] != 0:
                try:
                    if key == "composite":
                        tasks = [check_queue(f"{key}_{i}") for i in range(5,11)]
                        num_tasks = sum(tasks)
                    else:
                        num_tasks = check_queue(key)
                    total_size = gapi.get_cluster_size(project_id, cluster_info[key])
                    total_target_size = gapi.get_cluster_target_size(project_id, cluster_info[key])
                except:
                    slack_message(":exclamation:Failed to get the {} cluster information from google.".format(key), channel="#seuron-alerts")
                    continue
                if num_tasks < total_size:
                    if 10 < num_tasks < total_size//10:
                        target_sizes[key] = total_size//10
                        gapi.resize_instance_group(project_id, cluster_info[key], target_sizes[key])
                    continue
                else:
                    if num_tasks < target_sizes[key]:
                        target_sizes[key] = num_tasks

                if (total_target_size - total_size) > 0.1 * total_target_size:
                    slack_message(":exclamation: cluster {} is still stabilizing, {} of {} instances created".format(key, total_size, total_target_size))
                    if (total_target_size > 0):
                        gapi.resize_instance_group(project_id, cluster_info[key], total_target_size)
                else:
                    if total_target_size < target_sizes[key] and total_target_size != 0:
                        max_size = 0
                        for ig in cluster_info[key]:
                            max_size += ig['max_size']
                        new_target_size = min([target_sizes[key], total_target_size*2, max_size])
                        if total_target_size != new_target_size:
                            gapi.resize_instance_group(project_id, cluster_info[key], new_target_size)
                            slack_message(":arrow_up: ramping up cluster {} from {} to {} instances".format(key, total_target_size, new_target_size))
                    else:
                        if (total_target_size != 0):
                            slack_message(":information_source: status of cluster {}: {} out of {} instances up and running".format(key, total_size, total_target_size), channel="#seuron-alerts")

            else:
                total_target_size = gapi.get_cluster_target_size(project_id, cluster_info[key])
                if total_target_size != 0:
                    gapi.resize_instance_group(project_id, cluster_info[key], 0)

    Variable.set("cluster_target_size", target_sizes, serialize_json=True)

latest = LatestOnlyOperator(
    task_id='latest_only',
    priority_weight=100000,
    weight_rule=WeightRule.ABSOLUTE,
    queue='manager',
    dag=dag)

queue_sizes_task = PythonOperator(
    task_id="check_cluster_status",
    python_callable=cluster_control,
    priority_weight=100000,
    weight_rule=WeightRule.ABSOLUTE,
    queue="manager",
    dag=dag)

latest.set_downstream(queue_sizes_task)

