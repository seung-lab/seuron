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
from airflow.models import DagRun
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.operators.latest_only import LatestOnlyOperator
from airflow.utils.db import provide_session
from airflow.utils.state import State
from airflow import models

from slack_message import slack_message

DAG_ID = 'pipeline_heartbeat'

default_args = {
    'owner': 'seuronbot',
    'depends_on_past': False,
    'start_date': datetime(2019, 2, 8),
    'catchup': False,
    'retries': 0,
}

SCHEDULE_INTERVAL = '2-59/5 * * * *'

dag = DAG(
    dag_id=DAG_ID,
    schedule_interval=SCHEDULE_INTERVAL,
    default_args=default_args,
    catchup=False,
    tags=['maintenance'],
)

# To use infrakit with > 1 queue, we will have to modify this code to use
# separate groups file for each queue!
@provide_session
def get_num_task_instances(session):
    query = (session
        .query(DagRun)
        .filter(DagRun.dag_id.in_(("watershed", "agglomeration", "chunkflow_worker")))
        .filter(DagRun.state == State.RUNNING))
    if query.count() == 0:
        return

    TI = models.TaskInstance
    running = session.query(TI).filter(
        TI.state == State.RUNNING
    ).count()

    queued = session.query(TI).filter(
        TI.state == State.QUEUED
    ).count()

    up_for_retry = session.query(TI).filter(
        TI.state == State.UP_FOR_RETRY
    ).count()

    if running > 2: #ws or agg running
        running -= 2
    elif running == 2: #segmentation?
        running = 1

    message = '''*Pipeline heartbeat:*
*{}* tasks running, *{}* tasks queued, *{}* tasks up for retry'''.format(running, queued, up_for_retry)
    slack_message(message, notification=True)

def delete_dead_instances():
    import os
    import json
    import redis
    import humanize
    from datetime import datetime
    from airflow.models import Variable
    from airflow.hooks.base_hook import BaseHook
    if Variable.get("vendor") == "Google":
        import google_api_helper as cluster_api
    else:
        cluster_api = None

    if cluster_api is None:
        return

    try:
        cluster_info = json.loads(BaseHook.get_connection("InstanceGroups").extra)
        target_sizes = Variable.get("cluster_target_size", deserialize_json=True)
    except:
        slack_message(":exclamation:Failed to load the cluster information from connection InstanceGroups", notification=True)
        return

    redis_host = os.environ['REDIS_SERVER']
    timestamp = datetime.now().timestamp()
    r = redis.Redis(redis_host)

    for key in cluster_info:
        if target_sizes.get(key, 0) == 0:
            continue

        stable, _ = cluster_api.cluster_status(key, cluster_info[key])

        if not stable:
            slack_message(f"Cluster {key} is still stablizing, skip heartbeat check", notification=True)
            continue

        for ig in cluster_info[key]:
            instances = cluster_api.list_managed_instances(ig)
            if not instances:
                continue

            idle_instances = []
            dead_instances = []
            msg = ["The follow instances are deleted due to heartbeat timeout:"]
            for instance_url in instances:
                instance = instance_url.split("/")[-1]
                ts = r.get(instance)
                if not ts:
                    r.set(instance, timestamp)
                else:
                    delta = timestamp - float(ts)
                    if delta > 3600:
                        msg.append(f"{instance} has no heartbeat for {humanize.naturaldelta(delta)}")
                        dead_instances.append(instance_url)
                    elif delta > 300:
                        msg.append(f"{instance} has no heartbeat for {humanize.naturaldelta(delta)}")
                        idle_instances.append(instance_url)

            if (len(instances) == (len(idle_instances) + len(dead_instances))) and len(idle_instances) > 0:
                idle_instances = idle_instances[:-1]

            if idle_instances:
                cluster_api.delete_instances(ig, idle_instances)
                slack_message("\n".join(msg), notification=True)


latest = LatestOnlyOperator(
    task_id='latest_only',
    priority_weight=1000,
    queue='cluster',
    dag=dag)

queue_sizes_task = PythonOperator(
    task_id="check_task_status",
    python_callable=get_num_task_instances,
    priority_weight=1000,
    queue="cluster",
    dag=dag)

delete_dead_instances_task = PythonOperator(
    task_id="delete_dead_instances",
    python_callable=delete_dead_instances,
    priority_weight=1000,
    queue="cluster",
    dag=dag)

latest >> queue_sizes_task >> delete_dead_instances_task
