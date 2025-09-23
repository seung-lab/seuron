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

SCHEDULE_INTERVAL = '2-59/7 * * * *'

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

def remove_failed_instances():
    import os
    import json
    import redis
    import humanize
    from time import sleep
    from datetime import datetime, timezone
    from airflow.models import Variable
    from airflow.hooks.base_hook import BaseHook
    from common.redis_utils import get_hostname_failures

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
    r = redis.Redis(redis_host, decode_responses=True)

    FAILURE_THRESHOLD = 10

    for key in cluster_info:
        delta_threshold = 300
        delta2_threshold = 600
        if "deepem" in key:
            delta_threshold *= 2
            delta2_threshold *= 2

        if target_sizes.get(key, 0) == 0:
            continue

        hostname_failures = get_hostname_failures(key)
        failed_hostnames = {host for host, count in hostname_failures.items() if count >= FAILURE_THRESHOLD} if hostname_failures else set()

        cluster_alive = False

        for ig in cluster_info[key]:
            instances = cluster_api.list_managed_instances(ig)
            if not instances:
                continue

            failed_instances = []
            hostnames_to_delete = []
            dead_instances_msg = ["The follow instances are deleted due to heartbeat timeout:"]
            failed_instances_msg = [f"The following instances from queue {key} in IG are deleted due to repeated failures:"]


            for instance_url in instances:
                instance = instance_url.split("/")[-1]
                hostname = instance.split('.')[0]

                # Check for failed instances
                if hostname in failed_hostnames:
                    failed_instances.append(instance_url)
                    hostnames_to_delete.append(hostname)
                    failed_instances_msg.append(f"{hostname} (failed {hostname_failures[hostname]} times)")
                    continue

                # Check for dead instances
                ts = r.get(instance)
                if not ts:
                    r.set(instance, timestamp)
                else:
                    delta = timestamp - float(ts)
                    if delta > delta_threshold:
                        try:
                            creationTimestamp = datetime.fromisoformat(cluster_api.get_instance_property(ig["zone"], instance, "creationTimestamp"))
                            delta2 = timestamp - creationTimestamp.timestamp()
                        except:
                            continue
                        if delta2 > delta2_threshold:
                            dead_instances_msg.append(f"{instance} created {humanize.naturaltime(creationTimestamp.astimezone(timezone.utc), when=datetime.now(timezone.utc))} has no heartbeat for {humanize.naturaldelta(delta)}")
                            failed_instances.append(instance_url)
                            hostnames_to_delete.append(hostname)

            if len(instances) > len(failed_instances) or "deepem" in key:
                cluster_alive = True

            if failed_instances:
                cluster_api.delete_instances(ig, failed_instances)

                if len(dead_instances_msg) > 1:
                    slack_message("\n".join(dead_instances_msg), notification=True)
                if len(failed_instances_msg) > 1:
                    slack_message("\n".join(failed_instances_msg), notification=True)

                if hostnames_to_delete:
                    redis_key = f"{key}_hostname_failures"
                    r.hdel(redis_key, *list(set(hostnames_to_delete)))

                if "deepem" in key:
                    sleep(60)
                    slack_message(f"repopulate {key} instance group to {len(instances)}", notification=True)
                    cluster_api.resize_instance_group(ig, len(instances))
                    sleep(60)

        sleep(60)
        if not cluster_alive:
            slack_message(f"All instances in {key} are dead, add one instance back", notification=True)
            cluster_api.ramp_up_cluster(key, 1, 1)


def shutdown_easyseg_worker():
    import os
    import redis
    import humanize
    from datetime import datetime
    from airflow.models import Variable
    from dag_utils import get_connection
    if Variable.get("vendor") == "Google":
        import google_api_helper as cluster_api
    else:
        cluster_api = None

    if cluster_api is None:
        return

    redis_host = os.environ['REDIS_SERVER']
    timestamp = datetime.now().timestamp()
    r = redis.Redis(redis_host)

    ig_conn = get_connection("EasysegWorker")

    if not ig_conn:
        return

    deployment = ig_conn.host
    zone = ig_conn.login
    instance = f"{deployment}-easyseg-worker"
    status = cluster_api.get_instance_property(zone, instance, "status")
    if status != "RUNNING":
        return

    ts = r.get(instance)
    if not ts:
        r.set(instance, timestamp)
    else:
        delta = timestamp - float(ts)
        if delta > 300:
            slack_message(f"Shutdown easyseg worker idling for {humanize.naturaldelta(delta)}", notification=True)
            try:
                cluster_api.toggle_easyseg_worker(on=False)
            except Exception:
                pass


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

remove_failed_instances_task = PythonOperator(
    task_id="remove_failed_instances",
    python_callable=remove_failed_instances,
    priority_weight=1000,
    queue="cluster",
    dag=dag)

shutdown_easyseg_worker_task = PythonOperator(
    task_id="shutdown_easyseg_worker",
    python_callable=shutdown_easyseg_worker,
    priority_weight=1000,
    queue="cluster",
    dag=dag)

latest >> queue_sizes_task >> remove_failed_instances_task >> shutdown_easyseg_worker_task
