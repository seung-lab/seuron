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
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 2, 8),
    'catchup': False,
    'retries': 0,
}

SCHEDULE_INTERVAL = '*/20 * * * *'

dag = DAG(
    dag_id=DAG_ID,
    schedule_interval=SCHEDULE_INTERVAL,
    default_args=default_args,
    catchup=False,
)

# To use infrakit with > 1 queue, we will have to modify this code to use
# separate groups file for each queue!
@provide_session
def get_num_task_instances(session):
    query = (session
        .query(DagRun)
        .filter(DagRun.dag_id == "segmentation")
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
    slack_message(message, channel="#seuron-alerts")

latest = LatestOnlyOperator(
    task_id='latest_only',
    priority_weight=1000,
    queue='manager',
    dag=dag)

queue_sizes_task = PythonOperator(
    task_id="check_task_status",
    python_callable=get_num_task_instances,
    priority_weight=1000,
    queue="manager",
    dag=dag)

latest.set_downstream(queue_sizes_task)
