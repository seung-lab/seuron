import json
from bot_info import workerid, slack_notification_channel

import pendulum

from airflow import settings
from airflow.models import (DagBag, DagRun, Variable, Connection, DAG)
from airflow.models.dagrun import DagRun, DagRunType
from airflow.api.common.mark_tasks import set_dag_run_state_to_success
from airflow.api.common.trigger_dag import trigger_dag
from airflow.utils.state import State
from airflow.utils import timezone

from sqlalchemy.orm import exc

seuron_dags = ['sanity_check', 'segmentation','watershed','agglomeration', 'chunkflow_worker', 'chunkflow_generator', 'contact_surface', "igneous", "custom-cpu", "custom-gpu", "synaptor_sanity_check", "synaptor_file_seg", "synaptor_db_seg", "synaptor_assignment"]


def latest_task():
    state, exec_date = dag_state("sanity_check")


def mark_dags_success():
    dagbag = DagBag()
    for d in seuron_dags:
        if d in dagbag.dags:
            state, exec_date = dag_state(d)
            if state == "running":
                dag = dagbag.dags[d]
                set_dag_run_state_to_success(dag=dag, execution_date=dag.latest_execution_date, commit=True)


def update_slack_connection(payload, token):
    conn_id = "Slack"
    session = settings.Session()
    print("Delete slack connection")
    try:
        to_delete = (session
                     .query(Connection)
                     .filter(Connection.conn_id == conn_id)
                     .one())
    except exc.NoResultFound:
        pass
    except exc.MultipleResultsFound:
        msg = ('\n\tFound more than one connection with ' +
               '`conn_id`={conn_id}\n')
        msg = msg.format(conn_id=conn_id)
        print(msg)
        return
    else:
        session.delete(to_delete)
        session.commit()

    print("Add slack connection")

    new_conn = Connection(conn_id=conn_id, conn_type='http', host='localhost', login=workerid, password=token)

    new_conn.set_extra(json.dumps({**payload, "notification_channel": slack_notification_channel}, indent=4))
    session.add(new_conn)
    session.commit()
    session.close()


def update_user_info(userid):
    set_variable('author', userid)


def check_running():
    """Checks whether the DAGs within the seuron_dags list (above) is running."""
    for d in seuron_dags:
        state, exec_date = dag_state(d)
        if state == "running":
            return True

    return False






def run_dag(dag_id):
    trigger_dag(dag_id)


def get_variable(key, deserialize_json=False):
    return Variable.get(key, deserialize_json=deserialize_json)


def set_variable(key, value, serialize_json=False):
    Variable.set(key, value, serialize_json=serialize_json)


def dag_state(dag_id):
    print("check dag states")
    dagbag = DagBag()
    if dag_id not in dagbag.dags:
        print("=========== dag_id does not exist ============")
        return "null", "null"

    runs = DagRun.find(dag_id=dag_id)
    for r in runs:
        print(r.id, r.run_id, r.state, r.dag_id)
    if len(runs) == 0:
        return "unknown", "unknown"
    else:
        latest_run = runs[-1]
        #start_date = ((latest_run.start_date or '') and latest_run.start_date.isoformat())
        return latest_run.state, latest_run.execution_date
