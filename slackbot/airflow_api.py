import json
from bot_info import workerid, slack_notification_channel

import pendulum
import functools
import concurrent.futures

from airflow import settings
from airflow.models import (DagBag, DagRun, Variable, Connection, DAG)
from airflow.models.dagrun import DagRun, DagRunType
from airflow.api.common.mark_tasks import set_dag_run_state_to_success
from airflow.api.common.trigger_dag import trigger_dag
from airflow.utils.state import State, DagRunState
from airflow.utils import timezone

from sqlalchemy.orm import exc

seuron_dags = ['sanity_check', 'segmentation','watershed','agglomeration', 'chunkflow_worker', 'chunkflow_generator', 'contact_surface', "igneous", "custom-cpu", "custom-gpu", "synaptor_sanity_check", "synaptor_file_seg", "synaptor_db_seg", "synaptor_assignment"]


def run_in_executor(f, /, *args, **kwargs):
    with concurrent.futures.ProcessPoolExecutor() as executor:
        future = executor.submit(functools.partial(f, *args, **kwargs))
        return future


def __mark_dags_success():
    dagbag = DagBag()
    runs = DagRun.find(state=DagRunState.RUNNING)

    for r in runs:
        d = r.dag_id
        if d in seuron_dags:
            dag = dagbag.dags[d]
            set_dag_run_state_to_success(dag=dag, execution_date=dag.get_latest_execution_date(), commit=True)


def mark_dags_success():
    return run_in_executor(__mark_dags_success).result()


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


def __check_running():
    """Checks whether the DAGs within the seuron_dags list (above) is running."""
    runs = DagRun.find(state=DagRunState.RUNNING)

    for r in runs:
        if r.dag_id in seuron_dags:
            return True

    return False


def check_running():
    return run_in_executor(__check_running).result()


def __run_dag(dag_id):
    dbag = DagBag()
    dbag.sync_to_db()
    dag_run = trigger_dag(dag_id)


def run_dag(dag_id):
    return run_in_executor(__run_dag, dag_id)


def get_variable(key, deserialize_json=False):
    return Variable.get(key, deserialize_json=deserialize_json)


def set_variable(key, value, serialize_json=False):
    Variable.set(key, value, serialize_json=serialize_json)


def __latest_dagrun_state(dag_id):
    print("check dag states")
    dagbag = DagBag()
    if dag_id not in dagbag.dags:
        print("=========== dag_id does not exist ============")
        return "null"

    d = dagbag.dags[dag_id]
    execution_date = d.get_latest_execution_date()
    if not execution_date:
        return "unknown"
    else:
        latest_run = d.get_dagrun(execution_date=execution_date)
        return latest_run.state

def latest_dagrun_state(dag_id):
    return run_in_executor(__latest_dagrun_state, dag_id).result()
