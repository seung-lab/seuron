import time
import json
import functools
import concurrent.futures
from datetime import datetime

from airflow import settings
from airflow.models import DagBag, DagModel, Variable, Connection
from airflow.models.dagrun import DagRun
from airflow.models.serialized_dag import SerializedDagModel
from airflow.api.common.mark_tasks import set_dag_run_state_to_success
from airflow.api.common.trigger_dag import trigger_dag
from airflow.utils.state import State, DagRunState
from airflow.utils.session import create_session
from airflow.utils.types import DagRunTriggeredByType

from sqlalchemy.orm import exc

from bot_info import workerid, slack_notification_channel

seuron_dags = ['sanity_check', 'segmentation','watershed','agglomeration', "postprocess", 'chunkflow_worker', 'chunkflow_generator', 'contact_surface', "igneous", "custom-cpu", "custom-gpu", "synaptor_sanity_check", "synaptor", "wkt_cutouts", "wkt_export", "training"]


def run_in_executor(f, /, *args, **kwargs):
    with concurrent.futures.ProcessPoolExecutor() as executor:
        return executor.submit(functools.partial(f, *args, **kwargs)).result()


def __mark_dags_success():
    runs = DagRun.find(state=DagRunState.RUNNING)

    for r in runs:
        d = r.dag_id
        if d in seuron_dags:
            # mark_tasks operates on the serialized DAG, so look it
            # up by dag_id instead of pulling the DAG from a DagBag.
            dag = SerializedDagModel.get_dag(d)
            if dag is None:
                print(f"no serialized DAG found for {d}, skipping")
                continue
            set_dag_run_state_to_success(dag=dag, run_id=r.run_id, commit=True)


def mark_dags_success():
    return run_in_executor(__mark_dags_success)


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
    return run_in_executor(__check_running)


def __run_dag(dag_id):
    return trigger_dag(dag_id, triggered_by=DagRunTriggeredByType.CLI)


def wait_for_dag_refresh(dag_id):
    last_parsed_time = None
    for _ in range(10):
        dag = DagModel.get_dagmodel(dag_id)
        if not last_parsed_time:
            last_parsed_time = dag.last_parsed_time
        else:
            if dag.last_parsed_time > last_parsed_time:
                print(dag.last_parsed_time)
                print(last_parsed_time)
                return
        time.sleep(5)


def run_dag(dag_id, wait_for_completion=False):
    dags_need_refresh = ["segmentation", "chunkflow_worker", "synaptor"]
    if dag_id in dags_need_refresh:
        wait_for_dag_refresh(dag_id)
    dagrun = run_in_executor(__run_dag, dag_id)
    if wait_for_completion:
        while True:
            time.sleep(5)
            dagrun.refresh_from_db()
            state = dagrun.state
            print(f"waiting for dag {dagrun.dag_id}, {dagrun.run_id} state : {state}")
            if state == DagRunState.SUCCESS or state == DagRunState.FAILED:
                ntasks = len(dagrun.get_task_instances(state=State.task_states))
                print(dagrun.start_date, dagrun.end_date, ntasks)
                if state == DagRunState.SUCCESS and ntasks == 0:
                    print("0 task in the dagrun, retrigger")
                    dagrun = run_in_executor(__run_dag, dag_id)
                else:
                    break
    return dagrun


def get_variable(key, deserialize_json=False, **kwargs):
    return Variable.get(key, deserialize_json=deserialize_json, **kwargs)


def set_variable(key, value, serialize_json=False):
    Variable.set(key, value, serialize_json=serialize_json)


def __latest_dagrun_state(dag_id):
    print("check dag states")
    dagbag = DagBag()
    if dag_id not in dagbag.dags:
        print("=========== dag_id does not exist ============")
        return "null"

    # DAG.get_latest_execution_date()/get_dagrun() are gone, so the
    # latest run is looked up from the database instead.
    runs = DagRun.find(dag_id=dag_id)
    if not runs:
        return "unknown"
    latest_run = max(runs, key=lambda r: r.logical_date or r.run_after or datetime.min)
    return latest_run.state

def latest_dagrun_state(dag_id):
    return run_in_executor(__latest_dagrun_state, dag_id)

def set_is_paused(dag_id, is_paused):
    # DagModel.set_is_paused() is gone; update the column directly.
    with create_session() as session:
        dag = DagModel.get_dagmodel(dag_id, session=session)

        if not dag:
            return False

        dag.is_paused = is_paused
        session.commit()

    return True
