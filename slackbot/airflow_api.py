import json
from bot_info import workerid

import pendulum

from airflow import settings
from airflow.models import (DagBag, DagRun, Variable, Connection, DAG)
from airflow.models.dagrun import DagRun, DagRunType
from airflow.api.common.experimental.mark_tasks import set_dag_run_state_to_success
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
                set_dag_run_state_to_success(dag, dag.latest_execution_date, commit=True)


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

    new_conn.set_extra(json.dumps(payload, indent=4))
    session.add(new_conn)
    session.commit()
    session.close()


def update_user_info(userid):
    set_variable('author', userid)


def check_running():
    for d in seuron_dags:
        state, exec_date = dag_state(d)
        if state == "running":
            return True

    return False


def trigger_or_clear_dag(dag_id):
    state, exec_date = dag_state(dag_id)
    if state == "success" or state == "unknown":
        run_dag(dag_id)
        return True
    elif state == "failed":
        print(exec_date)
        dagbag = DagBag()
        dags = [dagbag.dags[dag_id]]
        DAG.clear_dags(dags, start_date=exec_date)
        return True
    else:
        print("do not understand {} state".format(state))
        return False


def sanity_check():
    dag_id = "sanity_check"
    trigger_or_clear_dag(dag_id)


def synaptor_sanity_check():
    """Runs the synaptor sanity check DAG."""
    trigger_or_clear_dag("synaptor_sanity_check")


def chunkflow_set_env():
    dag_id = "chunkflow_generator"
    trigger_or_clear_dag(dag_id)


def run_dag(dag_id):
    print("run dag {}".format(dag_id))
    execution_date = timezone.utcnow()
    run_id = 'trig__' + execution_date.isoformat()
    dbag = DagBag()
    dbag.sync_to_db()
    trigger_dag = dbag.get_dag(dag_id)
    session = settings.Session()

    dr = trigger_dag.create_dagrun(
        run_type=DagRunType.MANUAL,
        run_id=run_id,
        execution_date=execution_date,
        state=State.QUEUED,
        conf=None,
        data_interval=trigger_dag.timetable.infer_manual_data_interval(
            run_after=pendulum.instance(execution_date)
        ),
        dag_hash=dbag.dags_hash.get(dag_id),
        external_trigger=True)
    print("Creating DagRun {}".format(dr))
    session.add(dr)
    session.commit()
    session.close()


def run_igneous_tasks():
    dag_id = "igneous"

    if check_running():
        return False

    run_dag(dag_id)
    return True


def run_custom_tasks(task_type):
    dag_id = f"custom-{task_type}"

    if check_running():
        return False

    run_dag(dag_id)
    return True


def run_segmentation():
    dag_id = "segmentation"

    if check_running():
        return False

    run_dag(dag_id)
    return True


def run_inference():
    dag_id = "chunkflow_worker"

    if check_running():
        return False

    run_dag(dag_id)
    return True


def run_contact_surface():
    dag_id = "contact_surface"

    if check_running():
        return False

    run_dag(dag_id)
    return True


def run_synaptor_file_seg():
    """Runs the synaptor file segmentation DAG."""
    if check_running():
        return False

    run_dag("synaptor_file_seg")
    return True


def run_synaptor_db_seg():
    """Runs the synaptor database segmentation DAG."""
    if check_running():
        return False

    run_dag("synaptor_db_seg")
    return True


def run_synaptor_assignment():
    """Runs the synaptor assignment DAG."""
    if check_running():
        return False

    run_dag("synaptor_assignment")
    return True


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
