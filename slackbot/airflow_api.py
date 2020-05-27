import json
from datetime import datetime
from bot_info import workerid

from airflow import settings
from airflow.models import (DagBag, DagRun, Variable, Connection, DAG)
from airflow.utils.state import State

from sqlalchemy.orm import exc


def latest_task():
    state, exec_date = dag_state("sanity_check")


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
    dags = ['sanity_check', 'segmentation','watershed','agglomeration', 'chunkflow_worker', 'chunkflow_generator']

    for d in dags:
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


def chunkflow_set_env():
    dag_id = "chunkflow_generator"
    trigger_or_clear_dag(dag_id)


def run_dag(dag_id):
    print("run dag {}".format(dag_id))
    run_id='trig__' + datetime.utcnow().isoformat()
    dbag = DagBag()
    trigger_dag = dbag.get_dag(dag_id)
    session = settings.Session()

    dr = trigger_dag.create_dagrun(
        run_id=run_id,
        state=State.RUNNING,
        conf=None,
        external_trigger=True)
    print("Creating DagRun {}".format(dr))
    session.add(dr)
    session.commit()
    session.close()


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


def get_variable(key, deserialize_json=False):
    return Variable.get('param', deserialize_json=deserialize_json)


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
        return latest_run.state, latest_run.execution_date.isoformat()
