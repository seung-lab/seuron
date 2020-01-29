import os
import json
import subprocess
from collections import OrderedDict
from bot_info import workerid

fernet_key = os.environ["FERNET_KEY"]
#airflow_cmd = "sudo docker run -e FERNET_KEY={fernet_key} \
#    -it --rm {docker_image} airflow".format(
#        fernet_key=fernet_key,
#        docker_image="ranlu/air-tasks:ranl_minnie_test"
#    )
airflow_cmd = "airflow"


def latest_task():
    state, exec_date = dag_state("sanity_check")


def update_slack_connection(payload, token):
    conn_id = "Slack"
    output = subprocess.check_output(
        "{airflow_cmd} connections -d --conn_id {conn_id}".format(
            airflow_cmd=airflow_cmd,
            conn_id=conn_id),
        shell=True)
    print(output.decode("ascii", "ignore"))

    output = subprocess.check_output(
        "{airflow_cmd} connections -a --conn_id {conn_id} \
        --conn_type http --conn_login {workerid} \
        --conn_password {token} \
        --conn_extra '{extra}'".format(
            airflow_cmd=airflow_cmd,
            conn_id=conn_id,
            workerid=workerid,
            token=token,
            extra=json.dumps(payload)),
        shell=True)


def update_user_info(userid):
    output = subprocess.check_output(
        "{airflow_cmd} variables -s {key} '{value}'". format(
            airflow_cmd=airflow_cmd,
            key="author",
            value=userid),
        shell=True)
    print(output.decode("ascii", "ignore"))


def check_running():
    state, exec_date = dag_state("sanity_check")
    if state == "running":
        return True
    state, exec_date = dag_state("segmentation")
    if state == "running":
        return True
    return False


def sanity_check():
    dag_id = "sanity_check"
    state, exec_date = dag_state(dag_id)
    if state == "success" or state == "unknown":
        output = subprocess.check_output(
            "{airflow_cmd} trigger_dag {dag_id}".format(
                airflow_cmd=airflow_cmd,
                dag_id=dag_id),
            shell=True)
        print(output.decode("ascii", "ignore"))
        return True
    elif state == "failed":
        print(exec_date)
        output = subprocess.check_output(
            "{airflow_cmd} clear -c -s {exec_date} {dag_id}".format(
                airflow_cmd=airflow_cmd,
                exec_date=exec_date,
                dag_id=dag_id),
            shell=True)
        print(output.decode("ascii", "ignore"))
        return True
    else:
        print("do not understand {} state".format(state))
        return False


def run_segmentation():
    dag_id = "segmentation"

    if check_running():
        return False

    output = subprocess.check_output(
        "{airflow_cmd} trigger_dag {dag_id}".format(
            airflow_cmd=airflow_cmd,
            dag_id=dag_id),
        shell=True)
    print(output.decode("ascii", "ignore"))
    return True


def get_param():
    output = subprocess.check_output(
        "{airflow_cmd} variables -g {key}".format(
            airflow_cmd=airflow_cmd,
            key="param"),
        shell=True)
    data = output.decode("ascii", "ignore")
    json_starts = data.rfind("{")
    json_string = data[json_starts:]
    try:
        param = json.loads(json_string, object_pairs_hook=OrderedDict)
        return param
    except ValueError:
        return []


def set_variable(key, value):
    output = subprocess.check_output(
        "{airflow_cmd} variables -s {key} '{value}'". format(
            airflow_cmd=airflow_cmd,
            key=key,
            value=value),
        shell=True)
    print(output.decode("ascii", "ignore"))


def set_param(param):
    value = json.dumps(param)
    set_variable("param", value)
    return sanity_check()


def dag_state(dag_id):
    output = subprocess.check_output(
        "{airflow_cmd} list_dag_runs {dag_id}".format(
            airflow_cmd=airflow_cmd,
            dag_id=dag_id),
        shell=True)

    real_output = False
    for l in output.decode("ascii", "ignore").split("\n"):
        cols = l.split("|")
        if len(cols) == 6 and cols[0].strip() == 'id':
            real_output = True
            continue

        if real_output and len(cols) == 6:
            print(l)
            state = cols[2].strip()
            exec_date = cols[3].strip()

            return state, exec_date

    return "unknown", "unknown"
