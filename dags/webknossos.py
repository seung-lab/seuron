"""Webknossos interface."""
from __future__ import annotations

import json
from datetime import datetime
import requests

from airflow import DAG
from airflow.utils.weight_rule import WeightRule
from airflow.operators.python import PythonOperator
from airflow.models import Variable, BaseOperator as Operator

from worker_op import worker_op

from slack_message import slack_message, task_failure_alert, task_done_alert


PARAM = Variable.get("webknossos_param", {}, deserialize_json=True)
WKT_IMAGE = PARAM.get("wkt_image", "zettaai/wktools")


def wk_rest_api(wk_url, endpoint, headers):
    try:
        ret = requests.get(f"{wk_url}/api/{endpoint}", headers=headers)
        return ret
    except requests.exceptions.Timeout:
        slack_message(f"Timeout error. The request to {wk_url} took too long to complete.")
        raise RuntimeError("webknossos setup error")
    except requests.exceptions.RequestException as e:
        slack_message(f"Request error: {e}")
        raise RuntimeError("webknossos setup error")


def validate_wk_param():
    wk_url = PARAM["wk_url"].rstrip('/')
    auth_token = PARAM["auth_token"]
    headers = {"X-Auth-Token": auth_token}

    healthinfo = wk_rest_api(wk_url, "health", headers)
    if healthinfo.status_code != 200 or healthinfo.text != 'Ok':
        slack_message(f"*WARNING*: Webknossos server {wk_url} health check failed")
        raise RuntimeError("Webknossos health check error")

    organization = wk_rest_api(wk_url, "organizations/default", headers)
    if organization.status_code != 200:
        slack_message(f"*Error*: Cannot find default organization: {organization.text}")
        raise RuntimeError("Organization error")
    else:
        slack_message(f"Default organization: ```{json.dumps(organization.json(), indent=2)}```")

    organization_name = organization.json()["name"]
    wk_dset_name = PARAM["wk_dset_name"]

    dataset = wk_rest_api(wk_url, f"datasets/{organization_name}/{wk_dset_name}", headers)
    if dataset.status_code != 200:
        slack_message(f"*Error*: Cannot find dataset: {dataset.text}")
        raise RuntimeError("Dataset error")
    else:
        slack_message(f"Dataset details: ```{json.dumps(dataset.json(), indent=2)}```")

    task_type_id = PARAM["task_type_id"]
    task_type_details = wk_rest_api(wk_url, f"taskTypes/{task_type_id}", headers)
    if task_type_details.status_code != 200:
        slack_message(f"*Error*: Task type error: {task_type_details.text}")
        raise RuntimeError("Task type error")
    else:
        slack_message(f"Selected task type: ```{json.dumps(task_type_details.json(), indent=2)}```")

    project_name = PARAM["project_name"]
    project_details = wk_rest_api(wk_url, f"projects/byName/{project_name}", headers)
    if project_details.status_code != 200:
        slack_message(f"*WARNING*: Project name error: {project_details.text}")
    else:
        slack_message(f"Selected project: ```{json.dumps(project_details.json(), indent=2)}```")


# Op functions
def manager_op(
    dag: DAG, task_name: str, command: str = "", queue: str = "manager"
) -> Operator:
    """An operator fn for running webknossos tasks on the airflow node."""
    return worker_op(
        variables={},
        task_id=task_name,
        command=command,
        do_xcom_push=True,
        xcom_all=True,
        force_pull=True,
        on_failure_callback=task_failure_alert,
        on_success_callback=task_done_alert,
        image=WKT_IMAGE,
        priority_weight=100_000,
        weight_rule=WeightRule.ABSOLUTE,
        queue=queue,
        dag=dag,
    )


def sanity_check_op(dag: DAG, script: str) -> Operator:
    command = make_command("sanity_check", script=script)

    return manager_op(dag, f"{script}_sanity_check", command)


def cutout_op(dag: DAG) -> Operator:
    command = make_command("cv2wk")

    return manager_op(dag, "make_cutout", command)


def report_cutout(**kwargs):
    ti = kwargs['ti']
    output = ti.xcom_pull(task_ids="make_cutout")

    for line in output:
        if line.startswith("INFO:__main__:{\"tasks\":"):
            taskinfo = json.loads(line.replace("INFO:__main__:", "").strip())

            taskid = taskinfo["tasks"][0]["success"]["id"]
            slack_message(f"Created task ID: `{taskid}`")


def export_op(dag: DAG) -> Operator:
    command = make_command("wk2cv")

    return manager_op(dag, "export_labels", command)


def report_export(**kwargs):
    ti = kwargs['ti']
    output = ti.xcom_pull(task_ids="export_labels")

    output_to_send = ""
    for line in output:
        if line.startswith("INFO:__main__:Exporting"):
            output_to_send += f"{line[14:]}\n"
        if line.startswith("WARNING:"):
            output_to_send += f"`{line}`\n"

    slack_message(output_to_send)


# Helper fns
def make_command(scriptname: str, **args):
    """Makes a command-line argument string for a given wktools function.

    Dumps all parameters to the command-line (wktools ignores "unknown" arguments).
    And overrides parameters using the supplied arguments.
    """
    param = PARAM.copy()
    param.update(args)

    return f"{scriptname} {make_argstr(param)}"


def make_argstr(param: dict) -> str:

    def format_arg(item) -> str:
        k, v = item
        if v is None:
            return f"--{k}"
        elif isinstance(v, list):
            return f"--{k} " + " ".join(f"{elem}" for elem in v)
        elif isinstance(v, dict):
            return (
                f"--{k}"
                + " '{"
                + ", ".join(f"\"{vk}\":\"{vv}\"" for vk, vv in v.items())
                + "}'"
            )
        else:
            return f"--{k} {v}"

    return " ".join(map(format_arg, param.items()))


default_args = dict(
    owner="seuronbot",
    depends_on_path=False,
    start_date=datetime(2022, 1, 23),
    catchup=False,
    retries=0,
)


# Isolated sanity check (update parameters)
sanity_check_dag = DAG(
    "wkt_sanity_check",
    default_args=default_args,
    schedule_interval=None,
    tags=["webknossos"],
)

validate_wk_param_op = PythonOperator(
    task_id="validate_webknossos_parameters",
    python_callable=validate_wk_param,
    priority_weight=100000,
    on_failure_callback=task_failure_alert,
    weight_rule=WeightRule.ABSOLUTE,
    queue="manager",
    dag=sanity_check_dag,
)

# Individual ops test whether the current params are enough for
# the webknossos tools tasks
validate_wk_param_op >> sanity_check_op(sanity_check_dag, "cv2wk")
validate_wk_param_op >> sanity_check_op(sanity_check_dag, "wk2cv")


# Cutouts
cutout_dag = DAG(
    "wkt_cutouts",
    default_args=default_args,
    schedule_interval=None,
    tags=["webknossos", "training"],
)

cutout = cutout_op(cutout_dag)
report_cutout_task = PythonOperator(
    task_id="report_cutout",
    provide_context=True,
    python_callable=report_cutout,
    priority_weight=100000,
    on_failure_callback=task_failure_alert,
    weight_rule=WeightRule.ABSOLUTE,
    queue="manager",
    dag=cutout_dag,
)

cutout >> report_cutout_task


# Independent export to Zettaset (see training.py for coupled training)
export_dag = DAG(
    "wkt_export",
    default_args=default_args,
    schedule_interval=None,
    tags=["webknossos", "training"],
)

export = export_op(export_dag)
report_export_task = PythonOperator(
    task_id="report_export",
    provide_context=True,
    python_callable=report_export,
    priority_weight=100000,
    on_failure_callback=task_failure_alert,
    weight_rule=WeightRule.ABSOLUTE,
    queue="manager",
    dag=export_dag,
)

export >> report_export_task
