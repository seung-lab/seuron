"""Webknossos interface."""
from __future__ import annotations

import re
import json
from datetime import datetime
from collections import OrderedDict

from airflow import DAG
from airflow.utils.weight_rule import WeightRule
from airflow.operators.python import PythonOperator
from airflow.models import Variable, BaseOperator as Operator

from worker_op import worker_op

from slack_message import slack_message, task_failure_alert, task_done_alert

from nglinks import Layer, ImageLayer, SegLayer, generate_link


PARAM = Variable.get("webknossos_param", {}, deserialize_json=True)
WKT_IMAGE = PARAM.get("wkt_image", "zettaai/wktools")


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


def list_layers(dag: DAG) -> Operator:
    command = make_command("listlayers")

    return manager_op(dag, "list_layers", command)


def easy_seg_nglink(**kwargs):
    from cloudvolume import CloudVolume

    [inf_params, seg_params] = Variable.get(
        "easy_seg_param", [None, None], deserialize_json=True
    )

    if seg_params is None:
        slack_message("No easy seg has been run yet")
        return

    layers = OrderedDict()
    layers["img"] = ImageLayer(inf_params["IMAGE_PATH"])

    if "SEG_PATH" in seg_params:  # abiss
        layers["network output"] = ImageLayer(seg_params["AFF_PATH"])
        layers["seg"] = SegLayer(seg_params["SEG_PATH"])
    elif "Volumes" in seg_params:  # synaptor
        layers["network output"] = ImageLayer(seg_params["Volumes"]["descriptor"])
        layers["seg"] = SegLayer(seg_params["Volumes"]["output"])
    else:
        slack_message("No output recognized in parameters")
        return

    try:
        CloudVolume(layers["seg"].cloudpath)
    except:
        # easy seg failed
        slack_message(f"failed to find layer: {layers['seg']}")
        return

    #ti = kwargs["ti"]
    #output = ti.xcom_pull(task_ids="list_layers")
    #try:
    #    layers = collect_label_layers(layers, output)
    #except Exception as e:
    #    slack_message(f"Exception parsing link output: {e}")
    #    return

    slack_message(generate_link(layers), broadcast=True)


def collect_label_layers(layers: dict[str, Layer], output: str):
    layer_link_regexp = re.compile(".*LAYER: (.*)::(.*)")
    for line in output:
        if "LAYER: " in line:
            name, cloudpath = layer_link_regexp.match(line).groups()
            layers[name] = SegLayer(cloudpath)

    return layers


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

# Individual ops test whether the current params are enough for
# the webknossos tools tasks
sanity_check_op(sanity_check_dag, "cv2wk")
sanity_check_op(sanity_check_dag, "wk2cv")


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


# Giving the user an easy seg link combining output with labels
easy_seg_link_dag = DAG(
    "easy_seg_link",
    default_args=default_args,
    schedule_interval=None,
    tags=["webknossos", "easy-seg"],
)

list_layers_task = list_layers(easy_seg_link_dag)
link_task = PythonOperator(
    task_id="generate_link",
    provide_context=True,
    python_callable=easy_seg_nglink,
    priority_weight=100000,
    weight_rule=WeightRule.ABSOLUTE,
    queue="manager",
    dag=easy_seg_link_dag,
)

list_layers_task >> link_task
