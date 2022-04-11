from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.weight_rule import WeightRule
from airflow.models import Variable, DagRun, DagBag, BaseOperator as Operator
from time import sleep
from slack_message import slack_message, task_failure_alert
from param_default import default_args
from google_api_helper import ramp_up_cluster, ramp_down_cluster, reset_cluster

def slack_message_op(dag, tid, msg):
    return PythonOperator(
        task_id='slack_message_{}'.format(tid),
        python_callable=slack_message,
        op_args = (msg,),
        queue="manager",
        weight_rule=WeightRule.ABSOLUTE,
        priority_weight=1000,
        dag=dag
    )


def placeholder_op(dag, tid):
    return DummyOperator(
        task_id = "dummy_{}".format(tid),
        dag=dag,
        priority_weight=1000,
        weight_rule=WeightRule.ABSOLUTE,
        queue = "manager"
    )


def reset_flags_op(dag, param):
    return PythonOperator(
        task_id="reset_flags",
        python_callable=reset_flags,
        op_args=[param,],
        dag=dag,
        weight_rule=WeightRule.ABSOLUTE,
        priority_weight=1000,
        queue="manager"
    )


def reset_flags(param):
    if param.get("SKIP_WS", False):
        Variable.set("ws_done", "yes")
    else:
        Variable.set("ws_done", "no")
    if param.get("SKIP_AGG", False):
        Variable.set("agg_done", "yes")
    else:
        Variable.set("agg_done", "no")
    try:
        target_sizes = Variable.get("cluster_target_size", deserialize_json=True)
        for key in target_sizes:
            target_sizes[key] = 0
        Variable.set("cluster_target_size", target_sizes, serialize_json=True)
    except:
        slack_message(":exclamation:Cannot reset the sizes of the clusters")


def set_variable(key, value):
    Variable.set(key, value)


def mark_done_op(dag, process):
    return PythonOperator(
        task_id="mark_{}_done".format(process),
        python_callable=set_variable,
        op_args=(process, "yes"),
        dag=dag,
        weight_rule=WeightRule.ABSOLUTE,
        priority_weight=1000,
        queue="manager"
    )


def wait(process):
    Variable.setdefault(process, "no")

    while True:
        cond = Variable.get(process)
        if cond == "yes":
            return
        else:
            sleep(30)


def wait_op(dag, process):
    return PythonOperator(
        task_id="waiting_for_{}".format(process),
        python_callable=wait,
        op_args=(process,),
        dag=dag,
        weight_rule=WeightRule.ABSOLUTE,
        priority_weight=1000,
        queue="manager"
    )


def scale_up_cluster_op(dag, stage, key, initial_size, total_size, queue, trigger_rule="one_success"):
    return PythonOperator(
        task_id='resize_{}_{}'.format(stage, total_size),
        python_callable=ramp_up_cluster,
        op_args = [key, initial_size, total_size],
        default_args=default_args,
        weight_rule=WeightRule.ABSOLUTE,
        priority_weight=1000,
        trigger_rule=trigger_rule,
        queue=queue,
        dag=dag
    )


def scale_down_cluster_op(dag, stage, key, size, queue, trigger_rule="all_success"):
    return PythonOperator(
        task_id='resize_{}_{}'.format(stage, size),
        python_callable=ramp_down_cluster,
        op_args = [key, size],
        default_args=default_args,
        weight_rule=WeightRule.ABSOLUTE,
        priority_weight=1000,
        trigger_rule=trigger_rule,
        queue=queue,
        dag=dag
    )


def reset_cluster_op(dag, stage, key, initial_size, queue):
    return PythonOperator(
        task_id='reset_{}_{}'.format(stage, key),
        python_callable=reset_cluster,
        op_args = [key, initial_size],
        default_args=default_args,
        weight_rule=WeightRule.ABSOLUTE,
        priority_weight=1000,
        trigger_rule="all_success",
        queue=queue,
        dag=dag
    )


def mark_success_or_failure(dag_id: str, anchor_task: str) -> None:
    """Marks a dag run status based on the status of an "anchor" task.

    Assumes that the most recent DAG run is the one to mark.
    """
    finished_states = ["success", "failed", "skipped"]
    assert dag_id in DagBag().dags, f"dag_id {dag_id} does not exist in DagBag"

    dag_run = DagRun.find(dag_id=dag_id)[-1]

    anchor_task_state = dag_run.get_task_instance(anchor_task).state
    assert (
        anchor_task_state in finished_states
    ), f"anchor_task {anchor_task} not finished"

    for ti in dag_run.get_task_instances():
        if ti.state not in finished_states:
            ti.set_state(anchor_task_state)


def mark_success_or_failure_op(dag: DAG, dag_id: str, anchor_task: str) -> Operator:
    """Operator fn for mark_success_or_failure callable."""
    return PythonOperator(
        task_id="mark_success_or_failure",
        python_callable=mark_success_or_failure,
        priority_weight=100_000,
        op_args=(dag_id, anchor_task),
        weight_rule=WeightRule.ABSOLUTE,
        on_failure_callback=task_failure_alert,
        queue="manager",
        dag=dag,
    )
