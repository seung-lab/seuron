from airflow.operators.docker_plugin import DockerWithVariablesOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.weight_rule import WeightRule
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from datetime import timedelta
from time import sleep
from slack_message import slack_message
from param_default import default_args, cv_path
from google_api_helper import increase_instance_group_size, reduce_instance_group_size

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


def set_variable(key, value):
    Variable.set(key, value)


def mark_done_op(dag, var):
    return PythonOperator(
        task_id="mark_{}".format(var),
        python_callable=set_variable,
        op_args=(var, "yes"),
        dag=dag,
        weight_rule=WeightRule.ABSOLUTE,
        priority_weight=1000,
        queue="manager"
    )


def wait(var):
    Variable.setdefault(var, "no")

    while True:
        cond = Variable.get(var)
        if cond == "yes":
            return
        else:
            sleep(30)


def wait_op(dag, var):
    return PythonOperator(
        task_id="waiting_for_{}".format(var),
        python_callable=wait,
        op_args=(var,),
        dag=dag,
        weight_rule=WeightRule.ABSOLUTE,
        priority_weight=1000,
        queue="manager"
    )


def scale_up_cluster_op(dag, stage, key, size):
    return PythonOperator(
        task_id='resize_{}_{}'.format(stage, size),
        python_callable=increase_instance_group_size,
        op_args = [key, size],
        default_args=default_args,
        weight_rule=WeightRule.ABSOLUTE,
        priority_weight=1000,
        trigger_rule="one_success",
        queue='manager',
        dag=dag
    )


def scale_down_cluster_op(dag, stage, key, size):
    return PythonOperator(
        task_id='resize_{}_{}'.format(stage, size),
        python_callable=reduce_instance_group_size,
        op_args = [key, size],
        default_args=default_args,
        weight_rule=WeightRule.ABSOLUTE,
        priority_weight=1000,
        trigger_rule="all_success",
        queue='manager',
        dag=dag
    )
