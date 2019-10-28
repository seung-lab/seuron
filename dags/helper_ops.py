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


def resize_cluster_op(img, dag, config_mounts, stage, connection, size):
    try:
        zone = BaseHook.get_connection(connection).login
        cluster = BaseHook.get_connection(connection).host
        max_size = int(BaseHook.get_connection(connection).extra)
    except:
        tid = "{}_{}".format(stage, size)
        return -1, placeholder_op(dag, tid)

    trigger_rule = "one_success" if size > 0 else "all_success"
    real_size = size if size < max_size else max_size
    cmdline = '/bin/bash -c ". /root/google-cloud-sdk/path.bash.inc && gcloud compute instance-groups managed resize {cluster} --size {size} --zone {zone}" && sleep 120'.format(cluster=cluster, size=real_size, zone=zone)
    return real_size, DockerWithVariablesOperator(
        config_mounts,
        mount_point=cv_path,
        task_id='resize_{}_{}'.format(stage, size),
        command=cmdline,
        default_args=default_args,
        image=img,
        weight_rule=WeightRule.ABSOLUTE,
        priority_weight=1000,
        execution_timeout=timedelta(minutes=5),
        trigger_rule=trigger_rule,
        queue='manager',
        dag=dag
    )
