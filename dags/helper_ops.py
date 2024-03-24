from airflow.operators.python import PythonOperator
from airflow.utils.weight_rule import WeightRule
from airflow.models import Variable
from slack_message import slack_message
from param_default import default_args

if Variable.get("vendor") == "Google":
    import google_api_helper as cluster_api
else:
    cluster_api = None


def slack_message_op(dag, tid, msg):
    return PythonOperator(
        task_id='slack_message_{}'.format(tid),
        python_callable=slack_message,
        op_args=(msg,),
        queue="manager",
        weight_rule=WeightRule.ABSOLUTE,
        priority_weight=1000,
        dag=dag
    )


def placeholder_op(dag, tid):
    from airflow.operators.dummy import DummyOperator
    return DummyOperator(
        task_id="dummy_{}".format(tid),
        dag=dag,
        priority_weight=1000,
        weight_rule=WeightRule.ABSOLUTE,
        queue="manager"
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

    Variable.set("pp_done", "no")

    try:
        target_sizes = Variable.get("cluster_target_size", deserialize_json=True)
        for key in target_sizes:
            target_sizes[key] = 0
        Variable.set("cluster_target_size", target_sizes, serialize_json=True)
    except:
        slack_message(":exclamation:Cannot reset the sizes of the clusters")


def setup_redis(varname, dbname):
    import os
    import redis
    from param_default import redis_databases
    param = Variable.get(varname, deserialize_json=True)
    if "REDIS_SERVER" not in os.environ:
        slack_message("*No redis server, nothing to flush*")
    else:
        param["REDIS_SERVER"] = os.environ['REDIS_SERVER']
        param["REDIS_DB"] = redis_databases[dbname]
        slack_message(f'*Use redis database {param["REDIS_DB"]} to track the progress of the run*')
        r = redis.Redis(host=param["REDIS_SERVER"], db=redis_databases["SEURON"])
        r.flushdb()
        if param.get("RESET_REDIS_DB", True):
            r = redis.Redis(host=param["REDIS_SERVER"], db=param["REDIS_DB"])
            r.flushdb()
            slack_message(f'*Flush redis database {param["REDIS_DB"]}*')
        Variable.set(varname, param, serialize_json=True)


def setup_redis_op(dag, varname, dbname):
    return PythonOperator(
        task_id="setup_redis",
        python_callable=setup_redis,
        op_args=(varname, dbname, ),
        queue="manager",
        dag=dag)


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
    from time import sleep
    Variable.setdefault(process, "no")

    while True:
        cond = Variable.get(process)
        if cond == "yes":
            return
        else:
            sleep(5)


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


def scale_up_cluster_op(dag, stage, key, initial_size, total_size, queue, tag=None, trigger_rule="one_success"):
    if not tag:
        tag = str(total_size)
    if cluster_api:
        return PythonOperator(
            task_id=f'resize_{stage}_{tag}',
            python_callable=cluster_api.ramp_up_cluster,
            op_args=[key, initial_size, total_size],
            default_args=default_args,
            weight_rule=WeightRule.ABSOLUTE,
            priority_weight=1000,
            trigger_rule=trigger_rule,
            queue=queue,
            dag=dag
        )
    else:
        return placeholder_op(dag, f'resize_{stage}_{total_size}')


def scale_down_cluster_op(dag, stage, key, size, queue, tag=None, trigger_rule="all_success"):
    if not tag:
        tag = str(size)
    if cluster_api:
        return PythonOperator(
            task_id=f'resize_{stage}_{tag}',
            python_callable=cluster_api.ramp_down_cluster,
            op_args=[key, size],
            default_args=default_args,
            weight_rule=WeightRule.ABSOLUTE,
            priority_weight=1000,
            trigger_rule=trigger_rule,
            queue=queue,
            dag=dag
        )
    else:
        return placeholder_op(dag, f'resize_{stage}_{size}')


def reset_cluster_op(dag, stage, key, initial_size, queue):
    if cluster_api:
        return PythonOperator(
            task_id=f'reset_{stage}_{key}',
            python_callable=cluster_api.reset_cluster,
            op_args=[key, initial_size],
            default_args=default_args,
            weight_rule=WeightRule.ABSOLUTE,
            priority_weight=1000,
            trigger_rule="all_success",
            queue=queue,
            dag=dag
        )
    else:
        return placeholder_op(dag, f'reset_{stage}_{key}')


def collect_metrics_op(dag):
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator
    return TriggerDagRunOperator(
        task_id="trigger_compute_metrics",
        trigger_dag_id="compute_metrics",
        conf={
            "dag_id": "{{ dag_run.dag_id }}",
            "run_id": "{{ dag_run.run_id }}",
        },
        weight_rule=WeightRule.ABSOLUTE,
        priority_weight=1000,
        dag=dag,
        queue="manager"
    )


def toggle_nfs_server_op(dag, on=False):
    if cluster_api:
        return PythonOperator(
            task_id=f'toggle_nfs_server_{"on" if on else "off"}',
            python_callable=cluster_api.toggle_nfs_server,
            op_kwargs={"on": on},
            default_args=default_args,
            weight_rule=WeightRule.ABSOLUTE,
            priority_weight=1000,
            trigger_rule="all_done",
            queue='manager',
            dag=dag
        )
    else:
        return placeholder_op(dag, f'dummy_toggle_nfs_server_{"on" if on else "off"}')
