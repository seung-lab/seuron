import json
import time

import af_api

from bot_info import workerid, slack_notification_channel

seuron_dags = ['sanity_check', 'segmentation', 'watershed', 'agglomeration',
               "postprocess", 'chunkflow_worker', 'chunkflow_generator',
               'contact_surface', "igneous", "custom-cpu", "custom-gpu",
               "synaptor_sanity_check", "synaptor", "wkt_cutouts",
               "wkt_export", "training"]


_MISSING = object()


def check_running():
    """Checks whether the DAGs within the seuron_dags list (above) is running."""
    runs = af_api.list_dag_runs(state="running")
    return any(r.dag_id in seuron_dags for r in runs)


def wait_for_dag_refresh(dag_id):
    last_parsed_time = None
    for _ in range(10):
        dag = af_api.get_dag(dag_id)
        if dag is None:
            time.sleep(5)
            continue
        parsed = dag.last_parsed_time
        if not last_parsed_time:
            last_parsed_time = parsed
        elif parsed and parsed > last_parsed_time:
            print(parsed)
            print(last_parsed_time)
            return
        time.sleep(5)


def run_dag(dag_id, wait_for_completion=False):
    dags_need_refresh = ["segmentation", "chunkflow_worker", "synaptor"]
    if dag_id in dags_need_refresh:
        wait_for_dag_refresh(dag_id)
    run = af_api.trigger_dag_run(dag_id)
    if wait_for_completion:
        while True:
            time.sleep(5)
            run = af_api.get_dag_run(dag_id, run.dag_run_id)
            state = run.state
            print(f"waiting for dag {run.dag_id}, {run.dag_run_id} state : {state}")
            if state == "success" or state == "failed":
                ntasks = len(af_api.list_task_instances(dag_id, run.dag_run_id))
                print(run.start_date, run.end_date, ntasks)
                if state == "success" and ntasks == 0:
                    print("0 task in the dagrun, retrigger")
                    run = af_api.trigger_dag_run(dag_id)
                else:
                    break
    return run


def mark_dags_success():
    runs = af_api.list_dag_runs(state="running")
    for r in runs:
        if r.dag_id not in seuron_dags:
            continue
        try:
            af_api.set_dag_run_state(r.dag_id, r.dag_run_id, "success")
        except RuntimeError as exc:
            # e.g. 404 for a run whose DAG is no longer serialized
            print(f"failed to mark {r.dag_id}/{r.dag_run_id} success: {exc}")


def update_slack_connection(payload, token):
    conn_id = "Slack"
    print("Upsert slack connection")
    af_api.upsert_connection(
        conn_id,
        conn_type='http',
        host='localhost',
        login=workerid,
        password=token,
        extra=json.dumps({**payload, "notification_channel": slack_notification_channel}, indent=4),
    )


def update_user_info(userid):
    set_variable('author', userid)


def get_variable(key, deserialize_json=False, **kwargs):
    default_var = kwargs.pop("default_var", _MISSING)
    if kwargs:
        raise TypeError(f"unsupported kwargs: {sorted(kwargs)}")
    value = af_api.get_variable(key)
    if value is None:
        if default_var is _MISSING:
            raise KeyError(key)
        return default_var
    if deserialize_json:
        return json.loads(value)
    return value


def set_variable(key, value, serialize_json=False):
    if serialize_json:
        value = json.dumps(value)
    af_api.set_variable(key, str(value))


def latest_dagrun_state(dag_id):
    print("check dag states")
    if af_api.get_dag(dag_id) is None:
        print("=========== dag_id does not exist ============")
        return "null"

    runs = af_api.list_dag_runs(dag_id=dag_id)
    if not runs:
        return "unknown"
    # run_after is a required field of DAGRunResponse, so the key is always
    # a tz-aware datetime (logical_date is null for manual runs)
    latest_run = max(runs, key=lambda r: r.logical_date or r.run_after)
    return latest_run.state


def set_is_paused(dag_id, is_paused):
    return af_api.set_dag_paused(dag_id, is_paused)
