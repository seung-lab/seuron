from cloudfiles.paths import to_https_protocol, ascloudpath, ExtractedPath


def resolve_url(url: str) -> str:
    """Resolves protocol aliases and returns a https url."""
    if url.startswith("gs://"):
        return url
    result = to_https_protocol(url)
    if isinstance(result, ExtractedPath):
        return ascloudpath(result)
    return result


def check_manager_node(ntasks):
    import psutil
    import humanize
    from slack_message import slack_message
    available_mem = psutil.virtual_memory().available
    expected_mem = 2*ntasks*1024*1024*10 # 10MB for each postgresql connection
    if available_mem < expected_mem:
        slack_message(f":u7981:*ERROR: You need {humanize.naturalsize(expected_mem)} RAM to handle {ntasks} tasks, the manager node only have {humanize.naturalsize(available_mem)} RAM*")
        return False

    return True


def get_composite_worker_capacities(key=None):
    import json
    from airflow.exceptions import AirflowNotFoundException

    if not key:
        return get_composite_worker_capacities("composite").union(get_composite_worker_capacities("mega"))

    ig_conn = get_connection("InstanceGroups")
    if not ig_conn:
        raise AirflowNotFoundException("Connection 'InstanceGroups' not found")
    cluster_info = json.loads(ig_conn.extra)

    try:
        composite_worker_info = cluster_info[key]
    except:
        return set()

    if not isinstance(composite_worker_info, list):
        return set()

    layers = []
    for c in composite_worker_info:
        try:
            layers += [x["layer"] for x in c["workerConcurrencies"]]
        except:
            pass

    return set(layers)


def estimate_worker_instances(tasks, cluster_info):
    import math
    workers = 0
    remaining_tasks = tasks
    try:
        for c in cluster_info:
            if c['max_size']*c['concurrency'] < remaining_tasks:
                workers += c['max_size']
                remaining_tasks -= c['max_size']*c['concurrency']
            else:
                workers += int(math.ceil(remaining_tasks / c['concurrency']))
                break
    except:
        return 1

    return workers


def get_connection(conn, default_var=None):
    from airflow.exceptions import AirflowNotFoundException
    try:
        # resolves via the execution API inside task runners.
        from airflow.sdk.bases.hook import BaseHook
        ig_conn = BaseHook.get_connection(conn)
    except AirflowNotFoundException:
        # Parse-time (DAG processor) / legacy fallback: DB-backed lookup,
        # as in Airflow 2. Contains all errors: if the conn is missing we
        # return default_var exactly as Airflow 2 did.
        try:
            from airflow.models.connection import Connection
            return Connection.get_connection_from_secrets(conn)
        except Exception:
            return default_var

    return ig_conn


def query_task_instances(queue):
    """Task instances (v2 API models) assigned to `queue` that are not finished."""
    import af_api
    from airflow.utils.state import State

    states = [s for s in State.unfinished if s is not None]
    return af_api.list_task_instances(state=states, queue=queue)


def remove_workers(queue):
    """Mark the queue's unfinished task instances as successful."""
    import af_api
    from airflow.utils.state import State

    tis = query_task_instances(queue=queue)
    if not tis:
        return
    for ti in tis:
        af_api.set_task_instance_state(
            ti.dag_id, ti.dag_run_id, ti.task_id, ti.map_index,
            State.SUCCESS)


def db_name(run_name, data_ext):
    import re
    prefix = re.sub(r'[^a-z0-9]', '_', run_name.strip().lower()).lstrip('_')
    max_len = 62 - len(data_ext)
    res = prefix[:max_len] + '_' + data_ext
    print(f"squeeze the db name to {res}")
    return res
