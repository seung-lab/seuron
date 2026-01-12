from airflow.utils.db import provide_session


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


def get_composite_worker_capacities():
    import json
    from airflow.hooks.base_hook import BaseHook
    cluster_info = json.loads(BaseHook.get_connection("InstanceGroups").extra)
    try:
        composite_worker_info = cluster_info["composite"]
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
    from airflow.hooks.base_hook import BaseHook
    from airflow.exceptions import AirflowNotFoundException
    try:
        ig_conn = BaseHook.get_connection(conn)
    except AirflowNotFoundException:
        return default_var

    return ig_conn


@provide_session
def query_task_instances(queue, session):
    from airflow import models
    from airflow.utils.state import State
    TI = models.TaskInstance
    return session.query(TI).filter(TI.queue == queue).filter(TI.state.in_(State.unfinished)).all()


def remove_workers(queue):
    from airflow.utils.state import State
    tis = query_task_instances(queue=queue)
    for ti in tis:
        ti.set_state(State.SUCCESS)


def db_name(run_name, data_ext):
    import re
    prefix = re.sub(r'[^a-z0-9]', '_', run_name.strip().lower()).lstrip('_')
    max_len = 62 - len(data_ext)
    res = prefix[:max_len] + '_' + data_ext
    print(f"squeeze the db name to {res}")
    return res
