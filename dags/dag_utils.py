def check_manager_node(ntasks):
    import psutil
    import humanize
    from slack_message import slack_message
    total_mem = psutil.virtual_memory().total
    expected_mem = 2*ntasks*1024*1024*10 # 10MB for each postgresql connection
    if total_mem < expected_mem:
        slack_message(f"You need {humanize.naturalsize(expected_mem)} RAM to handle {ntasks} tasks, the manager node only have {humanize.naturalsize(total_mem)} RAM")
        return False

    return True

def get_composite_worker_limits():
    import json
    from airflow.hooks.base_hook import BaseHook
    cluster_info = json.loads(BaseHook.get_connection("InstanceGroups").extra)
    layers = []
    for c in cluster_info["composite"]:
        layers += [x["layer"] for x in c["workerConcurrencies"]]

    return min(layers), max(layers)


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
