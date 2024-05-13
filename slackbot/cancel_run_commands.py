import time
from dags.google_api_helper import toggle_nfs_server
from seuronbot import SeuronBot
from bot_utils import replyto, extract_command, clear_queues
from airflow_api import get_variable, set_variable, \
    check_running, mark_dags_success, run_dag
from bot_info import broker_url
from kombu_helper import drain_messages, put_message

if get_variable("vendor") == "Google":
    import google_api_helper as cluster_api
else:
    cluster_api = None


@SeuronBot.on_message("cancel run",
                      description="Cancel the current run, must provide a matching token",
                      exclusive=False,
                      cancelable=False,
                      extra_parameters=True)
def on_cancel_run(msg):
    token = get_variable("run_token")
    cmd = extract_command(msg["text"])
    if not token:
        replyto(msg, "The bot is idle, nothing to cancel")
    elif cmd != "cancelrun"+token:
        replyto(msg, "Wrong token")
    else:
        if check_running():
            clear_queues()
            put_message(broker_url, "seuronbot_cmd", "cancel")
            cancel_run(msg)
            set_variable("run_token", "")
        else:
            replyto(msg, "The bot is idle, nothing to cancel")


def shut_down_clusters():
    cluster_size = get_variable('cluster_target_size', deserialize_json=True)
    for k in cluster_size:
        cluster_size[k] = 0
    set_variable("cluster_target_size", cluster_size, serialize_json=True)
    run_dag("cluster_management")
    if cluster_api:
        toggle_nfs_server(on=False)


def cancel_run(msg):
    replyto(msg, "Shutting down clusters...")
    shut_down_clusters()
    time.sleep(10)

    replyto(msg, "Marking all DAG states to success...")
    mark_dags_success()
    time.sleep(60)

    #try again because some tasks might already been scheduled
    mark_dags_success()
    time.sleep(60)

    replyto(msg, "Draining tasks from the queues...")
    drain_messages(broker_url, "igneous")
    drain_messages(broker_url, "custom-cpu")
    drain_messages(broker_url, "custom-gpu")
    drain_messages(broker_url, "chunkflow")
    drain_messages(broker_url, "synaptor")
    drain_messages(broker_url, "deepem-gpu")
    time.sleep(10)

    # Shutting down clusters again in case a scheduled task
    # scales a cluster back up
    replyto(msg, "Making sure the clusters are shut down...")
    shut_down_clusters()
    time.sleep(30)

    replyto(msg, "*Current run cancelled*", broadcast=True)
