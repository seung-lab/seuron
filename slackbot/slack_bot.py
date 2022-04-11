from typing import Optional
import slack_sdk as slack
from slack_sdk.rtm_v2 import RTMClient
import json
import json5
from collections import OrderedDict
import string
from airflow_api import get_variable, run_segmentation, \
    update_slack_connection, check_running, dag_state, set_variable, \
    sanity_check, chunkflow_set_env, run_inference, run_contact_surface, \
    mark_dags_success, run_dag, run_igneous_tasks, run_custom_tasks, \
    synaptor_sanity_check, run_synaptor_file_seg, run_synaptor_db_seg, \
    run_synaptor_assignment, run_corgie, run_corgie_sanity_check
from bot_info import slack_token, botid, workerid, broker_url
from kombu_helper import drain_messages
from google_metadata import get_project_data, get_instance_data, get_instance_metadata, set_instance_metadata, gce_external_ip
from copy import deepcopy
import requests
import re
import time
import logging
from secrets import token_hex
import threading
import queue
import subprocess
import sys
import traceback

ADVANCED_PARAMETERS=["BATCH_MIP_TIMEOUT", "HIGH_MIP_TIMEOUT", "REMAP_TIMEOUT", "OVERLAP_TIMEOUT", "CHUNK_SIZE", "CV_CHUNK_SIZE", "HIGH_MIP"]

param_updated = False
rtmclient = RTMClient(token=slack_token)


def install_package(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])

def clear_queues():
    with q_payload.mutex:
        q_payload.queue.clear()

    with q_cmd.mutex:
        q_cmd.queue.clear()


def create_run_token(msg):
    token = token_hex(16)
    set_variable("run_token", token)
    sc = slack.WebClient(slack_token, timeout=300)
    userid = msg['user']
    reply_msg = "use `{}, cancel run {}` to cancel the current run".format(workerid, token)
    rc = sc.chat_postMessage(
        channel=userid,
        text=reply_msg
    )
    if not rc["ok"]:
        print("Failed to send direct message")
        print(rc)


def filter_msg(msg):
    if 'subtype' in msg and msg['subtype'] != "file_share":
        return False
    text = msg["text"].strip('''_*~"'`''')

    if text.startswith(botid):
        cmd = extract_command(msg)
        if cmd == "report":
            report(msg)
        return False

    if re.search(r"^{}[\s,:]".format(workerid), text, re.IGNORECASE):
        return True


def report(msg):
    print("preparing report!")
    if check_running():
        replyto(msg, "{workerid}: busy running segmentation for {owner}".format(
            workerid=workerid,
            owner=task_owner
        ), username="seuronbot", broadcast=True)
    else:
        replyto(msg, "{workerid}: idle".format(
            workerid=workerid
        ), username="seuronbot", broadcast=True)
    hello_world()


def extract_command(msg):
    cmd = msg["text"].replace(workerid, "").replace(botid, "")
    cmd = cmd.translate(str.maketrans('', '', string.punctuation))
    cmd = cmd.lower()
    return "".join(cmd.split())


def extract_corgie_command(msg: dict) -> Optional[str]:
    """Tries to extract a corgie command from the message text.

    Args:
       msg: slack event dict from RTM client

    Returns:
       The parsed command string or None if no '`'s are in the msg text.

    Raises:
        ValueError: "`" is contained in the string, but the cmd isn't found
    """
    text = msg["text"]
    match = re.match(".*```(.*)```", text, flags=re.DOTALL)
    if match:
        rawgroup = match.groups()[0]
        # slack adds '<>' to URLs
        return rawgroup.replace("<", "").replace(">", "")
    elif '`' in text:
        raise ValueError(f"Can't parse command from text: {text}")
    else:
        return None


def extract_corgie_cluster(msg: dict) -> str:
    """Extracts the cluster setting for corgie.

    Looks for the last use of the last word in the cluster command, and
    return the stripped string beyond that.
    """
    final_cmd_word = "cluster"
    text = msg["text"]

    while True:
        next_index = text.find(final_cmd_word)
        if next_index == -1:
            break

        text = text[next_index+len(final_cmd_word):]

    return text.strip()


def replyto(msg, reply, username=workerid, broadcast=False):
    sc = slack.WebClient(slack_token, timeout=300)
    channel = msg['channel']
    userid = msg['user']
    thread_ts = msg['thread_ts'] if 'thread_ts' in msg else msg['ts']
    reply_msg = "<@{}> {}".format(userid, reply)
    rc = sc.chat_postMessage(
        username=username,
        channel=channel,
        thread_ts=thread_ts,
        reply_broadcast=broadcast,
        text=reply_msg
    )
    if not rc["ok"]:
        print("Failed to send slack message")
        print(rc)


def shut_down_clusters():
    cluster_size = get_variable('cluster_target_size', deserialize_json=True)
    for k in cluster_size:
        cluster_size[k] = 0
    set_variable("cluster_target_size", cluster_size, serialize_json=True)
    run_dag("cluster_management")


def cancel_run(msg):
    replyto(msg, "Shutting down clusters...")
    shut_down_clusters()
    time.sleep(10)

    replyto(msg, "Marking all DAG states to success...")
    mark_dags_success()
    time.sleep(10)

    #try again because some tasks might already been scheduled
    mark_dags_success()
    time.sleep(10)

    replyto(msg, "Draining tasks from the queues...")
    drain_messages(broker_url, "igneous")
    drain_messages(broker_url, "custom-cpu")
    drain_messages(broker_url, "custom-gpu")
    drain_messages(broker_url, "chunkflow")
    drain_messages(broker_url, "synaptor")
    # corgie doesn't use RabbitMQ yet
    # drain_messages(broker_url, "corgie")
    time.sleep(10)

    # Shutting down clusters again in case a scheduled task
    # scales a cluster back up
    replyto(msg, "Making sure the clusters are shut down...")
    shut_down_clusters()
    time.sleep(30)

    replyto(msg, "*Current run cancelled*", broadcast=True)


def upload_param(msg, param):
    sc = slack.WebClient(slack_token, timeout=300)
    channel = msg['channel']
    userid = msg['user']
    thread_ts = msg['thread_ts'] if 'thread_ts' in msg else msg['ts']
    sc.files_upload(
        channels=channel,
        filename="param.json",
        filetype="javascript",
        thread_ts=thread_ts,
        content=json.dumps(param, indent=4),
        initial_comment="<@{}> current parameters".format(userid)
    )


def update_metadata(msg):
    sc = slack.WebClient(slack_token, timeout=300)
    payload = {
        'user': msg['user'],
        'channel': msg['channel'],
        'thread_ts': msg['thread_ts'] if 'thread_ts' in msg else msg['ts']
    }
    update_slack_connection(payload, slack_token)
    rc = sc.users_info(
        user=msg['user']
    )
    global task_owner
    if rc["ok"]:
        task_owner = rc["user"]["profile"]["display_name"]


def download_file(msg):
    if "files" not in msg:
        replyto(msg, "You need to upload a parameter file with this message")
        return None, None
    else:
        # only use the first file:
        file_info = msg["files"][0]
        private_url = file_info["url_private_download"]
        filetype = file_info["pretty_type"]
        response = requests.get(private_url, headers={'Authorization': 'Bearer {}'.format(slack_token)})

        if response.status_code == 200:
            return filetype, response.content.decode("ascii", "ignore")
        else:
            return None, None

def download_json(msg):
    filetype, content = download_file(msg)
    if not content:
        return None
    if filetype == "Python":
        scope = {}
        try:
            exec(content, scope)
            if "submit_parameters" not in scope or not callable(scope["submit_parameters"]):
                return None
            payloads = scope['submit_parameters']()
        except:
            replyto(msg, "Cannot execute the `submit_parameters` function in the script")
            replyto(msg, "{}".format(traceback.format_exc()))
        upload_param(msg, payloads)
        return payloads
    else: #if filetype == "JavaScript/JSON":
        try:
            json_obj = json5.loads(content, object_pairs_hook=OrderedDict)
        except (ValueError, TypeError) as e:
            replyto(msg, "Cannot load the json file: {}".format(str(e)))
            return None
        return json_obj


def check_advanced_settings(params):
    if not isinstance(params, list):
        params = [params,]

    kw = []
    for p in params:
        for k in p:
            if k in ADVANCED_PARAMETERS:
                kw.append(k)

    return kw


def update_inference_param(msg):
    global param_updated
    json_obj = download_json(msg)
    if json_obj:
        if not check_running():
            clear_queues()
            drain_messages(broker_url, "chunkflow")

            if isinstance(json_obj, list):
                replyto(msg, "*{} batch jobs detected, only sanity check the first one for now*".format(len(json_obj)))
                q_payload.put(json_obj)
                json_obj = json_obj[0]

            supply_default_param(json_obj)
            replyto(msg, "Running chunkflow setup_env, please wait")
            update_metadata(msg)
            set_variable('inference_param', json_obj, serialize_json=True)
            chunkflow_set_env()
            param_updated = True
        else:
            replyto(msg, "Busy right now")

    return


def update_param(msg, advanced=False):
    global param_updated
    json_obj = download_json(msg)
    if not advanced:
        kw = check_advanced_settings(json_obj)
        if kw:
            replyto(msg, f'You are trying to change advanced parameters: {",".join("`"+x+"`" for x in kw)}')
            replyto(msg, "Use `please update parameters` to confirm that you know what you are doing")
            return
    if json_obj:
        if not check_running():
            clear_queues()

            if isinstance(json_obj, list):
                if (len(json_obj) > 1):
                    replyto(msg, "*{} batch jobs detected, only sanity check the first one for now*".format(len(json_obj)))
                q_payload.put(json_obj)
                json_obj = json_obj[0]

            supply_default_param(json_obj)
            replyto(msg, "Running sanity check, please wait")
            update_metadata(msg)
            set_variable('param', json_obj, serialize_json=True)
            time.sleep(30)
            sanity_check()
            param_updated = True
        else:
            replyto(msg, "Busy right now")

    return


def update_synaptor_params(msg):
    """Parses the synaptor configuration file to check for simple errors."""
    # Current file format is ini/toml, not json
    _, content = download_file(msg)

    if content is not None:  # download_file returns None if there's a problem
        if check_running():
            replyto(msg, "Busy right now")
            return

        replyto(msg, "Running synaptor sanity check. Please wait.")

        update_metadata(msg)
        set_variable("synaptor_param", content)
        synaptor_sanity_check()

    else:
        replyto(msg, "Error reading file")


def synaptor_file_seg(msg):
    """Runs the file segmentation DAG."""
    if check_running():
        replyto(msg, "Busy right now")
        return

    replyto(msg, "Running synaptor file segmentation. Please wait.")
    create_run_token(msg)
    update_metadata(msg)
    run_synaptor_file_seg()


def synaptor_db_seg(msg):
    """Runs the file segmentation DAG."""
    if check_running():
        replyto(msg, "Busy right now")
        return

    replyto(msg, "Running synaptor file segmentation. Please wait.")
    create_run_token(msg)
    update_metadata(msg)
    run_synaptor_db_seg()


def synaptor_assignment(msg):
    """Runs the file segmentation DAG."""
    if check_running():
        replyto(msg, "Busy right now")
        return

    replyto(msg, "Running synaptor synapse assignment. Please wait.")
    create_run_token(msg)
    update_metadata(msg)
    run_synaptor_assignment()


def set_corgie_cluster(msg: dict) -> None:
    """Sets the corgie cluster to CPU/GPU."""
    if check_running():
        replyto(msg, "Please wait until the cluster is idle."
                " Changing the cluster while a command is running could cause"
                " problems.")
        return

    cluster = extract_corgie_cluster(msg)

    if cluster in ["cpu", "corgie-cpu"]:
        set_variable("active_corgie_cluster", "corgie-cpu")
        replyto(msg, "corgie cluster set to: corgie-cpu")

    elif cluster in ["gpu", "corgie-gpu"]:
        set_variable("active_corgie_cluster", "corgie-gpu")
        replyto(msg, "corgie cluster set to: corgie-gpu")

    else:
        replyto(msg, f"ERROR: cluster type {cluster} not recognized")


def corgie_sanity_check(msg: dict) -> None:
    """Runs the corgie_sanity_check DAG."""
    try:
        command = extract_corgie_command(msg)
    except ValueError as e:
        replyto(msg, str(e))
        return

    if command is None:
        command = get_variable("corgie_command")
    else:
        set_variable("corgie_command", command)

    replyto(msg, "Running corgie sanity check. Please wait")
    run_corgie_sanity_check()


def corgie_command(msg) -> None:
    """Parses a corgie command from the message and runs it."""
    try:
        command = extract_corgie_command(msg)
    except ValueError as e:
        replyto(msg, str(e))
        return
    set_variable("corgie_command", command)

    # Politely deny request if busy
    currently_running = check_running()
    if currently_running and command is not None:
        set_variable("corgie_command", command)
        replyto(msg, "Busy right now, but I've saved the command")
        return
    elif currently_running:
        replyto(msg, "Busy right now")
        return

    # Run (a possibly previous) command
    if command is None:
        command = get_variable("corgie_command")
        replyto(msg, f"Running previously saved command:\n```{command}```")
    else:
        replyto(msg, f"Running command:\n```{command}```")

    create_run_token(msg)
    update_metadata(msg)
    run_corgie()


def run_igneous_scripts(msg):
    _, payload = download_file(msg)
    if payload:
        if not check_running():
            drain_messages(broker_url, "igneous")
            drain_messages(broker_url, "igneous_ret")
            drain_messages(broker_url, "igneous_err")
            create_run_token(msg)
            update_metadata(msg)
            set_variable('igneous_script', payload)
            replyto(msg, "Execute `submit_tasks` function")
            run_igneous_tasks()
        else:
            replyto(msg, "Busy right now")

    return


def run_custom_scripts(msg, task_type):
    _, payload = download_file(msg)
    if payload:
        if not check_running():
            for t in ['gpu', 'cpu']:
                drain_messages(broker_url, f"custom-{t}")
                drain_messages(broker_url, f"custom-{t}_ret")
                drain_messages(broker_url, f"custom-{t}_err")
            create_run_token(msg)
            update_metadata(msg)
            set_variable('custom_script', payload)
            replyto(msg, "Execute `submit_tasks` function")
            run_custom_tasks(task_type)
        else:
            replyto(msg, "Busy right now")

    return


def update_python_packages(msg):
    _, payload = download_file(msg)
    replyto(msg, "*WARNING:Extra python packages are available for workers only*")
    if payload:
        for l in payload.splitlines():
            replyto(msg, f"Testing python packages *{l}*")
            try:
                install_package(l)
            except:
                replyto(msg, f":u7981:Failed to install package *{l}*")
                replyto(msg, "{}".format(traceback.format_exc()))
                return

        set_variable('python_packages', payload)
        replyto(msg, "Packages are ready for *workers*")


def supply_default_param(json_obj):
    if not json_obj.get("NAME", ""):
        json_obj["NAME"] = token_hex(16)

    if "SCRATCH_PREFIX" not in json_obj and "SCRATCH_PATH" not in json_obj:
        json_obj["SCRATCH_PREFIX"] = "gs://ranl_pipeline_scratch/"

    for p in ["WS","SEG"]:
        if "{}_PREFIX".format(p) not in json_obj and "{}_PATH".format(p) not in json_obj:
            json_obj["{}_PREFIX".format(p)] = json_obj.get("NG_PREFIX", "gs://ng_scratch_ranl/make_cv_happy/") + p.lower() + "/"


def update_ip_address():
    host_ip = gce_external_ip()
    try:
        set_variable("webui_ip", host_ip)
    except:
        sys.exit("database not ready")
    return host_ip


def dispatch_command(cmd, msg):
    global param_updated
    print(cmd)
    if cmd == "parameters":
        param = get_variable("param", deserialize_json=True)
        upload_param(msg, param)
    elif cmd == "updateparameters":
        update_param(msg, advanced=False)
    elif cmd == "pleaseupdateparameters":
        update_param(msg, advanced=True)
    elif cmd == "updateinferenceparameters":
        update_inference_param(msg)
    elif cmd.startswith("cancelrun"):
        token = get_variable("run_token")
        if not token:
            replyto(msg, "The bot is idle, nothing to cancel")
        elif cmd != "cancelrun"+token:
            replyto(msg, "Wrong token")
        else:
            if check_running():
                clear_queues()
                q_cmd.put("cancel")
                cancel_run(msg)
                set_variable("run_token", "")
            else:
                replyto(msg, "The bot is idle, nothing to cancel")

    elif cmd == "runsegmentation" or cmd == "runsegmentations":
        state, _ = dag_state("sanity_check")
        if check_running():
            replyto(msg, "I am busy right now")
        elif not param_updated:
            replyto(msg, "You have to update the parameters before starting the segmentation")
        elif state != "success":
            replyto(msg, "Sanity check failed, try again")
        else:
            replyto(msg, "Start segmentation")
            create_run_token(msg)
            update_metadata(msg)
            param_updated = False
            if q_payload.qsize() == 0:
                run_segmentation()
            else:
                q_payload.put(msg)
                q_cmd.put("runseg")
    elif cmd == "runinference" or cmd == "runinferences":
        state, _ = dag_state("chunkflow_generator")
        if check_running():
            replyto(msg, "I am busy right now")
        elif not param_updated:
            replyto(msg, "You have to update the parameters before starting the inference")
        elif state != "success":
            replyto(msg, "Chunkflow set_env failed, try again")
        else:
            replyto(msg, "Start inference")
            create_run_token(msg)
            update_metadata(msg)
            param_updated = False
            if q_payload.qsize() == 0:
                run_inference()
            else:
                q_payload.put(msg)
                q_cmd.put("runinf")
    elif cmd == "runigneoustask" or cmd == "runigneoustasks":
        if check_running():
            replyto(msg, "I am busy right now")
        else:
            run_igneous_scripts(msg)
    elif cmd == "runcustomcputask" or cmd == "runcustomcputasks":
        if check_running():
            replyto(msg, "I am busy right now")
        else:
            run_custom_scripts(msg, "cpu")
    elif cmd == "runcustomgputask" or cmd == "runcustomgputasks":
        if check_running():
            replyto(msg, "I am busy right now")
        else:
            run_custom_scripts(msg, "gpu")
    elif cmd == "updatepythonpackage" or cmd == "updatepythonpackages":
        update_python_packages(msg)
    elif cmd == "redeploydockerstack":
        if check_running():
            replyto(msg, "I am busy right now")
        else:
            replyto(msg, "Redeploy seuronbot docker stack on the bootstrap node")
            update_metadata(msg)
            set_redeploy_flag(True)
            time.sleep(300)
            replyto(msg, "Failed to restart the bot")
    elif cmd == "extractcontactsurfaces":
        state, _ = dag_state("sanity_check")
        if check_running():
            replyto(msg, "I am busy right now")
        elif state != "success":
            replyto(msg, "Sanity check failed, try again")
        else:
            replyto(msg, "Extract contact surfaces")
            create_run_token(msg)
            update_metadata(msg)
            param_updated = False
            run_contact_surface()
    elif cmd in ["updatesynaptorparams", "updatesynaptorparameters"]:
        update_synaptor_params(msg)
    elif cmd in ["runsynaptorfileseg",
                 "runsynaptorfilesegmentation"]:
        synaptor_file_seg(msg)
    elif cmd in ["runsynaptordbseg",
                 "runsynaptordatabaseseg",
                 "runsynaptordbsegmentation",
                 "runsynaptordatabasesegmentation"]:
        synaptor_db_seg(msg)
    elif cmd in ["runsynaptorassignment",
                 "runsynaptorsynapseassignment"]:
        synaptor_assignment(msg)
    elif cmd.startswith("setcorgiecluster"):
        set_corgie_cluster(msg)
    elif cmd.startswith("runcorgiecommand"):
        corgie_command(msg)
    elif cmd.startswith("runcorgiesanitycheck"):
        corgie_sanity_check(msg)
    else:
        replyto(msg, "Sorry I do not understand, please try again.")


@rtmclient.on('message')
def process_message(client: RTMClient, event: dict):
    print(json.dumps(event, indent=4))
    if filter_msg(event):
        cmd = extract_command(event)
        dispatch_command(cmd, event)

@rtmclient.on('reaction_added')
def process_reaction(client: RTMClient, event: dict):
    print("reaction added")
    print(json.dumps(event, indent=4))


@rtmclient.on('hello')
def process_hello(client: RTMClient, event: dict):
    hello_world(client.web_client)


def hello_world(client=None):
    if not client:
        client = slack.WebClient(token=slack_token)

    host_ip = update_ip_address()

    client.chat_postMessage(
        channel='#seuron-alerts',
        username=workerid,
        text="Hello from <https://{}/airflow/home|{}>".format(host_ip, host_ip))

    if get_instance_data("attributes/redeploy") == 'true':
        set_redeploy_flag(False)
        send_reset_message(client)


def send_reset_message(client):
    from airflow.hooks.base_hook import BaseHook
    SLACK_CONN_ID = "Slack"
    try:
        slack_workername = BaseHook.get_connection(SLACK_CONN_ID).login
        slack_extra = json.loads(BaseHook.get_connection(SLACK_CONN_ID).extra)
    except:
        return

    slack_username = slack_extra['user']
    slack_channel = slack_extra['channel']
    slack_thread = slack_extra['thread_ts']

    client.chat_postMessage(
        username=slack_workername,
        channel=slack_channel,
        thread_ts=slack_thread,
        reply_broadcast=True,
        text=f"<@{slack_username}>, bot upgraded/rebooted."
    )


def handle_batch(q_payload, q_cmd):
    while True:
        current_task="runseg"
        logger.debug("check queue")
        time.sleep(1)
        if q_payload.qsize() == 0:
            continue
        if q_cmd.qsize() != 0:
            cmd = q_cmd.get()
            if cmd != "runseg" and cmd != "runinf":
                continue
            else:
                current_task = cmd
        else:
            continue

        logger.debug("get message from queue")

        json_obj = q_payload.get()
        msg = q_payload.get()

        if json_obj is None:
            continue

        if (not isinstance(json_obj, list)) or (not isinstance(json_obj[0], dict)):
            replyto(msg, "Batch process expects an array of dicts from the json file")
            continue

        replyto(msg, "Batch jobs will reuse on the parameters from the first job unless new parameters are specified, *including those with default values*")

        default_param = json_obj[0]
        for i, p in enumerate(json_obj):
            if q_cmd.qsize() != 0:
                cmd = q_cmd.get()
                if cmd == "cancel":
                    replyto(msg, "Cancel batch process")
                    break
            param = deepcopy(default_param)
            if i > 0:
                if 'NAME' in param:
                    del param['NAME']
                for k in p:
                    param[k] = p[k]
                supply_default_param(param)
                update_metadata(msg)
                replyto(msg, "*Sanity check: batch job {} out of {}*".format(i+1, len(json_obj)))
                state = "unknown"
                if current_task == "runseg":
                    set_variable('param', param, serialize_json=True)
                    sanity_check()
                    wait_for_airflow()
                    state, _ = dag_state("sanity_check")
                elif current_task == "runinf":
                    set_variable('inference_param', param, serialize_json=True)
                    chunkflow_set_env()
                    wait_for_airflow()
                    state, _ = dag_state("chunkflow_generator")

                if state != "success":
                    replyto(msg, "*Sanity check failed, abort!*")
                    break

            state = "unknown"
            replyto(msg, "*Starting batch job {} out of {}*".format(i+1, len(json_obj)), broadcast=True)
            if current_task == "runseg":
                run_segmentation()
                wait_for_airflow()
                state, _ = dag_state("segmentation")
            elif current_task == "runinf":
                run_inference()
                wait_for_airflow()
                state, _ = dag_state("chunkflow_worker")

            if state != "success":
                replyto(msg, "*Bach job failed, abort!*")
                break

        replyto(msg, "*Batch process finished*")

def set_redeploy_flag(value):
    project_id = get_project_data("project-id")
    vm_name = get_instance_data("name")
    vm_zone = get_instance_data("zone").split('/')[-1]
    data = get_instance_metadata(project_id, vm_zone, vm_name)
    key_exist = False
    for item in data['items']:
        if item['key'] == 'redeploy':
            item['value'] = value
            key_exist = True

    if not key_exist:
        data['items'].append({'key': 'redeploy', 'value':value})
    set_instance_metadata(project_id, vm_zone, vm_name, data)

def wait_for_airflow():
    time.sleep(60)
    while check_running():
        logger.debug("waiting for airflow")
        time.sleep(60)

if __name__ == '__main__':
    task_owner = "seuronbot"

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    q_payload = queue.Queue()
    q_cmd = queue.Queue()
    batch = threading.Thread(target=handle_batch, args=(q_payload, q_cmd,))

    batch.start()

    #logger.info("subprocess pid: {}".format(batch.pid))

    rtmclient.start()

    batch.join()
