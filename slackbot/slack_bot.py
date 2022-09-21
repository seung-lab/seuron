import slack_sdk as slack
import json
import json5
from collections import OrderedDict
from airflow_api import get_variable, \
    check_running, latest_dagrun_state, set_variable, \
    mark_dags_success, run_dag
from bot_info import slack_token, botid, workerid, broker_url, slack_notification_channel
from kombu_helper import drain_messages, visible_messages, get_message, put_message
from bot_utils import replyto, extract_command, download_file
from seuronbot import SeuronBot
from google_metadata import get_project_data, get_instance_data, get_instance_metadata, set_instance_metadata, gce_external_ip
from copy import deepcopy
import time
import logging
from secrets import token_hex
from datetime import datetime
from configparser import ConfigParser
import threading
import queue
import sys
import traceback

import update_python_packages
import redeploy_docker_stack
import igneous_tasks_commands
import custom_tasks_commands


def excepthook(exctype, excvalue, exctraceback):
    client = slack.WebClient(token=slack_token)
    client.chat_postMessage(
            channel=slack_notification_channel,
            username=workerid,
            text=f"An uncaught exception occured. Restart slackbot!\n ```{traceback.format_exc()}```",
    )
    sys.exit()


sys.excepthook = excepthook

ADVANCED_PARAMETERS=["BATCH_MIP_TIMEOUT", "HIGH_MIP_TIMEOUT", "REMAP_TIMEOUT", "OVERLAP_TIMEOUT", "CHUNK_SIZE", "CV_CHUNK_SIZE", "HIGH_MIP"]

param_updated = None


def clear_queues():
    drain_messages(broker_url, "seuronbot_payload")
    drain_messages(broker_url, "seuronbot_cmd")


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


def guess_run_type(param):
    if "WORKER_IMAGE" in param:
        return "seg_run"
    elif "CHUNKFLOW_IMAGE" in param:
        return "inf_run"
    else:
        return None


def update_inference_param(msg):
    global param_updated
    json_obj = download_json(msg)
    if json_obj:
        clear_queues()
        drain_messages(broker_url, "chunkflow")

        if isinstance(json_obj, list):
            replyto(msg, "*{} batch jobs detected, only sanity check the first one for now*".format(len(json_obj)))
            put_message(broker_url, "seuronbot_payload", json_obj)
            json_obj = json_obj[0]

        supply_default_param(json_obj)
        replyto(msg, "Running chunkflow setup_env, please wait")
        set_variable('inference_param', json_obj, serialize_json=True)
        param_updated = "inf_run"
        run_dag("chunkflow_generator")

    return

@SeuronBot.on_message("show segmentation parameters",
                      description="Upload parameters of the last segmentation",
                      exclusive=False,
                      cancelable=False)
def on_parameters(msg):
    param = get_variable("param", deserialize_json=True)
    upload_param(msg, param)

@SeuronBot.on_message(["update parameters",
                       "please update parameters"],
                      description="Update segmentation/inference parameters",
                      cancelable=False,
                      file_inputs=True)
def on_update_parameters(msg):
    cmd = extract_command(msg)
    json_obj = download_json(msg)
    if json_obj:
        if isinstance(json_obj, list):
            json_obj = json_obj[0]

        run_type = guess_run_type(json_obj)
        if run_type == "seg_run":
            on_update_segmentation_parameters(msg)
        elif run_type == "inf_run":
            on_update_inference_parameters(msg)
        else:
            replyto(msg, "Cannot guess run type from input parameters, please be more specific")

@SeuronBot.on_message(["update segmentation parameters",
                       "please update segmentation parameters"],
                      description="Update segmentation parameters",
                      cancelable=False,
                      file_inputs=True)
def on_update_segmentation_parameters(msg):
    cmd = extract_command(msg)
    if cmd.startswith("please"):
        update_segmentation_param(msg, advanced=True)
    else:
        update_segmentation_param(msg, advanced=False)

@SeuronBot.on_message("update inference parameters",
                      description="Update inference parameters",
                      cancelable=False,
                      file_inputs=True)
def on_update_inference_parameters(msg):
    update_inference_param(msg)

@SeuronBot.on_message("cancel run",
                      description="Cancel the current run, must provide a matching token",
                      exclusive=False,
                      extra_parameters=True)
def on_cancel_run(msg):
    token = get_variable("run_token")
    cmd = extract_command(msg)
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

@SeuronBot.on_message(["run segmentation", "run segmentations"],
                      description="Create segmentation with updated parameters")
def on_run_segmentations(msg):
    global param_updated
    state = latest_dagrun_state("sanity_check")
    if param_updated != 'seg_run':
        replyto(msg, "You have to update the parameters before starting the segmentation")
    elif state != "success":
        replyto(msg, "Sanity check failed, try again")
    else:
        replyto(msg, "Start segmentation")
        param_updated = None
        if visible_messages(broker_url, "seuronbot_payload") == 0:
            run_dag("segmentation")
        else:
            put_message(broker_url, "seuronbot_payload", msg)
            put_message(broker_url, "seuronbot_cmd", "seg_run")

@SeuronBot.on_message(["run inference", "run inferences"],
                      description="Inference with updated parameters")
def on_run_inferences(msg):
    global param_updated
    state = latest_dagrun_state("chunkflow_generator")
    if param_updated != 'inf_run':
        replyto(msg, "You have to update the parameters before starting the inference")
    elif state != "success":
        replyto(msg, "Chunkflow set_env failed, try again")
    else:
        replyto(msg, "Start inference")
        param_updated = None
        if visible_messages(broker_url, "seuronbot_payload") == 0:
            run_dag("chunkflow_worker")
        else:
            put_message(broker_url, "seuronbot_payload", msg)
            put_message(broker_url, "seuronbot_cmd", "inf_run")

@SeuronBot.on_message(["run pipeline"],
                      description="Run pipeline with updated parameters")
def on_run_pipeline(msg):
    if not param_updated:
        replyto(msg, "You have to update the parameters before starting the pipeline")
    elif param_updated == 'inf_run':
        on_run_inferences(msg)
    elif param_updated == 'seg_run':
        on_run_segmentations(msg)
    else:
        replyto(msg, "Do not understand the parameters, please upload them again")


@SeuronBot.on_message("extract contact surfaces",
                      description="Extract the contact surfaces between segments")
def on_extract_contact_surfaces(msg):
    global param_updated
    state = latest_dagrun_state("sanity_check")
    if state != "success":
        replyto(msg, "Sanity check failed, try again")
    else:
        replyto(msg, "Extract contact surfaces")
        param_updated = None
        run_dag("contact_surface")

def update_segmentation_param(msg, advanced=False):
    global param_updated
    json_obj = download_json(msg)
    kw = check_advanced_settings(json_obj)

    if len(kw) > 0 and not advanced:
        replyto(msg, f'You are trying to change advanced parameters: {",".join("`"+x+"`" for x in kw)}')
        replyto(msg, "Use `please update segmentation parameters` to confirm that you know what you are doing!")
        return
    elif len(kw) == 0 and advanced:
        replyto(msg, "You are too polite, do not use `please update segmentation parameters` without any advanced parameters!")
        return

    if json_obj:
        clear_queues()

        if isinstance(json_obj, list):
            if (len(json_obj) > 1):
                replyto(msg, "*{} batch jobs detected, only sanity check the first one for now*".format(len(json_obj)))
            put_message(broker_url, "seuronbot_payload", json_obj)
            json_obj = json_obj[0]

        supply_default_param(json_obj)
        replyto(msg, "Running sanity check, please wait")
        set_variable('param', json_obj, serialize_json=True)
        param_updated = "seg_run"
        run_dag("sanity_check")

    return

@SeuronBot.on_message("update synaptor parameters",
                      description=(
                          "Updates parameters for synaptor segmentation or assignment."
                          " Performs a light sanity check."
                      ),
                      exclusive=True,  # allows metadata update for callbacks
                      file_inputs=True,
                      cancelable=False)
def update_synaptor_params(msg):
    """Parses the synaptor configuration file to check for simple errors."""
    # Current file format is ini/toml, not json
    _, content = download_file(msg)

    def config_to_json(content):
        cp = ConfigParser()
        cp.read_string(content)

        return {
            section: {
                field: cp[section][field] for field in cp[section]
            }
            for section in cp
        }

    if content is not None:  # download_file returns None if there's a problem
        replyto(msg, "Running synaptor sanity check. Please wait.")
        param = config_to_json(content)

        set_variable("synaptor_param.json", param, serialize_json=True)
        run_dag("synaptor_sanity_check")

    else:
        replyto(msg, "Error reading file")

@SeuronBot.on_message("run synaptor file segmentation",
                      description=(
                          "Runs a synaptor segmentation using the file backend."
                      ))
def synaptor_file_seg(msg):
    """Runs the file segmentation DAG."""
    replyto(msg, "Running synaptor file segmentation. Please wait.")
    run_dag("synaptor_file_seg")

@SeuronBot.on_message(["run synaptor db segmentation",
                       "run synaptor database segmentation"],
                      description=(
                          "Runs a synaptor segmentation using the database backend."
                          " NOTE: This requires the user to set up an accessible database."
                      ))
def synaptor_db_seg(msg):
    """Runs the database segmentation DAG."""
    replyto(msg, "Running synaptor file segmentation. Please wait.")
    run_dag("synaptor_db_seg")

@SeuronBot.on_message("run synaptor synapse assignment",
                      description=(
                          "Runs synaptor synapse segmentation and assignment."
                          " NOTE: This requires the user to set up an accessible"
                          " database."
                      ))
def synaptor_assignment(msg):
    """Runs the synapse assignment DAG."""
    replyto(msg, "Running synaptor synapse assignment. Please wait.")
    run_dag("synaptor_assignment")


def supply_default_param(json_obj):
    if not json_obj.get("NAME", ""):
        json_obj["NAME"] = datetime.now().strftime("%Y%m%d%H%M%S")

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


@SeuronBot.on_hello()
def process_hello():
    hello_world()


def hello_world(client=None):
    if not client:
        client = slack.WebClient(token=slack_token)

    host_ip = update_ip_address()

    client.chat_postMessage(
        channel=slack_notification_channel,
        username=workerid,
        text="Hello from <https://{}/airflow/home|{}>".format(host_ip, host_ip))


def handle_batch():
    while True:
        current_task="seg_run"
        logger.debug("check queue")
        time.sleep(1)
        if visible_messages(broker_url, "seuronbot_payload") == 0:
            continue
        if visible_messages(broker_url, "seuronbot_cmd") != 0:
            cmd = get_message(broker_url, "seuronbot_cmd")
            if cmd != "seg_run" and cmd != "inf_run":
                continue
            else:
                current_task = cmd
        else:
            continue

        logger.debug("get message from queue")

        json_obj = get_message(broker_url, "seuronbot_payload")
        msg = get_message(broker_url, "seuronbot_payload")

        if json_obj is None:
            continue

        if (not isinstance(json_obj, list)) or (not isinstance(json_obj[0], dict)):
            replyto(msg, "Batch process expects an array of dicts from the json file")
            continue

        replyto(msg, "Batch jobs will reuse on the parameters from the first job unless new parameters are specified, *including those with default values*")

        default_param = json_obj[0]
        for i, p in enumerate(json_obj):
            if visible_messages(broker_url, "seuronbot_cmd") != 0:
                cmd = get_message(broker_url, "seuronbot_cmd")
                if cmd == "cancel":
                    replyto(msg, "Cancel batch process")
                    break

            if p.get("INHERIT_PARAMETERS", True):
                param = deepcopy(default_param)
            else:
                param = {}

            if i > 0:
                if 'NAME' in param:
                    del param['NAME']
                for k in p:
                    param[k] = p[k]
                supply_default_param(param)
                replyto(msg, "*Sanity check: batch job {} out of {}*".format(i+1, len(json_obj)))
                state = "unknown"
                current_task = guess_run_type(param)
                if current_task == "seg_run":
                    set_variable('param', param, serialize_json=True)
                    state = run_dag("sanity_check", wait_for_completion=True).state
                elif current_task == "inf_run":
                    set_variable('inference_param', param, serialize_json=True)
                    state = run_dag("chunkflow_generator", wait_for_completion=True).state

                if state != "success":
                    replyto(msg, "*Sanity check failed, abort!*")
                    break

            state = "unknown"
            replyto(msg, "*Starting batch job {} out of {}*".format(i+1, len(json_obj)), broadcast=True)
            if current_task == "seg_run":
                state = run_dag('segmentation', wait_for_completion=True).state
            elif current_task == "inf_run":
                state = run_dag("chunkflow_worker", wait_for_completion=True).state

            if state != "success":
                replyto(msg, "*Bach job failed, abort!*")
                break

        replyto(msg, "*Batch process finished*")



if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    seuronbot = SeuronBot(slack_token=slack_token)

    batch = threading.Thread(target=handle_batch)

    batch.start()

    #logger.info("subprocess pid: {}".format(batch.pid))

    seuronbot.start()

    batch.join()
