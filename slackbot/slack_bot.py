import slack
import json
import json5
from collections import OrderedDict
import string
from airflow_api import get_variable, run_segmentation, \
    update_slack_connection, check_running, dag_state, set_variable, \
    sanity_check, chunkflow_set_env, run_inference, run_contact_surface, \
    mark_dags_success, run_dag
from bot_info import slack_token, botid, workerid
from copy import deepcopy
import requests
import re
import time
import logging
from secrets import token_hex
import threading
import queue
from queue import Empty

param_updated = False


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


def gcloud_ip():
    metadata_url = "http://169.254.169.254/computeMetadata/v1/instance/network-interfaces/0/access-configs/0/external-ip"
    response = requests.get(metadata_url, headers={"Metadata-Flavor": "Google"})

    if response.status_code == 200:
        return response.content.decode("ascii", "ignore")
    else:
        return "Unknown ip address"

def filter_msg(msg):
    if 'subtype' in msg and msg['subtype'] != "thread_broadcast":
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


def extract_command(msg):
    cmd = msg["text"].replace(workerid, "").replace(botid, "")
    cmd = cmd.translate(str.maketrans('', '', string.punctuation))
    cmd = cmd.lower().replace(" ", "")
    return cmd


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


def cancel_run(msg):
    replyto(msg, "Marking all DAG states to success...")
    dags = ['segmentation','watershed','agglomeration', 'chunkflow_worker', 'chunkflow_generator']
    mark_dags_success(dags)
    time.sleep(10)
    #try again because some tasks might already been scheduled
    dags = ['segmentation','watershed','agglomeration', 'chunkflow_worker', 'chunkflow_generator']
    mark_dags_success(dags)

    replyto(msg, "Shutting down clusters...")
    cluster_size = get_variable('cluster_target_size', deserialize_json=True)
    for k in cluster_size:
        cluster_size[k] = 0
    set_variable("cluster_target_size", cluster_size, serialize_json=True)
    run_dag("cluster_management")

    replyto(msg, "*Current run cancelled*", broadcast=True)


def upload_param(msg):
    sc = slack.WebClient(slack_token, timeout=300)
    param = get_variable("param", deserialize_json=True)
    channel = msg['channel']
    userid = msg['user']
    sc.files_upload(
        channels=channel,
        filename="param.json",
        filetype="javascript",
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
        return None
    else:
        time.sleep(2)
        # only use the first file:
        file_info = msg["files"][0]
        private_url = file_info["url_private_download"]
        response = requests.get(private_url, headers={'Authorization': 'Bearer {}'.format(slack_token)})

        if response.status_code == 200:
            return response.content.decode("ascii", "ignore")
        else:
            return None


def update_inference_param(msg):
    global param_updated
    payload = download_file(msg)
    if payload:
        try:
            json_obj = json5.loads(payload, object_pairs_hook=OrderedDict)
        except (ValueError, TypeError) as e:
            replyto(msg, "Cannot load the json file: {}".format(str(e)))
            print(payload)
            return

        if not check_running():
            #try:
            #    q_payload.get_nowait()
            #except Empty:
            #    pass

            #if isinstance(json_obj, list):
            #    replyto(msg, "*{} batch jobs detected, only sanity check the first one for now*".format(len(json_obj)))
            #    json_obj = json_obj[0]
            #    q_payload.put(msg)

            supply_default_param(json_obj)
            replyto(msg, "Running chunkflow setup_env, please wait")
            update_metadata(msg)
            set_variable('inference_param', json_obj, serialize_json=True)
            chunkflow_set_env()
            param_updated = True
        else:
            replyto(msg, "Busy right now")

    return


def update_param(msg):
    global param_updated
    payload = download_file(msg)
    if payload:
        try:
            json_obj = json5.loads(payload, object_pairs_hook=OrderedDict)
        except (ValueError, TypeError) as e:
            replyto(msg, "Cannot load the json file: {}".format(str(e)))
            print(payload)
            return

        if not check_running():
            clear_queues()

            if isinstance(json_obj, list):
                if (len(json_obj) > 1):
                    replyto(msg, "*{} batch jobs detected, only sanity check the first one for now*".format(len(json_obj)))
                json_obj = json_obj[0]
                q_payload.put(msg)

            supply_default_param(json_obj)
            replyto(msg, "Running sanity check, please wait")
            update_metadata(msg)
            set_variable('param', json_obj, serialize_json=True)
            sanity_check()
            param_updated = True
        else:
            replyto(msg, "Busy right now")

    return

def supply_default_param(json_obj):
    if "NAME" not in json_obj:
        json_obj["NAME"] = token_hex(16)

    if "SCRATCH_PREFIX" not in json_obj and "SCRATCH_PATH" not in json_obj:
        json_obj["SCRATCH_PREFIX"] = "gs://ranl_pipeline_scratch/"

    for p in ["WS","SEG"]:
        if "{}_PREFIX".format(p) not in json_obj and "{}_PATH".format(p) not in json_obj:
            json_obj["{}_PREFIX".format(p)] = json_obj.get("NG_PREFIX", "gs://ng_scratch_ranl/make_cv_happy/") + p.lower() + "/"


def dispatch_command(cmd, payload):
    global param_updated
    msg = payload['data']
    print(cmd)
    if cmd == "parameters":
        upload_param(msg)
    elif cmd == "updateparameters":
        update_param(msg)
    elif cmd == "updateinferenceparameters":
        update_inference_param(msg)
    elif cmd.startswith("cancelrun"):
        token = get_variable("run_token")
        if cmd != "cancelrun"+token:
            replyto(msg, "Wrong token")
        else:
            if check_running():
                clear_queues()
                q_cmd.put("cancel")
                cancel_run(msg)
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
    elif cmd == "runinference":
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
            run_inference()
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
    else:
        replyto(msg, "Sorry I do not understand, please try again.")


@slack.RTMClient.run_on(event='message')
def process_message(**payload):
    m = payload['data']
    print(json.dumps(m, indent=4))
    if filter_msg(m):
        cmd = extract_command(m)
        dispatch_command(cmd, payload)

@slack.RTMClient.run_on(event='reaction_added')
def process_reaction(**payload):
    print("reaction added")
    m = payload['data']
    print(json.dumps(m, indent=4))


@slack.RTMClient.run_on(event='hello')
def hello_world(**payload):
    client = slack.WebClient(token=slack_token)

    host_ip = gcloud_ip()
    set_variable("webui_ip", host_ip)

    client.chat_postMessage(
        channel='#seuron-alerts',
        username=workerid,
        text="Hello world from {}!".format(host_ip))


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
        msg = q_payload.get()

        payload = download_file(msg)
        msg = q_payload.get()

        if payload is None:
            continue
        try:
            json_obj = json5.loads(payload, object_pairs_hook=OrderedDict)
        except (ValueError, TypeError) as e:
            replyto(msg, "Cannot load the json file: {}".format(str(e)))

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
                replyto(msg, "*Segmentation failed, abort!*")
                break

        replyto(msg, "*Batch process finished*")

def wait_for_airflow():
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

    hello_world()
    batch.start()
    #logger.info("subprocess pid: {}".format(batch.pid))

    rtmclient = slack.RTMClient(token=slack_token)
    rtmclient.start()

    batch.join()
