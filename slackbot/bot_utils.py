import re
import string
import requests
import json
import json5
from collections import OrderedDict
from secrets import token_hex
import slack_sdk as slack
from airflow.hooks.base_hook import BaseHook
from bot_info import slack_token, botid, workerid, broker_url
from airflow_api import update_slack_connection, set_variable
from kombu_helper import drain_messages, peek_message


def clear_queues():
    drain_messages(broker_url, "seuronbot_payload")
    drain_messages(broker_url, "seuronbot_cmd")


def extract_command(msg):
    cmd = msg["text"].replace(workerid, "").replace(botid, "")
    cmd = cmd.translate(str.maketrans('', '', string.punctuation))
    cmd = cmd.lower()
    return "".join(cmd.split())


def replyto(msg, reply, username=workerid, broadcast=False):
    sc = slack.WebClient(slack_token, timeout=300)
    channel = msg['channel']
    userid = msg['user']
    thread_ts = msg['thread_ts'] if 'thread_ts' in msg else msg['ts']
    reply_msg = f"<@{userid}> {reply}"
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


def update_slack_thread(msg):
    payload = {
        'user': msg['user'],
        'channel': msg['channel'],
        'thread_ts': msg['thread_ts'] if 'thread_ts' in msg else msg['ts']
    }
    update_slack_connection(payload, slack_token)


def fetch_slack_thread():
    SLACK_CONN_ID = "Slack"
    slack_workername = BaseHook.get_connection(SLACK_CONN_ID).login
    slack_extra = json.loads(BaseHook.get_connection(SLACK_CONN_ID).extra)
    return slack_workername, slack_extra


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
        #upload_param(msg, payloads)
        return payloads
    else: #if filetype == "JavaScript/JSON":
        try:
            json_obj = json5.loads(content, object_pairs_hook=OrderedDict)
        except (ValueError, TypeError) as e:
            replyto(msg, "Cannot load the json file: {}".format(str(e)))
            return None
        return json_obj


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


def guess_run_type(param):
    if "WORKER_IMAGE" in param:
        return "seg_run"
    elif "CHUNKFLOW_IMAGE" in param:
        return "inf_run"
    elif "Workflow" in param and "synaptor_image" in param["Workflow"]:
        return "syn_run"
    else:
        return None


def latest_param_type():
    json_obj = peek_message(broker_url, "seuronbot_payload")
    if isinstance(json_obj, list):
        param = json_obj[0]
    else:
        param = json_obj

    if param:
        return guess_run_type(param)
    else:
        return None


def extract_bbox(msgtext: str) -> tuple[int, int, int, int, int, int]:
    """Extracts a bounding box of coordinates from a message's text."""
    regexp = re.compile(
        ".* @ "
        "\(?\[?"  # noqa
        "([0-9]+),?"
        " ([0-9]+),?"
        " ([0-9]+),?"
        " ([0-9]+),?"
        " ([0-9]+),?"
        " ([0-9]+)"
        "\)?\]?"  # noqa
        "[ \n]*"
    )
    rematch = regexp.match(msgtext)
    if rematch:
        coords = tuple(map(int, rematch.groups()))
    else:
        raise ValueError("unable to match text")
        return

    return coords


def extract_point(msgtext: str) -> tuple[int, int, int]:
    """Extracts a point of coordinates from the end of a message's text."""
    regexp = re.compile(
        ".* @ "
        "\(?\[?"  # noqa
        "([0-9]+),?"
        " ([0-9]+),?"
        " ([0-9]+)"
        "\)?\]?"  # noqa
        "[ \n]*"
    )
    rematch = regexp.match(msgtext)
    if rematch:
        coords = tuple(map(int, rematch.groups()))
    else:
        raise ValueError("unable to match text")
        return

    return coords
