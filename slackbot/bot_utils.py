import string
import requests
import json
import json5
import base64
import traceback
from functools import lru_cache
from collections import OrderedDict
from secrets import token_hex
import slack_sdk as slack
from airflow.hooks.base_hook import BaseHook
from bot_info import slack_token, botid, workerid, broker_url, slack_notification_channel
from airflow_api import update_slack_connection, set_variable
from kombu_helper import drain_messages, peek_message


def clear_queues():
    drain_messages(broker_url, "seuronbot_payload")
    drain_messages(broker_url, "seuronbot_cmd")


@lru_cache(maxsize=1)
def extract_command(text):
    cmd = text.replace(workerid, "").replace(botid, "")
    cmd = cmd.translate(str.maketrans('', '', string.punctuation))
    cmd = cmd.lower()
    return "".join(cmd.split())


def send_slack_message(msg_payload, client=None, context=None):
    if client is None:
        client = slack.WebClient(slack_token, timeout=300)

    slack_info = fetch_slack_thread()

    if context:
        slack_info = context

    if "workername" in msg_payload:
        slack_workername = msg_payload["workername"]
    else:
        slack_workername = workerid

    if msg_payload.get("notification", False):
        text = msg_payload["text"]
        slack_channel = slack_notification_channel
        slack_thread = None
    else:
        text = f"<@{slack_info['user']}>, {msg_payload['text']}"
        slack_channel = slack_info["channel"]
        slack_thread = slack_info.get("thread_ts", slack_info.get("ts", None))

    if msg_payload.get("attachment", None) is None:
        client.chat_postMessage(
            username=slack_workername,
            channel=slack_channel,
            thread_ts=slack_thread,
            reply_broadcast=msg_payload.get("broadcast", False),
            text=text
        )
    else:
        attachment = msg_payload["attachment"]
        client.files_upload(
            username=slack_workername,
            channels=slack_channel,
            thread_ts=slack_thread,
            title=attachment['title'],
            filetype=attachment['filetype'],
            content=base64.b64decode(attachment['content']),
            initial_comment=text
        )


def replyto(msg, reply, workername=workerid, broadcast=False):
    msg_payload = {
            "text": reply,
            "broadcast": broadcast,
            "workername": workername,
    }
    send_slack_message(msg_payload, context=msg)


def update_slack_thread(msg):
    payload = {
        'user': msg['user'],
        'channel': msg['channel'],
        'thread_ts': msg['thread_ts'] if 'thread_ts' in msg else msg['ts']
    }
    update_slack_connection(payload, slack_token)


def fetch_slack_thread():
    SLACK_CONN_ID = "Slack"
    slack_extra = json.loads(BaseHook.get_connection(SLACK_CONN_ID).extra)
    return slack_extra


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
    attachment = {
            "title": "Pipeline parameters",
            "filetype": "javascript",
            "content": base64.b64encode(json.dumps(param, indent=4).encode("utf-8")).decode("utf-8"),
            }
    msg_payload = {
            "text": "current parameters",
            "attachment": attachment,
    }
    send_slack_message(msg_payload, context=msg)


def guess_run_type(param):
    if "WORKER_IMAGE" in param:
        return "seg_run"
    elif "CHUNKFLOW_IMAGE" in param:
        return "inf_run"
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
