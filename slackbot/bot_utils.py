from __future__ import annotations

import re
import string
import requests
import json
import json5
import base64
import traceback
from functools import lru_cache
from collections import OrderedDict
from secrets import token_hex
from kombu_helper import put_message, visible_messages
import slack_sdk as slack
from typing import Optional
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


def send_message(msg_payload, client=None, context=None):
    output_queue = "jupyter-output-queue"
    if msg_payload.get("text", None):
        if visible_messages(broker_url, output_queue) < 100:
            put_message(broker_url, output_queue, msg_payload)

    try:
        send_slack_message(msg_payload, client, context)
    except Exception:
        pass


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
        client.files_upload_v2(
            username=slack_workername,
            channel=slack_channel,
            thread_ts=slack_thread,
            title=attachment['title'],
            content=base64.b64decode(attachment['content']),
            initial_comment=text
        )


def replyto(msg, reply, workername=workerid, broadcast=False):
    msg_payload = {
            "text": reply,
            "broadcast": broadcast,
            "workername": workername,
    }

    if msg.get("from_jupyter", False):
        send_message(msg_payload, context=None)
    else:
        send_message(msg_payload, context=msg)


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
    jupyter_msg = f"use `cancel run {token}` to cancel the current run"
    put_message(broker_url, "jupyter-output-queue", {"text": jupyter_msg})

    try:
        sc = slack.WebClient(slack_token, timeout=300)
        if 'user' in msg:
            userid = msg['user']
        else:
            slack_info = fetch_slack_thread()
            userid = slack_info["user"]

        reply_msg = "use `{}, cancel run {}` to cancel the current run".format(workerid, token)
        sc.chat_postMessage(
            channel=userid,
            text=reply_msg
        )
    except Exception:
        pass


def download_file(msg):
    if "files" in msg:
        # only use the first file:
        file_info = msg["files"][0]

        if file_info.get("file_access", None) == "check_file_info":
            if "url_private_download" not in file_info:
                sc = slack.WebClient(slack_token, timeout=300)
                ret = sc.files_info(file=file_info["id"])
                file_info = ret.data["file"]

        private_url = file_info["url_private_download"]

        filetype = file_info["pretty_type"]
        response = requests.get(private_url, headers={'Authorization': 'Bearer {}'.format(slack_token)})

        if response.status_code == 200:
            return filetype, response.content.decode("ascii", "ignore")
        else:
            return None, None
    elif msg.get("from_jupyter", False) and msg.get("attachment", None):
        attachment = {
            'title': 'Script sent by jupyter',
            'filetype': 'python',
            'content': msg["attachment"],
        }
        msg_payload = {
            "text": "Use python script from jupyter cell",
            "attachment": attachment,
        }
        send_message(msg_payload)
        return "python", base64.b64decode(attachment["content"].encode("utf-8")).decode("utf-8")
    else:
        replyto(msg, "You need to upload a parameter file with this message")
        return None, None


def download_json(msg):
    filetype, content = download_file(msg)
    if not content:
        return None
    if filetype.lower() == "python":
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
    send_message(msg_payload, context=msg)


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
        r".* @\s*"
        r"\(?\[?"  # noqa
        r"([0-9]+),?"
        r" ([0-9]+),?"
        r" ([0-9]+),?"
        r" ([0-9]+),?"
        r" ([0-9]+),?"
        r" ([0-9]+)"
        r"\)?\]?"  # noqa
        r"[ \n]*"
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
        r".* @ "
        r"\(?\[?"  # noqa
        r"([0-9]+),?"
        r" ([0-9]+),?"
        r" ([0-9]+)"
        r"\)?\]?"  # noqa
        r"[ \n]*"
    )
    rematch = regexp.match(msgtext)
    if rematch:
        coords = tuple(map(int, rematch.groups()))
    else:
        raise ValueError("unable to match text")
        return

    return coords


def bbox_and_center(
    defaults: dict,
    bbox: Optional[tuple[tuple[int, int, int], tuple[int, int, int]]] = None,
    center_pt: Optional[tuple[int, int, int]] = None,
    resample: bool = True,
) -> tuple[tuple[int, int, int, int, int, int], tuple[int, int, int]]:
    assert bbox is not None or center_pt is not None, (
        "either bbox or center_pt needs to be filled in"
    )

    if center_pt is None:
        bboxsz = (bbox[3] - bbox[0], bbox[4] - bbox[1], bbox[5] - bbox[2])

        center_pt = (
            bbox[0] + bboxsz[0] // 2,
            bbox[1] + bboxsz[1] // 2,
            bbox[2] + bboxsz[2] // 2,
        )

    if bbox is None:
        assert "bbox_width" in defaults, "no bbox and default box width"
        bbox_width = defaults["bbox_width"]
        bboxsz = tuple(width * 2 for width in bbox_width)

        bbox = (
            center_pt[0] - bbox_width[0],
            center_pt[1] - bbox_width[1],
            center_pt[2] - bbox_width[2],
            center_pt[0] + bbox_width[0],
            center_pt[1] + bbox_width[1],
            center_pt[2] + bbox_width[2],
        )

    maxsz = defaults.get("max_bbox_width", None)
    if maxsz is not None and any(sz > mx for sz, mx in zip(bboxsz, maxsz)):
        raise ValueError(f"bounding box is too large: max size = {maxsz}")

    # resampling coordinates to data resolution
    if resample and "index_resolution" in defaults and "data_resolution" in defaults:
        assert isinstance(defaults["index_resolution"], list)
        assert isinstance(defaults["data_resolution"], list)

        center_pt = tuple(
            int(c * ir / dr)
            for c, ir, dr in zip(
                center_pt, defaults["index_resolution"], defaults["data_resolution"]
            )
        )

        bbox = tuple(
            int(b * ir / dr)
            for b, ir, dr in zip(
                bbox,
                defaults["index_resolution"] + defaults["index_resolution"],
                defaults["data_resolution"] + defaults["data_resolution"],
            )
        )

    return bbox, center_pt
