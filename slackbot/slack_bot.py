import slack_sdk as slack
import json
import json5
from collections import OrderedDict
from airflow_api import get_variable, \
    check_running, latest_dagrun_state, set_variable, \
    mark_dags_success, run_dag
from bot_info import slack_token, botid, workerid, broker_url, slack_notification_channel
from kombu_helper import drain_messages, visible_messages, peek_message, get_message, put_message
from bot_utils import fetch_slack_thread, replyto, extract_command, download_file, clear_queues
from seuronbot import SeuronBot
from google_metadata import get_project_data, get_instance_data, get_instance_metadata, set_instance_metadata, gce_external_ip
from warm_up import init_timestamp
from copy import deepcopy
import time
import logging
from secrets import token_hex
from datetime import datetime
import threading
import queue
import sys
import traceback
import concurrent.futures
import time

import update_packages_commands
import redeploy_commands
import cancel_run_commands
import igneous_tasks_commands
import custom_tasks_commands
import synaptor_commands
import pipeline_commands
import heartbeat_commands
import warm_up_commands
import webknossos_commands
import easy_seg_commands
import training_commands


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


def fetch_oom_messages(queue="oom-queue"):
    client = slack.WebClient(token=slack_token)
    while True:
        msg = get_message(broker_url, queue, timeout=30)
        if msg:
            slack_workername, slack_info = fetch_slack_thread()
            slack_username = slack_info["user"]
            client.chat_postMessage(
                username=slack_workername,
                channel=slack_info["channel"],
                thread_ts=slack_info["thread_ts"],
                text=f"<@{slack_username}>, :u6e80: *OOM detected from instance* `{msg}`!"
            )
            time.sleep(1)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    seuronbot = SeuronBot(slack_token=slack_token)
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    f = executor.submit(fetch_oom_messages)
    seuronbot.start()
