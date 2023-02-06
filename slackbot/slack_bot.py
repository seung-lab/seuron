import slack_sdk as slack
from airflow_api import set_variable
from bot_info import slack_token, workerid, broker_url, slack_notification_channel
from kombu_helper import get_message
from bot_utils import fetch_slack_thread
from seuronbot import SeuronBot

import os
import time
import logging
import sys
import concurrent.futures

if os.environ.get("VENDOR", None) == "Google":
    from google_metadata import gce_external_ip
else:
    import socket

import update_packages_commands
if os.environ.get("VENDOR", None) == "Google":
    import redeploy_commands
import cancel_run_commands
import igneous_tasks_commands
import custom_tasks_commands
import synaptor_commands
import pipeline_commands
import heartbeat_commands


def update_ip_address():
    if os.environ.get("VENDOR", None) == "Google":
        host_ip = gce_external_ip()
    else:
        hostname = socket.gethostname()
        host_ip = socket.gethostbyname(hostname)
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


def fetch_oom_messages(queue="worker-message-queue"):
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
                text=f"<@{slack_username}>, {msg}"
            )
            time.sleep(1)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    seuronbot = SeuronBot(slack_token=slack_token)
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    f = executor.submit(fetch_oom_messages)
    seuronbot.start()
