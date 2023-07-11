import slack_sdk as slack
from airflow_api import set_variable
from bot_info import slack_token, workerid, slack_notification_channel
from seuronbot import SeuronBot

import os
import logging
import sys
from bot_utils import send_slack_message

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
import weburl_commands


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
    host_ip = update_ip_address()
    msg_payload = {
            'text': f"Hello from <https://{host_ip}/airflow/home|{host_ip}>",
            'notification': True,
    }
    send_slack_message(msg_payload)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    seuronbot = SeuronBot(slack_token=slack_token)
    seuronbot.start()
