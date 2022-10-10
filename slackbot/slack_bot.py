import slack_sdk as slack
import json
import json5
from collections import OrderedDict
from airflow_api import get_variable, \
    check_running, latest_dagrun_state, set_variable, \
    mark_dags_success, run_dag
from bot_info import slack_token, botid, workerid, broker_url, slack_notification_channel
from kombu_helper import drain_messages, visible_messages, peek_message, get_message, put_message
from bot_utils import replyto, extract_command, download_file, clear_queues
from seuronbot import SeuronBot
from google_metadata import get_project_data, get_instance_data, get_instance_metadata, set_instance_metadata, gce_external_ip
from copy import deepcopy
import time
import logging
from secrets import token_hex
from datetime import datetime
import threading
import queue
import sys
import traceback

import update_packages_commands
import redeploy_commands
import cancel_run_commands
import igneous_tasks_commands
import custom_tasks_commands
import synaptor_commands
import pipeline_commands


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



if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    seuronbot = SeuronBot(slack_token=slack_token)

    seuronbot.start()
