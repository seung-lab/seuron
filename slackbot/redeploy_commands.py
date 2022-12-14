import sys
import time
import json
import slack_sdk as slack
from airflow.hooks.base_hook import BaseHook
from seuronbot import SeuronBot
from bot_utils import replyto, fetch_slack_thread
from bot_info import slack_token
from google_metadata import get_project_data, get_instance_data, get_instance_metadata, set_instance_metadata
import tenacity


@SeuronBot.on_message("redeploy docker stack",
                      description="Restart the manager stack with updated docker images",
                      cancelable=False)
def on_redeploy_docker_stack(msg):
    replyto(msg, "Redeploy seuronbot docker stack on the bootstrap node")
    set_redeploy_flag(True)
    time.sleep(300)
    replyto(msg, "Failed to restart the bot")


@SeuronBot.on_hello()
def bot_restarted():
    if get_instance_data("attributes/redeploy") == 'true':
        set_redeploy_flag(False)
        send_reset_message()


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


@tenacity.retry(
            reraise=True,
            stop=tenacity.stop_after_attempt(10),
            wait=tenacity.wait_random_exponential(multiplier=0.5, max=60.0),
)
def send_reset_message():
    slack_workername, slack_extra = fetch_slack_thread()

    client = slack.WebClient(token=slack_token)
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
