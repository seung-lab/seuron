from seuronbot import SeuronBot
from bot_utils import replyto, extract_command, clear_queues
from airflow_api import set_is_paused


@SeuronBot.on_message(["enable heartbeat check", "disable heartbeat check"],
                      description="Enable/disable heartbeat check, workers not sending heartbeats will be shutdown if heartbeat check is enabled",
                      exclusive=False,
                      cancelable=False,
                      extra_parameters=False)
def on_heartbeat_check(msg):
    cmd = extract_command(msg["text"])
    if cmd.startswith("disable"):
        set_is_paused("pipeline_heartbeat", True)
        replyto(msg, "Heartbeat check disabled")
    else:
        set_is_paused("pipeline_heartbeat", False)
        replyto(msg, "Heartbeat check enabled")
