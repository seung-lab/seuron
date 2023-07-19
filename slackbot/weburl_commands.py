import os
import re
import subprocess

from seuronbot import SeuronBot
from airflow_api import get_variable
from slackbot.bot_utils import replyto


def extract_jupyterlab_token():
    try:
        command_output = subprocess.check_output(["jupyter", "lab", "list"], encoding="utf-8")
    except FileNotFoundError:
        return None
    pattern = r"token=(\w+)"
    matches = re.findall(pattern, command_output)
    if matches:
        return matches[0]
    return None


@SeuronBot.on_message(["airflow", "show airflow"],
                      description="URL of the Airflow web interface",
                      exclusive=False,
                      cancelable=False,
                      extra_parameters=False)
def on_show_airflow(msg):
    host_ip = get_variable("webui_ip")
    replyto(msg, f"https://{host_ip}/airflow/home")


@SeuronBot.on_message(["grafana", "show grafana"],
                      description="URL of the Granfana web interface",
                      exclusive=False,
                      cancelable=False,
                      extra_parameters=False)
def on_show_grafana(msg):
    host_ip = get_variable("webui_ip")
    replyto(msg, f"https://{host_ip}/grafana")


@SeuronBot.on_message(["jupyter", "jupyterlab", "show jupyterlab"],
                      description="URL of the Jupter Lab instance",
                      exclusive=True,
                      cancelable=False,
                      extra_parameters=False)
def on_show_jupyter(msg):
    if not os.environ.get("ENABLE_JUPYTER_INTERFACE"):
        replyto(msg, "Jupyter interface is not enabled")
        return

    host_ip = get_variable("webui_ip")
    token = extract_jupyterlab_token()
    if token:
        replyto(msg, f"https://{host_ip}/jupyter/lab?token={token}&file-browser-path=/jupyterlab")
    else:
        replyto(msg, "Cannot find a running jupyter lab server")
