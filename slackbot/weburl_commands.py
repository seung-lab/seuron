from seuronbot import SeuronBot
from airflow_api import get_variable
from slackbot.bot_utils import replyto


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
