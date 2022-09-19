from seuronbot import SeuronBot
from bot_utils import replyto, download_file
from bot_info import broker_url
from kombu_helper import drain_messages
from airflow_api import set_variable, run_dag

@SeuronBot.on_message(["run igneous task", "run igneous tasks"],
                      description="Run igneous tasks defined in the uploaded script",
                      file_inputs=True)
def on_run_igneous_tasks(msg):
    run_igneous_scripts(msg)


def run_igneous_scripts(msg):
    _, payload = download_file(msg)
    if payload:
        drain_messages(broker_url, "igneous")
        drain_messages(broker_url, "igneous_ret")
        drain_messages(broker_url, "igneous_err")
        set_variable('igneous_script', payload)
        replyto(msg, "Execute `submit_tasks` function")
        run_dag("igneous")

    return
