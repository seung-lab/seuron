from seuronbot import SeuronBot
from bot_utils import replyto, download_file
from bot_info import broker_url
from kombu_helper import drain_messages
from airflow_api import set_variable, run_dag

@SeuronBot.on_message(["run custom cpu task", "run custom cpu tasks"],
                      description="Run custom cpu tasks defined in the uploaded script",
                      file_inputs=True)
def on_run_custom_cpu_tasks(msg):
    run_custom_scripts(msg, "cpu")

@SeuronBot.on_message(["run custom gpu task", "run custom gpu tasks"],
                      description="Run custom gpu tasks defined in the uploaded script",
                      file_inputs=True)
def on_run_custom_gpu_tasks(msg):
    run_custom_scripts(msg, "gpu")


def run_custom_scripts(msg, task_type):
    _, payload = download_file(msg)
    if payload:
        for t in ['gpu', 'cpu']:
            drain_messages(broker_url, f"custom-{t}")
            drain_messages(broker_url, f"custom-{t}_ret")
            drain_messages(broker_url, f"custom-{t}_err")
        set_variable('custom_script', payload)
        replyto(msg, "Execute `submit_tasks` function")
        run_dag(f"custom-{task_type}")

    return
