from configparser import ConfigParser
from seuronbot import SeuronBot
from airflow_api import set_variable, run_dag
from bot_info import broker_url
from bot_utils import replyto, download_file, download_json
from kombu_helper import put_message


@SeuronBot.on_message("update synaptor parameters",
                      description=(
                          "Updates parameters for synaptor segmentation or assignment."
                          " Performs a light sanity check."
                      ),
                      exclusive=True,  # allows metadata update for callbacks
                      file_inputs=True,
                      cancelable=False)
def update_synaptor_params(msg):
    """Parses the synaptor configuration file to check for simple errors."""
    # Current file format is ini/toml, not json

    def config_to_json(content):
        cp = ConfigParser()
        cp.read_string(content)

        return {
            section: {
                field: cp[section][field] for field in cp[section]
            }
            for section in cp
        }

    content = download_json(msg)
    if content:
        param = content
    else:
        _, content = download_file(msg)
        param = config_to_json(content)

    if param is not None:  # download_file returns None if there's a problem
        replyto(msg, "Running synaptor sanity check. Please wait.")
        set_variable("synaptor_param.json", param, serialize_json=True)
        put_message(broker_url, "seuronbot_payload", param)
        run_dag("synaptor_sanity_check")

    else:
        replyto(msg, "Error reading file")


@SeuronBot.on_message("run synaptor",
                      description="Runs a synaptor workflow",
                      exclusive=True,
                      cancelable=True,
                      )
def run_synaptor(msg):
    """Runs the file segmentation DAG."""
    replyto(msg, "Running synaptor. Please wait.")
    run_dag("synaptor")
