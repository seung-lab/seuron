from configparser import ConfigParser
from seuronbot import SeuronBot
from airflow_api import set_variable, run_dag
from bot_utils import replyto, download_file


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
    _, content = download_file(msg)

    def config_to_json(content):
        cp = ConfigParser()
        cp.read_string(content)

        return {
            section: {
                field: cp[section][field] for field in cp[section]
            }
            for section in cp
        }

    if content is not None:  # download_file returns None if there's a problem
        replyto(msg, "Running synaptor sanity check. Please wait.")
        param = config_to_json(content)

        set_variable("synaptor_param.json", param, serialize_json=True)
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
