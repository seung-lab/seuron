from configparser import ConfigParser
from seuronbot import SeuronBot
from airflow_api import set_variable, run_dag
from bot_utils import replyto, extract_command, download_file


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


@SeuronBot.on_message("run synaptor file segmentation",
                      description=(
                          "Runs a synaptor segmentation using the file backend."
                      ))
def synaptor_file_seg(msg):
    """Runs the file segmentation DAG."""
    replyto(msg, "Running synaptor file segmentation. Please wait.")
    run_dag("synaptor_file_seg")


@SeuronBot.on_message(["run synaptor db segmentation",
                       "run synaptor database segmentation"],
                      description=(
                          "Runs a synaptor segmentation using the database backend."
                          " NOTE: This requires the user to set up an accessible database."
                      ))
def synaptor_db_seg(msg):
    """Runs the database segmentation DAG."""
    replyto(msg, "Running synaptor file segmentation. Please wait.")
    run_dag("synaptor_db_seg")


@SeuronBot.on_message("run synaptor synapse assignment",
                      description=(
                          "Runs synaptor synapse segmentation and assignment."
                          " NOTE: This requires the user to set up an accessible"
                          " database."
                      ))
def synaptor_assignment(msg):
    """Runs the synapse assignment DAG."""
    replyto(msg, "Running synaptor synapse assignment. Please wait.")
    run_dag("synaptor_assignment")
