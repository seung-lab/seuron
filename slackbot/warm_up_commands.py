from datetime import datetime, timedelta

from seuronbot import SeuronBot
from bot_utils import replyto
from airflow_api import cluster_exists, get_variable, set_variable, run_dag


@SeuronBot.on_message(
    "warm up",
    description="Keep at least 10 nodes running in a cluster for 1 hour",
    exclusive=True,  # updates "task owner" for slack responses
    cancelable=False,
    extra_parameters=True,
)
def warm_up(msg: dict) -> None:
    """Sets the min size variable for a cluster and runs cluster_management."""

    # bot_utils.extract_command removes punctuation from some cluster names
    def extract_clustername(text):
        cmd = text.split(",")[-1]
        return cmd.replace("warm up", "").strip()

    clustername = extract_clustername(msg["text"])

    if not cluster_exists(clustername):
        replyto(msg, f"cluster {clustername} not found. Please try again.")

    try:
        min_sizes = get_variable('cluster_min_size', deserialize_json=True)
        target_sizes = get_variable('cluster_target_size', deserialize_json=True)

        expiration = (datetime.utcnow() + timedelta(hours=1)).isoformat()

        min_sizes[clustername] = [10, expiration]
        target_sizes[clustername] = target_sizes.get(clustername, 0)

        set_variable("cluster_min_size", min_sizes, serialize_json=True)
        set_variable("cluster_target_size", target_sizes, serialize_json=True)
        replyto(msg, f":fire: cluster {clustername} warming up")

        run_dag("cluster_management")

    except Exception as e:
        replyto(msg, f"Unable to warm up {clustername}: ({type(e)}, {str(e)})")
