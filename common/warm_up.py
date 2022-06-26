"""Functions to manage the cluster_min_size Variable for the "warm up" feature."""
from datetime import datetime
from typing import Optional

from airflow.models import Variable

from slack_message import slack_message


# A special timestamp to signal when a warm up period has expired and
# we've already notified the user about it.
SENTINEL = datetime(2000, 1, 1)


def init_timestamp(fmt: bool = True):
    """An initial timestamp that won't cause user notifications downstream.

    timestamp_expired notifies the user if a timestamp expires and it's not
    equal to this timestamp.
    """
    if fmt:
        return SENTINEL.isoformat()
    else:
        return SENTINEL


def get_min_size(cluster_name: str, min_sizes: Optional[dict[str, list]] = None) -> int:
    """Determines the minimum size to enforce on a cluster.

    A minimum size value is considered active if its timestamp hasn't passed.
    The min_sizes argument comes from the cluster_min_size Variable. The user
    can pass this dictionary to reduce the number of database queries.

    Notifies the user if the timestamp is newly expired.

    If there is an error reading the cluster_min_size Variable, this fn returns
    0 as the min_size.
    """
    if min_sizes is None:
        try:
            min_sizes = Variable.get("cluster_min_size", deserialize_json=True)
        except:
            return 0

    if cluster_name not in min_sizes:
        return 0

    min_size, rawtimestamp = min_sizes[cluster_name]
    timestamp = datetime.fromisoformat(rawtimestamp)
    now = datetime.utcnow()

    if now > timestamp:

        # expired, but we haven't notified the user yet
        if timestamp != init_timestamp(fmt=False):
            slack_message(f":ice_cube: warm up period for {cluster_name} has expired")
            min_sizes[cluster_name] = [0, init_timestamp()]
            Variable.set("cluster_min_size", min_sizes, serialize_json=True)

        return 0

    remaining = timestamp - now

    # a minor safeguard for users manually messing with the Variable
    if remaining.days > 0:
        slack_message(
            ":eyes: this is a suspiciously long warm up period."
            f" Did I parse this correctly? {timestamp}"
        )

    else:
        slack_message(
            ":hourglass_flowing_sand:"
            f" {remaining.seconds // 60}m and {remaining.seconds % 60}s remain on"
            f" the {cluster_name} warm up period."
        )

    return min_size
