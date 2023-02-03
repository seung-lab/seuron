from __future__ import annotations

from seuronbot import SeuronBot
from airflow_api import get_variable, set_variable, run_dag
from bot_utils import replyto, download_json


@SeuronBot.on_message("update webknossos parameters",
                      description=(
                          "Updates parameters for webknossos cutouts and export."
                          " Performs a light sanity check."
                      ),
                      exclusive=True,  # allows metadata update for callbacks
                      file_inputs=True,
                      cancelable=False)
def update_webknossos_params(msg) -> None:
    """Parses the provided parameters to check for simple errors."""
    json_obj = download_json(msg)

    if json_obj:
        replyto(msg, "Running wktools sanity check. Please wait.")
        set_variable("webknossos_param", json_obj, serialize_json=True)

        run_dag("wkt_sanity_check")
    else:
        replyto(msg, "Error reading file")


@SeuronBot.on_message("make a cutout",
                      description=(
                          "Creates a new webknossos task from a CloudVolume cutout."
                      ),
                      exclusive=True,  # allows metadata update for callbacks
                      extra_parameters=True)
def make_cutout_task(msg) -> None:
    try:
        bbox = extract_bbox(msg["text"])
    except Exception as e:
        replyto(msg, f"Error parsing message: {e}")
        return

    param = get_variable("webknossos_param", deserialize_json=True)
    param["bbox_begin"] = f"{bbox[0]} {bbox[1]} {bbox[2]}"
    param["bbox_end"] = f"{bbox[3]} {bbox[4]} {bbox[5]}"
    set_variable("webknossos_param", param, serialize_json=True)

    replyto(msg, "Running cutout task")
    run_dag("wkt_cutouts")


def extract_bbox(msgtext: str) -> tuple[tuple[int, int, int], tuple[int, int, int]]:
    """Extracts a bounding box of coordinates from the message text."""
    regexp = re.compile(
        ".* "
        "\(?\[?"  # noqa
        "([0-9]+),?"
        " ([0-9]+),?"
        " ([0-9]+),?"
        " ([0-9]+),?"
        " ([0-9]+),?"
        " ([0-9]+)"
        "\)?\]?"  # noqa
        "[ \n]*"
    )
    rematch = regexp.match(msgtext)
    if rematch:
        coords = tuple(map(int, rematch.groups()))
    else:
        raise ValueError("unable to match text")
        return

    return (coords[:3], coords[-3:])


@SeuronBot.on_message("update cutout source",
                      description="Updates the CloudVolume to use for cutouts",
                      exclusive=False,
                      extra_parameters=True)
def update_source(msg) -> None:
    try:
        cvpath = extract_cvpath(msg["text"])
    except Exception as e:
        replyto(msg, f"Error parsing message: {e}")
        return

    param = get_variable("webknossos_param", deserialize_json=True)

    if cvpath.lower() == "none":
        del param["src_path"]
        set_variable("webknossos_param", param, serialize_json=True)
        replyto(msg, "Cleared cutout source")
    else:
        param["src_path"] = cvpath
        set_variable("webknossos_param", param, serialize_json=True)
        replyto(msg, f"Set cutout source to: `{cvpath}`")


def extract_cvpath(msgtext: str) -> str:
    """Extracts a CloudVolume path from the end of a message.

    Removes hyperlink formatting.
    """
    raw = msgtext.split(" ")[-1].strip()

    while raw.startswith("<") and raw.endswith(">"):
        raw = raw[1:-1]

    return raw


@SeuronBot.on_message("export annotations",
                      description="Exports annotations from webknossos to a Zettaset.",
                      exclusive=True,
                      extra_parameters=True)
def export_annotations(msg) -> None:
    try:
        annotation_ids = extract_annotation_ids(msg["text"])
    except Exception as e:
        replyto(msg, f"Error parsing message: {e}")
        return

    param = get_variable("webknossos_param", deserialize_json=True)
    param["annotation_ids"] = " ".join(annotation_ids)
    set_variable("webknossos_param", param, serialize_json=True)

    replyto(msg, "Running export")
    run_dag("wkt_export")


def extract_annotation_ids(msgtext: str) -> list[str]:
    """Extracts annotation ids from an export command."""
    words = msgtext.split()
    assert "annotations" in words or "annotation" in words
    if "annotation" in words:
        i = words.index("annotation")
    else:
        i = words.index("annotations")

    return [word.replace(",", "") for word in words[i + 1:]]
