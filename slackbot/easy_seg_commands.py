"""Non-technical user segmentation commands ("easy seg")."""
from __future__ import annotations

import os
import re
from typing import Any
from datetime import datetime

from seuronbot import SeuronBot
from bot_info import broker_url
from bot_utils import clear_queues, replyto, download_json
from bot_utils import extract_bbox, extract_point, bbox_and_center
from kombu_helper import put_message
from airflow_api import get_variable, set_variable, run_dag
from pipeline_commands import handle_batch, supply_default_param
from warm_up_commands import warm_up


@SeuronBot.on_message("update easy seg parameters",
                      description="Update the default parameters for easy-seg",
                      exclusive=True,
                      file_inputs=True)
def update_easy_seg(msg: dict) -> None:
    json_obj = download_json(msg)

    if json_obj:
        try:
            initial_sanity_check(json_obj)
        except Exception as e:
            replyto(msg, f"Error parsing parameters: {e}")

        set_variable("easy_seg_defaults", json_obj, serialize_json=True)
        replyto(msg, "Parameters successfully updated")

    else:
        replyto(msg, "No json found")


def initial_sanity_check(json_obj: dict) -> None:
    assert (
        "chunkflow" in json_obj
        and (
            "abiss" in json_obj
            or "synaptor" in json_obj
        )
    ), "No top-level structure ('chunkflow' and ['abiss','synaptor'])"

    assert "bbox_width" in json_obj, "No bbox width in json"

    sanity_check_chunkflow(json_obj["chunkflow"])

    if "abiss" in json_obj:
        sanity_check_abiss(json_obj["abiss"])

    if "synaptor" in json_obj:
        sanity_check_synaptor(json_obj["synaptor"], json_obj["bbox_width"])


def sanity_check_one_level(task, json_obj, args):
    unfilled = [arg for arg in args if arg not in json_obj]
    assert len(unfilled) == 0, f"required {task} arguments: {unfilled}"


def sanity_check_chunkflow(json_obj):
    reqd_chunkflow_args = [
        "IMAGE_PATH",
        "IMAGE_RESOLUTION",
        "CHUNKFLOW_IMAGE",
        "INPUT_PATCH_SIZE",
        "OUTPUT_PREFIX",
    ]

    sanity_check_one_level("chunkflow", json_obj, reqd_chunkflow_args)

    assert "OUTPUT_PATH" not in json_obj, "supply output prefix instead of path"


def sanity_check_abiss(json_obj):
    reqd_abiss_args = [
        "WS_HIGH_THRESHOLD",
        "WS_LOW_THRESHOLD",
        "WS_SIZE_THRESHOLD",
        "AGG_THRESHOLD",
        "WORKER_IMAGE",
    ]

    sanity_check_one_level("abiss", json_obj, reqd_abiss_args)


def sanity_check_synaptor(json_obj, bbox_width):
    # synaptor uses a two-level json, so checking the json requires
    # a bit more structure
    def sanity_check_synaptor_level(level, args):
        assert level in json_obj
        sanity_check_one_level(f"synaptor::{level}", json_obj[level], args)

    sanity_check_synaptor_level("Dimensions", ["chunkshape", "blockshape"])
    sanity_check_synaptor_level("Parameters", ["ccthresh", "szthresh"])
    sanity_check_synaptor_level("Workflow", ["synaptor_image"])
    sanity_check_synaptor_level("Provenance", ["motivation"])

    assert all(
        (2 * w) % s == 0
        for w, s in zip(bbox_width, json_obj["Dimensions"]["chunkshape"])
    ), (
        f"synaptor chunkshape ({json_obj['Dimensions']['chunkshape']})"
        f" doesn't match bbox width ({bbox_width})"
    )
    assert all(
        c % b == 0
        for c, b in zip(
            json_obj["Dimensions"]["chunkshape"], json_obj["Dimensions"]["chunkshape"]
        )
    ), (
        f"synaptor chunkshape ({json_obj['Dimensions']['chunkshape']})"
        " doesn't match synaptor blockshape width "
        f" ({json_obj['Dimensions']['blockshape']})"
    )


@SeuronBot.on_message("run easy seg",
                      description=(
                          "Run inference in a bounding box with pre-set defaults"
                      ),
                      exclusive=True,
                      extra_parameters=True,
                      cancelable=True)
def run_easy_seg(msg: dict) -> None:
    try:
        model = extract_model(msg["text"])
    except Exception as e:
        replyto(msg, f"Error parsing model: {e}")
        return

    try:
        bbox = extract_bbox(msg["text"])
        center_pt = None  # will be filled in later (bbox_and_center)
    except Exception as bbox_e:
        bbox = None  # will be filled in later (bbox_and_center)

        try:
            center_pt = extract_point(msg["text"])
        except Exception as pt_e:
            replyto(msg, f"Errors parsing bbox: {bbox_e}, {pt_e}")
            return

    defaults = get_variable("easy_seg_defaults", deserialize_json=True)

    try:
        bbox, center_pt = bbox_and_center(defaults, bbox, center_pt)
    except Exception as e:
        replyto(msg, f"Errors creating bbox: {e}")
        return

    replyto(
        msg,
        f"Running easy seg with\n"
        f"model: {model}\n"
        f"bbox: {bbox}\n"
        f"center: {center_pt}"
    )

    inf_params, seg_params = populate_parameters(model, bbox, defaults)
    clear_queues()  # empties the seuronbot_payload batch queue
    put_message(broker_url, "seuronbot_payload", [inf_params, seg_params])

    warm_up_clusters(defaults, msg)

    # sanity checking inference params (handle_batch skips the first sanity check)
    supply_default_param(inf_params)
    replyto(msg, "Running chunkflow setup_env")
    set_variable("inference_param", inf_params, serialize_json=True)
    set_variable("easy_seg_param", [inf_params, seg_params], serialize_json=True)
    state = run_dag("chunkflow_generator", wait_for_completion=True).state

    if state != "success":
        replyto(msg, "chunkflow check failed")
        return

    handle_batch("inf_run", msg)


def populate_parameters(
    model: str, bbox: tuple[int, int, int, int, int, int], defaults: dict
) -> tuple[dict, dict]:
    inf_params = defaults["chunkflow"]
    inf_params["NAME"] = f"{datetime.now().strftime('%Y%m%d%H%M%S')}/inference"
    inf_params["ONNX_MODEL_PATH"] = model
    inf_params["BBOX"] = bbox

    output_path = os.path.join(inf_params["OUTPUT_PREFIX"], inf_params["NAME"])

    if "abiss" in defaults:
        seg_params = defaults["abiss"]
        seg_params["BBOX"] = bbox
        seg_params["IMAGE_PATH"] = inf_params["IMAGE_PATH"]
        seg_params["AFF_PATH"] = output_path
        seg_params["WS_PATH"] = seg_params['AFF_PATH'].replace("inference", "ws")
        seg_params["SEG_PATH"] = seg_params['AFF_PATH'].replace("inference", "seg")
        seg_params["SCRATCH_PREFIX"] = seg_params['AFF_PATH'].replace("inference", "scratch")

    elif "synaptor" in defaults:
        seg_params = defaults["synaptor"]
        seg_params["Volumes"] = {
            "descriptor": output_path,
            "output": output_path.replace("inference", "seg"),
            "tempoutput": output_path.replace("inference", "seg_temp"),
            "baseseg": "None",
            "image": inf_params["IMAGE_PATH"],
        }

        seg_params["Dimensions"] = {
            "voxelres": tupstr(inf_params["IMAGE_RESOLUTION"]),
            "startcoord": tupstr(bbox[:3]),
            "volshape": tupstr(
                (bbox[3] - bbox[0], bbox[4] - bbox[1], bbox[5] - bbox[2])
            ),
            "chunkshape": tupstr(seg_params["Dimensions"]["chunkshape"]),
            "blockshape": tupstr(seg_params["Dimensions"]["blockshape"]),
            "patchshape": tupstr((0, 0, 0)),
        }

        seg_params["Parameters"] = {
            "ccthresh": seg_params["Parameters"]["ccthresh"],
            "szthresh": seg_params["Parameters"]["szthresh"],
            "dustthresh": 0,
            "nummergetasks": 1,
            "mergethresh": 0,
        }

        seg_params["Workflow"] = {
            "workflowtype": "Segmentation",
            "workspacetype": "File",
            "queueurl": "SHOULD_BE_SET_BY_AIRFLOW",
            "queuename": "SHOULD_BE_SET_BY_AIRFLOW",
            "connectionstr": "None",
            "storagedir": output_path.replace("inference", "scratch"),
            "maxclustersize": 10,
            "synaptor_image": seg_params["Workflow"]["synaptor_image"],
        }

    # a flag for handle_batch so that chunkflow parameters aren't
    # injected into these
    seg_params["INHERIT_PARAMETERS"] = False

    return inf_params, seg_params


def extract_model(msgtext: str) -> str:
    regexp = re.compile("<?(gs://.*[A-z])>?\[?.*\]?")

    models = [regexp.match(word).groups()[0] for word in msgtext.split() if regexp.match(word)]

    if len(models) > 1:
        raise ValueError(f"more than one model found: {models}")
    elif len(models) == 0:
        raise ValueError(f"no models found matching pattern {regexp}")

    return models[0]


def tupstr(t: tuple[Any, ...]) -> str:
    return ", ".join(map(str, t))


def warm_up_clusters(defaults: dict, msg: dict) -> None:
    """Warms up the clusters that we'll need to reduce overall latency."""
    if "chunkflow" in defaults:
        warm_up("gpu", msg, run_cluster_management=False)

    if "abiss" in defaults:
        warm_up("atomic", msg, run_cluster_management=False)
        warm_up("igneous", msg)

    if "synaptor" in defaults:
        warm_up("synaptor-cpu", msg)
