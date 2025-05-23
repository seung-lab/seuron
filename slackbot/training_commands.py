"""Training commands."""
from __future__ import annotations

import re
from datetime import datetime
from typing import Optional

from seuronbot import SeuronBot
from airflow_api import run_dag
from bot_utils import replyto, download_json
from airflow_api import get_variable, set_variable
from common import docker_helper


@SeuronBot.on_message("update training parameters",
                      description="Update the default parameters for training",
                      exclusive=False,
                      cancelable=False,
                      file_inputs=True)
def update_training_parameters(msg: dict) -> None:
    json_obj = download_json(msg)

    if json_obj:
        try:
            initial_sanity_check(json_obj, full=False)
        except Exception as e:
            replyto(msg, f"Error parsing parameters: {e}")

        replyto(msg, "Download deepem image and check for custom entrypoint")
        deepem_image = json_obj.get("deepem_image", "zettaai/deepem")
        if "TORCHRUN_LAUNCHER" in json_obj:
            if json_obj["TORCHRUN_LAUNCHER"]:
                replyto(msg, ":cool:Launch training script with torchrun")
            else:
                replyto(msg, ":disappointed:Torchrun disabled")
                json_obj["NUM_TRAINERS"] = 1
        else:
            if docker_helper.has_custom_entrypoint(deepem_image):
                replyto(msg, ":disappointed:Custom entrypoint found, disable DDP")
                json_obj["TORCHRUN_LAUNCHER"] = False
                json_obj["NUM_TRAINERS"] = 1
            else:
                replyto(msg, ":cool:Launch training script with torchrun")
                json_obj["TORCHRUN_LAUNCHER"] = True

        set_variable("training_param", json_obj, serialize_json=True)
        replyto(msg, "Parameters successfully updated")

    else:
        replyto(msg, "No json found")


def initial_sanity_check(json_obj: dict, full: bool = False) -> None:

    required_keys = [
        "data", "sampler", "model", "augment", "chkpt_num", "max_iter", "remote_dir",
    ]

    for k in required_keys:
        assert k in json_obj, f"{k} is a required parameter"

    assert_type(json_obj, "fov", str)
    assert_type(json_obj, "outputsz", str)
    assert_type(json_obj, "gpu_ids", list)
    assert_type(json_obj, "width", list)

    flag_keys = [
        "no_eval", "inverse", "class_balancing", "amsgrad", "export_onnx"
    ]
    for k in flag_keys :
        assert_type(json_obj, k, type(None))

    if "zettaset_lookup" in json_obj:
        matches = {
            "affinity": "aff",
            "synapse": ["syn", "psd"],
            "boundary": "bdr",
            "mitochondria": "mit",
            "myelin": "mye",
            "fold": "fld",
            "blv": "blood vessel",
            "glia": "glia",
        }

        for k, v in json_obj["zettaset_lookup"].items():
            if k not in matches:
                continue

            matching_target = matches[k]

            if isinstance(matching_target, str):
                assert matching_target in json_obj, (
                    "target matching lookup {k} not found"
                )
            elif isinstance(matching_target, list):
                assert any(t in json_obj for t in matching_target), (
                    "target matching lookup {k} not found"
                )

            else:
                raise Exception(
                    f"unknown match type: {matching_target} - {type(matching_target)}"
                )

    if full:
        assert "exp_name" in json_obj, "empty experiment name"
        assert "annotation_ids" in json_obj, "no annotation ids"
        assert_type(json_obj, "exp_name", str)
        assert_type(json_obj, "annotation_ids", list)
        assert_type(json_obj, "pretrain", str)


def assert_type(json_obj: dict, key: str, argtype: type):
    if key in json_obj:
        assert isinstance(json_obj[key], argtype), (
            f"flag argument {key} needs to be of type {argtype}"
            f" (not {type(json_obj[key])})"
        )


@SeuronBot.on_message("run training",
                      description=(
                          "Run training on a set of webknossos annotations"
                      ),
                      exclusive=True,
                      extra_parameters=True,
                      cancelable=True)
def run_training(msg: dict) -> None:
    try:
        pretrain = extract_seed_model(msg["text"])
    except ValueError:
        replyto(msg, "No seed model found")
        pretrain = None

    try:
        exp_name = extract_exp_name(msg["text"])
    except ValueError:
        exp_name = generate_exp_name()

    try:
        annotation_ids = extract_annotations(msg["text"], exp_name, pretrain)
    except Exception as e:
        replyto(msg, f"Error parsing annotations: {e}")
        return

    params = get_variable("training_param", deserialize_json=True)
    wkparams = get_variable("webknossos_param", deserialize_json=True, default_var=None)

    if pretrain:
        params["pretrain"] = pretrain

    params["exp_name"] = exp_name
    params["annotation_ids"] = annotation_ids

    try:
        initial_sanity_check(params, full=True)
    except Exception as e:
        replyto(msg, f"Sanity check error: {e}")
        return

    set_variable("training_param", params, serialize_json=True)
    if wkparams:
        wkparams["annotation_ids"] = " ".join(annotation_ids)
        set_variable("webknossos_param", wkparams, serialize_json=True)
    replyto(msg, f"Running training experiment: `{params['exp_name']}`")
    run_dag("training")


def generate_exp_name():
    now = datetime.utcnow()
    return (
        f"{now.year:04d}{now.month:02d}{now.day:02d}"
        f"-{now.hour:02d}{now.minute:02d}{now.second:02d}"
    )


def regex_match(msgtext: str, regexp: re.Pattern, fieldname: str) -> str:

    candidates = list(filter(lambda word: regexp.match(word), msgtext.split()))

    if len(candidates) > 1:
        raise ValueError(f"more than one {fieldname} found: {candidates}")
    elif len(candidates) == 0:
        raise ValueError(f"no {fieldname}s found matching pattern {regexp}")

    return regexp.match(candidates[0]).groups()[0]


def extract_exp_name(msgtext: str) -> str:
    return regex_match(msgtext, re.compile("\"(.*)\""), "exp_name")


def extract_seed_model(msgtext: str) -> str:
    return regex_match(msgtext, re.compile("<(gs://.*.chkpt)>"), "seed model")


def extract_annotations(
    msgtext: str, exp_name: str, pretrain: Optional[str]
) -> list[str]:
    training_word_index = msgtext.split().index("training")
    words_after_training = msgtext.split()[training_word_index + 1:]

    return [
        word for word in words_after_training
        if (
            (pretrain is None or pretrain not in word)
            and exp_name not in word
        )
    ]
