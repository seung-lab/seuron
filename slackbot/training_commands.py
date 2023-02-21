"""Training commands."""
from __future__ import annotations

import re
import random
import string

from seuronbot import SeuronBot
from airflow_api import run_dag
from bot_utils import replyto, download_json
from airflow_api import get_variable, set_variable


@SeuronBot.on_message("update training parameters",
                      description="Update the default parameters for training",
                      exclusive=True,
                      file_inputs=True)
def update_easy_seg(msg: dict) -> None:
    json_obj = download_json(msg)

    if json_obj:
        try:
            sanity_check(json_obj, full=False)
        except Exception as e:
            replyto(msg, f"Error parsing parameters: {e}")

        set_variable("training_param", json_obj, serialize_json=True)
        replyto(msg, "Parameters successfully updated")

    else:
        replyto(msg, "No json found")


def sanity_check(json_obj: dict, full: bool = False) -> None:
    pass


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
        annotation_ids = extract_annotations(msg["text"], None)
    except Exception as e:
        replyto(msg, f"Error parsing annotations: {e}")
        return

    try:
        exp_name = extract_exp_name(msg["text"])
    except ValueError:
        exp_name = generate_exp_name()

    params = get_variable("training_param", deserialize_json=True)

    if pretrain:
        params["pretrain"] = pretrain

    params["exp_name"] = exp_name
    params["annotation_ids"] = annotation_ids

    try:
        sanity_check(params, full=True)
    except Exception as e:
        replyto(msg, f"Sanity check error: {e}")
        return

    set_variable("training_param", params, serialize_json=True)
    replyto(msg, f"Running training experiment: `{params['exp_name']}`")
    run_dag("training")


def generate_exp_name():
    return "".join(random.sample(string.ascii_letters, k=10))


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
    return regex_match(
        msgtext, re.compile("<?(gs://.*[A-z].chkpt)>?\[?.*\]?"), "seed model"  # noqa
    )


def extract_annotations(msgtext: str, pretrain: str) -> list[str]:
    training_word_index = msgtext.split().index("training")
    words_after_training = msgtext.split()[training_word_index + 1:]

    return [
        word for word in words_after_training if (pretrain is None or pretrain != word)
    ]
