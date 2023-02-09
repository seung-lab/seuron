import json
from copy import deepcopy
from datetime import datetime
from seuronbot import SeuronBot
from bot_info import broker_url
from bot_utils import replyto, extract_command, clear_queues, download_json, guess_run_type, latest_param_type, upload_param
from kombu_helper import drain_messages, visible_messages, get_message, put_message
from airflow_api import get_variable, set_variable, latest_dagrun_state, run_dag


@SeuronBot.on_message("show segmentation parameters",
                      description="Upload parameters of the last segmentation",
                      exclusive=False,
                      cancelable=False)
def on_parameters(msg):
    param = get_variable("param", deserialize_json=True)
    upload_param(msg, param)


@SeuronBot.on_message(["update parameters",
                       "please update parameters"],
                      description="Update segmentation/inference parameters",
                      cancelable=False,
                      file_inputs=True)
def on_update_parameters(msg):
    json_obj = download_json(msg)
    if json_obj:
        if isinstance(json_obj, list):
            json_obj = json_obj[0]

        run_type = guess_run_type(json_obj)
        if run_type == "seg_run":
            on_update_segmentation_parameters(msg)
        elif run_type == "inf_run":
            on_update_inference_parameters(msg)
        else:
            replyto(msg, "Cannot guess run type from input parameters, please be more specific")


@SeuronBot.on_message(["update segmentation parameters",
                       "please update segmentation parameters"],
                      description="Update segmentation parameters",
                      cancelable=False,
                      file_inputs=True)
def on_update_segmentation_parameters(msg):
    cmd = extract_command(msg)
    if cmd.startswith("please"):
        update_segmentation_param(msg, advanced=True)
    else:
        update_segmentation_param(msg, advanced=False)


@SeuronBot.on_message("update inference parameters",
                      description="Update inference parameters",
                      cancelable=False,
                      file_inputs=True)
def on_update_inference_parameters(msg):
    update_inference_param(msg)


@SeuronBot.on_message(["run segmentation", "run segmentations"],
                      description="Create segmentation with updated parameters")
def on_run_segmentations(msg):
    state = latest_dagrun_state("sanity_check")
    if latest_param_type() != 'seg_run':
        replyto(msg, "You have to update the parameters before starting the segmentation")
    elif state != "success":
        replyto(msg, "Sanity check failed, try again")
    else:
        replyto(msg, "Start segmentation")
        handle_batch("seg_run", msg)


@SeuronBot.on_message(["run inference", "run inferences"],
                      description="Inference with updated parameters")
def on_run_inferences(msg):
    state = latest_dagrun_state("chunkflow_generator")
    if latest_param_type() != 'inf_run':
        replyto(msg, "You have to update the parameters before starting the inference")
    elif state != "success":
        replyto(msg, "Chunkflow set_env failed, try again")
    else:
        replyto(msg, "Start inference")
        handle_batch("inf_run", msg)


@SeuronBot.on_message(["run pipeline"],
                      description="Run pipeline with updated parameters")
def on_run_pipeline(msg):
    param_updated = latest_param_type()
    if not param_updated:
        replyto(msg, "You have to update the parameters before starting the pipeline")
    elif param_updated == 'inf_run':
        on_run_inferences(msg)
    elif param_updated == 'seg_run':
        on_run_segmentations(msg)
    else:
        replyto(msg, "Do not understand the parameters, please upload them again")


@SeuronBot.on_message("extract contact surfaces",
                      description="Extract the contact surfaces between segments")
def on_extract_contact_surfaces(msg):
    state = latest_dagrun_state("sanity_check")
    if state != "success":
        replyto(msg, "Sanity check failed, try again")
    else:
        replyto(msg, "Extract contact surfaces")
        run_dag("contact_surface")


def check_advanced_settings(params):
    ADVANCED_PARAMETERS=["BATCH_MIP_TIMEOUT", "HIGH_MIP_TIMEOUT", "REMAP_TIMEOUT", "OVERLAP_TIMEOUT", "CHUNK_SIZE", "CV_CHUNK_SIZE", "HIGH_MIP"]
    if not isinstance(params, list):
        params = [params,]

    kw = []
    for p in params:
        for k in p:
            if k in ADVANCED_PARAMETERS:
                kw.append(k)

    return kw


def supply_default_param(json_obj):
    if not json_obj.get("NAME", ""):
        json_obj["NAME"] = datetime.now().strftime("%Y%m%d%H%M%S")

    if "SCRATCH_PREFIX" not in json_obj and "SCRATCH_PATH" not in json_obj:
        json_obj["SCRATCH_PREFIX"] = "gs://ranl_pipeline_scratch/"

    for p in ["WS","SEG"]:
        if "{}_PREFIX".format(p) not in json_obj and "{}_PATH".format(p) not in json_obj:
            json_obj["{}_PREFIX".format(p)] = json_obj.get("NG_PREFIX", "gs://ng_scratch_ranl/make_cv_happy/") + p.lower() + "/"


def update_inference_param(msg):
    json_obj = download_json(msg)
    if json_obj:
        clear_queues()
        drain_messages(broker_url, "chunkflow")

        put_message(broker_url, "seuronbot_payload", json_obj)

        if isinstance(json_obj, list):
            replyto(msg, "*{} batch jobs detected, only sanity check the first one for now*".format(len(json_obj)))
            json_obj = json_obj[0]

        supply_default_param(json_obj)
        replyto(msg, "Running chunkflow setup_env, please wait")
        set_variable('inference_param', json_obj, serialize_json=True)
        run_dag("chunkflow_generator")

    return


def update_segmentation_param(msg, advanced=False):
    json_obj = download_json(msg)
    kw = check_advanced_settings(json_obj)

    if len(kw) > 0 and not advanced:
        replyto(msg, f'You are trying to change advanced parameters: {",".join("`"+x+"`" for x in kw)}')
        replyto(msg, "Use `please update segmentation parameters` to confirm that you know what you are doing!")
        return
    elif len(kw) == 0 and advanced:
        replyto(msg, "You are too polite, do not use `please update segmentation parameters` without any advanced parameters!")
        return

    if json_obj:
        clear_queues()

        put_message(broker_url, "seuronbot_payload", json_obj)

        if isinstance(json_obj, list):
            if (len(json_obj) > 1):
                replyto(msg, "*{} batch jobs detected, only sanity check the first one for now*".format(len(json_obj)))
            json_obj = json_obj[0]

        supply_default_param(json_obj)
        replyto(msg, "Running sanity check, please wait")
        set_variable('param', json_obj, serialize_json=True)
        run_dag("sanity_check")

    return


def handle_batch(task, msg):
    current_task=task

    json_obj = get_message(broker_url, "seuronbot_payload")

    if json_obj is None:
        return
    if isinstance(json_obj, dict):
        json_obj = [json_obj]
    elif (not isinstance(json_obj, list)) or (not isinstance(json_obj[0], dict)):
        replyto(msg, "Batch process expects an array of dicts from the json file")
        return

    if len(json_obj) > 1:
        replyto(msg, "Batch jobs will reuse on the parameters from the first job unless new parameters are specified, *including those with default values*")

    default_param = json_obj[0]
    for i, p in enumerate(json_obj):
        if visible_messages(broker_url, "seuronbot_cmd") != 0:
            cmd = get_message(broker_url, "seuronbot_cmd")
            if cmd == "cancel":
                replyto(msg, "Cancel batch process")
                break

        if p.get("INHERIT_PARAMETERS", True):
            param = deepcopy(default_param)
        else:
            param = {}

        if i > 0:
            if 'NAME' in param:
                del param['NAME']
            for k in p:
                param[k] = p[k]
            supply_default_param(param)
            replyto(msg, "*Sanity check: batch job {} out of {}*".format(i+1, len(json_obj)))
            state = "unknown"
            current_task = guess_run_type(param)
            if current_task == "seg_run":
                set_variable('param', param, serialize_json=True)
                state = run_dag("sanity_check", wait_for_completion=True).state
            elif current_task == "inf_run":
                set_variable('inference_param', param, serialize_json=True)
                state = run_dag("chunkflow_generator", wait_for_completion=True).state
            elif current_task == "syn_run":
                set_variable("synaptor_param.json", param, serialize_json=True)
                state = run_dag("synaptor_sanity_check", wait_for_completion=True).state

            if state != "success":
                replyto(msg, "*Sanity check failed, abort!*")
                break

        state = "unknown"
        replyto(msg, "*Starting batch job {} out of {}*".format(i+1, len(json_obj)), broadcast=True)

        if current_task == "seg_run":
            state = run_dag('segmentation', wait_for_completion=True).state
        elif current_task == "inf_run":
            state = run_dag("chunkflow_worker", wait_for_completion=True).state
        elif current_task == "syn_run":
            state = run_dag("synaptor_file_seg", wait_for_completion=True).state

        if state != "success":
            replyto(msg, f"*Bach job failed, abort!* ({state})")
            break

    replyto(msg, "*Batch process finished*")
