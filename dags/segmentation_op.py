from worker_op import worker_op
from airflow.utils.weight_rule import WeightRule
from datetime import timedelta
from param_default import default_args, cmd_proto, default_mount_path, default_seg_workspace, check_worker_image_labels
from slack_message import task_retry_alert
import os


def composite_chunks_wrap_op(img, dag, config_mounts, queue, tag, stage, op, params):
    workspace_path = params.get("WORKSPACE_PATH", default_seg_workspace)
    overlap = 0
    overlap_mode = params.get("OVERLAP_MODE", False)
    if overlap_mode:
        overlap_mip = params.get("OVERLAP_MIP", params.get("BATCH_MIP", 3))
        overlap = 2 if int(tag.split("_")[0]) > overlap_mip else 1
    cmdlist = f'export OVERLAP={overlap} && export STAGE={stage} && {os.path.join(workspace_path, "scripts/run_wrapper.sh")} . composite_chunk_{op} {tag}'

    return worker_op(
        variables=config_mounts,
        mount_point=params.get("MOUNT_PATH", default_mount_path),
        task_id='composite_chunk_{}_{}'.format(stage, tag),
        command=cmd_proto.format(cmdlist),
        default_args=default_args,
        image=img,
        on_retry_callback=task_retry_alert,
        weight_rule=WeightRule.ABSOLUTE,
        execution_timeout=timedelta(minutes=params.get("HIGH_MIP_TIMEOUT", 600)),
        force_pull=True,
        queue=queue,
        dag=dag
    )


def composite_chunks_overlap_op(img, dag, config_mounts, queue, tag, params):
    workspace_path = params.get("WORKSPACE_PATH", default_seg_workspace)
    cmdlist = f'export STAGE=agg && {os.path.join(workspace_path, "scripts/run_wrapper.sh")} . composite_chunk_overlap {tag}'

    return worker_op(
        variables=config_mounts,
        mount_point=params.get("MOUNT_PATH", default_mount_path),
        task_id='composite_chunk_overlap_{}'.format(tag),
        command=cmd_proto.format(cmdlist),
        default_args=default_args,
        image=img,
        on_retry_callback=task_retry_alert,
        weight_rule=WeightRule.ABSOLUTE,
        execution_timeout=timedelta(minutes=params.get("OVERLAP_TIMEOUT", 240)),
        force_pull=True,
        queue=queue,
        dag=dag
    )


def composite_chunks_batch_op(img, dag, config_mounts, queue, mip, tag, stage, op, params):
    workspace_path = params.get("WORKSPACE_PATH", default_seg_workspace)
    overlap = 1 if params.get("OVERLAP_MODE", False) else 0
    cmdlist = f'export OVERLAP={overlap} && export STAGE={stage} && {os.path.join(workspace_path, "scripts/run_batch.sh")} {op} {mip} {tag}'

    return worker_op(
        variables=config_mounts,
        mount_point=params.get("MOUNT_PATH", default_mount_path),
        task_id='batch_chunk_{}_{}'.format(stage, tag),
        command=cmd_proto.format(cmdlist),
        default_args=default_args,
        image=img,
        on_retry_callback=task_retry_alert,
        weight_rule=WeightRule.ABSOLUTE,
        execution_timeout=timedelta(minutes=params.get("BATCH_MIP_TIMEOUT", 120)),
        force_pull=True,
        queue=queue,
        dag=dag
    )


def remap_chunks_batch_op(img, dag, config_mounts, queue, mip, tag, stage, op, params):
    workspace_path = params.get("WORKSPACE_PATH", "/root/seg")
    cmdlist = f'export STAGE={stage} && {os.path.join(workspace_path, "scripts/remap_batch.sh")} {stage} {mip} {tag}'
    return worker_op(
        variables=config_mounts,
        mount_point=params.get("MOUNT_PATH", "/root/.cloudvolume/secrets"),
        task_id='remap_chunk_{}_{}'.format(stage, tag),
        command=cmd_proto.format(cmdlist),
        default_args=default_args,
        image=img,
        on_retry_callback=task_retry_alert,
        weight_rule=WeightRule.ABSOLUTE,
        execution_timeout=timedelta(minutes=params.get("REMAP_TIMEOUT", 120)),
        force_pull=True,
        queue=queue,
        dag=dag
    )
