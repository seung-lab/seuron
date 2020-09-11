from airflow.operators.docker_plugin import DockerWithVariablesOperator
from airflow.utils.weight_rule import WeightRule
from datetime import timedelta
from param_default import default_args, cv_path, cmd_proto
from slack_message import task_retry_alert


def composite_chunks_wrap_op(img, dag, config_mounts, queue, tag, stage, op, params):
    overlap = 1 if params.get("OVERLAP", False) else 0
    cmdlist = "export OVERLAP={} && export STAGE={} && /root/seg/scripts/run_wrapper.sh . composite_chunk_{} {}".format(overlap, stage, op, tag)

    return DockerWithVariablesOperator(
        config_mounts,
        mount_point=cv_path,
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
    cmdlist = "export OVERLAP=1 && export STAGE=agg && /root/seg/scripts/run_wrapper.sh . composite_chunk_overlap {}".format(tag)

    return DockerWithVariablesOperator(
        config_mounts,
        mount_point=cv_path,
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
    cmdlist = "export OVERLAP=0 && export STAGE={} && /root/seg/scripts/run_batch.sh {} {} {}".format(stage, op, mip, tag)

    return DockerWithVariablesOperator(
        config_mounts,
        mount_point=cv_path,
        task_id='composite_chunk_{}_{}'.format(stage, tag),
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
    cmdlist = "export OVERLAP=0 && export STAGE={} && /root/seg/scripts/remap_batch.sh {} {} {}".format(stage, stage, mip, tag)
    return DockerWithVariablesOperator(
        config_mounts,
        mount_point=cv_path,
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
