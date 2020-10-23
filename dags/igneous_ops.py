from airflow.operators.python_operator import PythonOperator
from airflow.utils.weight_rule import WeightRule
from slack_message import task_done_alert, task_retry_alert
from igneous_and_cloudvolume import downsample, downsample_for_meshing, mesh, mesh_manifest, create_skeleton_fragments, merge_skeleton_fragments
from helper_ops import placeholder_op

def create_igneous_ops(param, dag):
    import os
    seg_cloudpath = param["SEG_PATH"]
    ws_cloudpath = param["WS_PATH"]
    ops = [placeholder_op(dag, "start_igneous_tasks")]

    if not param.get("SKIP_DOWNSAMPLE", False):
        if not param.get("SKIP_MESHING", False):
            current_op = PythonOperator(
                task_id="downsample_with_mask",
                python_callable=downsample_for_meshing,
                op_args = [seg_cloudpath, param.get("SIZE_THRESHOLDED_MESH", False), ],
                on_success_callback=task_done_alert,
                on_retry_callback=task_retry_alert,
                weight_rule=WeightRule.ABSOLUTE,
                queue="manager",
                dag=dag
            )
            ops[-1] >> current_op
            ops.append(current_op)

    if not param.get("SKIP_MESHING", False):
        current_op = PythonOperator(
            task_id="mesh",
            python_callable=mesh,
            op_args = [seg_cloudpath, param.get("MESH_QUALITY", "NORMAL"), ],
            on_success_callback=task_done_alert,
            on_retry_callback=task_retry_alert,
            weight_rule=WeightRule.ABSOLUTE,
            queue="manager",
            dag=dag
        )

        ops[-1] >> current_op
        ops.append(current_op)

        current_op = PythonOperator(
            task_id="mesh_manifest",
            python_callable=mesh_manifest,
            op_args = [seg_cloudpath, param["BBOX"], param["CHUNK_SIZE"], ],
            on_success_callback=task_done_alert,
            on_retry_callback=task_retry_alert,
            weight_rule=WeightRule.ABSOLUTE,
            queue="manager",
            dag=dag
        )

        ops[-1] >> current_op
        ops.append(current_op)

    if not param.get("SKIP_DOWNSAMPLE", False):
        if not param.get("SKIP_WS", False):
            current_op = PythonOperator(
                task_id="downsample_ws",
                python_callable=downsample,
                op_args = [ws_cloudpath, ],
                on_success_callback=task_done_alert,
                on_retry_callback=task_retry_alert,
                weight_rule=WeightRule.ABSOLUTE,
                queue="manager",
                dag=dag
            )

            ops[-1] >> current_op
            ops.append(current_op)

        current_op = PythonOperator(
            task_id="downsample_seg",
            python_callable=downsample,
            op_args = [seg_cloudpath, ],
            on_success_callback=task_done_alert,
            on_retry_callback=task_retry_alert,
            weight_rule=WeightRule.ABSOLUTE,
            queue="manager",
            dag=dag
        )

        ops[-1] >> current_op
        ops.append(current_op)

        current_op = PythonOperator(
            task_id="downsample_size_map",
            python_callable=downsample,
            op_args = [os.path.join(seg_cloudpath, "size_map"), ],
            on_success_callback=task_done_alert,
            on_retry_callback=task_retry_alert,
            weight_rule=WeightRule.ABSOLUTE,
            queue="manager",
            dag=dag
        )

        ops[-1] >> current_op
        ops.append(current_op)

    if not param.get("SKIP_SKELETON", False):
        current_op = PythonOperator(
            task_id="skeleton_fragment",
            python_callable=create_skeleton_fragments,
            op_args = [seg_cloudpath, param.get("TEASAR_PARAMS", {'scale':10, 'const': 10}), ],
            on_success_callback=task_done_alert,
            on_retry_callback=task_retry_alert,
            weight_rule=WeightRule.ABSOLUTE,
            queue="manager",
            dag=dag
        )

        ops[-1] >> current_op
        ops.append(current_op)

        current_op = PythonOperator(
            task_id="merge_skeleton",
            python_callable=merge_skeleton_fragments,
            op_args = [seg_cloudpath, ],
            on_success_callback=task_done_alert,
            on_retry_callback=task_retry_alert,
            weight_rule=WeightRule.ABSOLUTE,
            queue="manager",
            dag=dag
        )

        ops[-1] >> current_op
        ops.append(current_op)

    return ops

