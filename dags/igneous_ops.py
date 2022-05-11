from airflow.operators.python_operator import PythonOperator
from airflow.utils.weight_rule import WeightRule
from slack_message import task_retry_alert
from igneous_and_cloudvolume import downsample, downsample_for_meshing, mesh, mesh_manifest, merge_mesh_fragments, create_skeleton_fragments, merge_skeleton_fragments
from helper_ops import placeholder_op

def create_igneous_ops(param, dag):
    import os
    seg_cloudpath = param["SEG_PATH"]
    ws_cloudpath = param["WS_PATH"]
    ops = [placeholder_op(dag, "start_igneous_tasks")]
    run_name = f'{param["NAME"]}.segmentation'

    if not param.get("SKIP_DOWNSAMPLE", False):
        if not param.get("SKIP_MESHING", False):
            current_op = PythonOperator(
                task_id="downsample_with_mask",
                python_callable=downsample_for_meshing,
                op_args = [run_name, seg_cloudpath, param.get("SIZE_THRESHOLDED_MESH", False), ],
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
            op_args = [run_name, seg_cloudpath, param.get("MESH_QUALITY", "NORMAL"), param.get("SHARDED_MESH", False), ],
            on_retry_callback=task_retry_alert,
            weight_rule=WeightRule.ABSOLUTE,
            queue="manager",
            dag=dag
        )

        ops[-1] >> current_op
        ops.append(current_op)

        if param.get("SHARDED_MESH", False):
            current_op = PythonOperator(
                task_id="merge_mesh_fragments",
                python_callable=merge_mesh_fragments,
                op_args = [run_name, seg_cloudpath, ],
                on_retry_callback=task_retry_alert,
                weight_rule=WeightRule.ABSOLUTE,
                queue="manager",
                dag=dag
            )
        else:
            current_op = PythonOperator(
                task_id="mesh_manifest",
                python_callable=mesh_manifest,
                op_args = [run_name, seg_cloudpath, param["BBOX"], param["CHUNK_SIZE"], ],
                on_retry_callback=task_retry_alert,
                weight_rule=WeightRule.ABSOLUTE,
                queue="manager",
                dag=dag
            )

        ops[-1] >> current_op
        ops.append(current_op)

    downsample_target = [seg_cloudpath, os.path.join(seg_cloudpath, "size_map")]
    if not param.get("SKIP_DOWNSAMPLE", False):
        if not param.get("SKIP_WS", False):
            downsample_target.append(ws_cloudpath)

        current_op = PythonOperator(
            task_id="downsample",
            python_callable=downsample,
            op_args = [run_name, downsample_target],
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
            op_args = [run_name, seg_cloudpath, param.get("TEASAR_PARAMS", {'scale':10, 'const': 10}), ],
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
            op_args = [run_name, seg_cloudpath, ],
            on_retry_callback=task_retry_alert,
            weight_rule=WeightRule.ABSOLUTE,
            queue="manager",
            dag=dag
        )

        ops[-1] >> current_op
        ops.append(current_op)

    return ops

