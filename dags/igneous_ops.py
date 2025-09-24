from airflow.operators.python_operator import PythonOperator
from airflow.utils.weight_rule import WeightRule
from slack_message import task_retry_alert
from igneous_and_cloudvolume import downsample, downsample_for_meshing, ingest_spatial_index, mesh, mesh_manifest, merge_mesh_fragments, create_skeleton_fragments, merge_skeleton_fragments, ingest_spatial_index
from helper_ops import placeholder_op
from dag_utils import get_connection, db_name


def create_igneous_ops(param, dag):
    import os
    seg_cloudpath = param["SEG_PATH"]
    ws_cloudpath = param["WS_PATH"]
    ops = [placeholder_op(dag, "start_igneous_tasks")]
    run_name = f'{param["NAME"].replace(".", "_")}.segmentation'

    nfs_conn = get_connection("NFSServer")

    if nfs_conn:
        extra_args = nfs_conn.extra_dejson
        sql_url = f"postgres://postgres:airflow@{extra_args['hostname']}"
        queue = "nfs"
    else:
        extra_args = None
        sql_url = None
        queue = "manager"


    def nfs_kwargs(io_mode):
        if not nfs_conn:
            return {"frag_path": None, "sql_url": None}
        if io_mode == "write":
            return {"frag_path": f"file:///share/{db_name(param['NAME'], 'segmentation')}", "sql_url": sql_url}
        elif io_mode == "read":
            return {"frag_path": f"http://{extra_args['hostname']}/share/{db_name(param['NAME'], 'segmentation')}", "sql_url": sql_url}
        else:
            return {"frag_path": None, "sql_url": None}


    if not param.get("SKIP_DOWNSAMPLE", False):
        if not param.get("SKIP_MESHING", False):
            current_op = PythonOperator(
                task_id="downsample_with_mask",
                python_callable=downsample_for_meshing,
                op_args=[run_name, seg_cloudpath, param.get("SIZE_THRESHOLDED_MESH", False), ],
                on_retry_callback=task_retry_alert,
                weight_rule=WeightRule.ABSOLUTE,
                queue=queue,
                dag=dag
            )
            ops[-1] >> current_op
            ops.append(current_op)

    if not param.get("SKIP_MESHING", False):
        current_op = PythonOperator(
            task_id="mesh",
            python_callable=mesh,
            op_args=[run_name, seg_cloudpath, param.get("MESH_QUALITY", "NORMAL"), param.get("SHARDED_MESH", True), ],
            op_kwargs=nfs_kwargs(io_mode="write"),
            on_retry_callback=task_retry_alert,
            weight_rule=WeightRule.ABSOLUTE,
            queue=queue,
            dag=dag
        )

        ops[-1] >> current_op
        ops.append(current_op)

        if sql_url and param.get("SHARDED_MESH", True):
            current_op = PythonOperator(
                task_id="ingest_spatial_index_mesh",
                python_callable=ingest_spatial_index,
                op_args=[run_name, seg_cloudpath, sql_url, "mesh",],
                on_retry_callback=task_retry_alert,
                weight_rule=WeightRule.ABSOLUTE,
                queue=queue,
                dag=dag
            )
            ops[-1] >> current_op
            ops.append(current_op)

        if param.get("SHARDED_MESH", True):
            current_op = PythonOperator(
                task_id="merge_mesh_fragments",
                python_callable=merge_mesh_fragments,
                op_args=[run_name, seg_cloudpath, param.get("SHARDED_MESH_WORKER_CONCURRENCY", None)],
                op_kwargs=nfs_kwargs(io_mode="read"),
                on_retry_callback=task_retry_alert,
                weight_rule=WeightRule.ABSOLUTE,
                queue=queue,
                dag=dag
            )
        else:
            current_op = PythonOperator(
                task_id="mesh_manifest",
                python_callable=mesh_manifest,
                op_args=[run_name, seg_cloudpath, param["BBOX"], param["CHUNK_SIZE"], ],
                op_kwargs=nfs_kwargs(io_mode="read"),
                on_retry_callback=task_retry_alert,
                weight_rule=WeightRule.ABSOLUTE,
                queue=queue,
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
            op_args=[run_name, downsample_target],
            on_retry_callback=task_retry_alert,
            weight_rule=WeightRule.ABSOLUTE,
            queue=queue,
            dag=dag
        )

        ops[-1] >> current_op
        ops.append(current_op)

    if not param.get("SKIP_SKELETON", False):
        current_op = PythonOperator(
            task_id="skeleton_fragment",
            python_callable=create_skeleton_fragments,
            op_args=[run_name, seg_cloudpath, param.get("TEASAR_PARAMS", {'scale': 10, 'const': 10}), ],
            op_kwargs=nfs_kwargs(io_mode="write"),
            on_retry_callback=task_retry_alert,
            weight_rule=WeightRule.ABSOLUTE,
            queue=queue,
            dag=dag
        )

        ops[-1] >> current_op
        ops.append(current_op)

        if sql_url:
            current_op = PythonOperator(
                task_id="ingest_spatial_index_skeleton",
                python_callable=ingest_spatial_index,
                op_args=[run_name, seg_cloudpath, sql_url, "skeleton",],
                on_retry_callback=task_retry_alert,
                weight_rule=WeightRule.ABSOLUTE,
                queue=queue,
                dag=dag
            )
            ops[-1] >> current_op
            ops.append(current_op)

        current_op = PythonOperator(
            task_id="merge_skeleton",
            python_callable=merge_skeleton_fragments,
            op_args=[run_name, seg_cloudpath, ],
            op_kwargs=nfs_kwargs(io_mode="read"),
            on_retry_callback=task_retry_alert,
            weight_rule=WeightRule.ABSOLUTE,
            queue=queue,
            dag=dag
        )

        ops[-1] >> current_op
        ops.append(current_op)

    return ops
