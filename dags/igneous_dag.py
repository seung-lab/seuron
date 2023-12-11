from igneous_and_cloudvolume import submit_igneous_tasks, submit_custom_cpu_tasks, submit_custom_gpu_tasks
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.weight_rule import WeightRule
from datetime import datetime
from slack_message import task_failure_alert
from helper_ops import scale_down_cluster_op, collect_metrics_op, toggle_nfs_server_op

igneous_default_args = {
    'owner': 'seuronbot',
    'depends_on_past': False,
    'start_date': datetime(2019, 2, 8),
    'catchup': False,
    'retries': 0,
}

dag_igneous = DAG("igneous", default_args=igneous_default_args, schedule_interval=None, tags=['igneous tasks'])
scaling_igneous_finish = scale_down_cluster_op(dag_igneous, "igneous_finish", "igneous", 0, "cluster")

start_nfs_server = toggle_nfs_server_op(dag_igneous, on=True)
stop_nfs_server = toggle_nfs_server_op(dag_igneous, on=False)

submit_igneous_tasks = PythonOperator(
    task_id="submit_igneous_tasks",
    python_callable=submit_igneous_tasks,
    priority_weight=100000,
    on_failure_callback=task_failure_alert,
    weight_rule=WeightRule.ABSOLUTE,
    queue="manager",
    dag=dag_igneous
)

collect_metrics_op(dag_igneous) >> start_nfs_server >> submit_igneous_tasks >> scaling_igneous_finish >> stop_nfs_server


dag_custom_cpu = DAG("custom-cpu", default_args=igneous_default_args, schedule_interval=None, tags=['custom tasks'])
scaling_custom_cpu_finish = scale_down_cluster_op(dag_custom_cpu, "custom_cpu_finish", "custom-cpu", 0, "cluster")


submit_custom_cpu_tasks = PythonOperator(
    task_id="submit_custom_cpu_tasks",
    python_callable=submit_custom_cpu_tasks,
    priority_weight=100000,
    on_failure_callback=task_failure_alert,
    weight_rule=WeightRule.ABSOLUTE,
    queue="manager",
    dag=dag_custom_cpu
)

collect_metrics_op(dag_custom_cpu) >> submit_custom_cpu_tasks >> scaling_custom_cpu_finish


dag_custom_gpu = DAG("custom-gpu", default_args=igneous_default_args, schedule_interval=None, tags=['custom tasks'])
scaling_custom_gpu_finish = scale_down_cluster_op(dag_custom_gpu, "custom_gpu_finish", "custom-gpu", 0, "cluster")


submit_custom_gpu_tasks = PythonOperator(
    task_id="submit_custom_gpu_tasks",
    python_callable=submit_custom_gpu_tasks,
    priority_weight=100000,
    on_failure_callback=task_failure_alert,
    weight_rule=WeightRule.ABSOLUTE,
    queue="manager",
    dag=dag_custom_gpu
)

collect_metrics_op(dag_custom_gpu) >> submit_custom_gpu_tasks >> scaling_custom_gpu_finish
