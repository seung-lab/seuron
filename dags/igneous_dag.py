from igneous_and_cloudvolume import submit_igneous_tasks, submit_custom_cpu_tasks, submit_custom_gpu_tasks
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.weight_rule import WeightRule
from datetime import datetime
from slack_message import task_failure_alert

igneous_default_args = {
    'owner': 'seuronbot',
    'depends_on_past': False,
    'start_date': datetime(2019, 2, 8),
    'catchup': False,
    'retries': 0,
}

dag_igneous = DAG("igneous", default_args=igneous_default_args, schedule_interval=None, tags=['igneous tasks'])


submit_igneous_tasks = PythonOperator(
    task_id="submit_igneous_tasks",
    python_callable=submit_igneous_tasks,
    priority_weight=100000,
    on_failure_callback=task_failure_alert,
    weight_rule=WeightRule.ABSOLUTE,
    queue="manager",
    dag=dag_igneous
)


dag_custom_cpu = DAG("custom-cpu", default_args=igneous_default_args, schedule_interval=None, tags=['custom tasks'])


submit_custom_cpu_tasks = PythonOperator(
    task_id="submit_custom_cpu_tasks",
    python_callable=submit_custom_cpu_tasks,
    priority_weight=100000,
    on_failure_callback=task_failure_alert,
    weight_rule=WeightRule.ABSOLUTE,
    queue="manager",
    dag=dag_custom_cpu
)


dag_custom_gpu = DAG("custom-gpu", default_args=igneous_default_args, schedule_interval=None, tags=['custom tasks'])


submit_custom_gpu_tasks = PythonOperator(
    task_id="submit_custom_gpu_tasks",
    python_callable=submit_custom_gpu_tasks,
    priority_weight=100000,
    on_failure_callback=task_failure_alert,
    weight_rule=WeightRule.ABSOLUTE,
    queue="manager",
    dag=dag_custom_gpu
)


