import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator


metrics_default_args = {
    'owner': 'seuronbot',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2019, 2, 8),
    'catchup': False,
    'retries': 3,
}

def format_uptime(dt):
    for attr in ["weeks", "days", "hours", "minutes", "seconds"]:
        if getattr(dt, attr) > 0:
            return f'{getattr(dt, "total_"+attr)():.2f} instance-{attr}'


with DAG("compute_metrics",
        default_args=metrics_default_args,
        schedule_interval=None,
        tags=['maintenance']) as dag:

    def resource_summary(dag_run):
        import humanize
        from time import sleep
        from airflow.models.dagrun import DagRun
        from airflow.utils.state import DagRunState
        from airflow.models import Variable
        from slack_message import slack_message

        if Variable.get("vendor") != "Google":
            slack_message("Seuron only support compute metric collection for GCE")
            return

        run_metadata = Variable.get("run_metadata", deserialize_json=True, default_var={})

        if not run_metadata.get("track_resources", True):
            return

        from google_api_helper import collect_resource_metrics
        conf = dag_run.conf
        target_dag_id = conf["dag_id"]
        target_run_id = conf["run_id"]
        runs = DagRun.find(dag_id=target_dag_id, run_id=target_run_id)

        if not runs:
            slack_message("Cannot find the target dag run")
            return

        target_dag_run = runs[0]

        while target_dag_run.state == DagRunState.RUNNING:
            sleep(60)
            target_dag_run.refresh_from_db()

        start_time = target_dag_run.logical_date
        end_time = target_dag_run.end_date

        delay = 240 - (pendulum.now() - end_time).seconds

        if delay > 0:
            slack_message(f"Sleep {delay} seconds before collecting metrics")
            sleep(delay)


        messages = []
        resources = collect_resource_metrics(start_time, end_time)
        for ig in resources:
            if ig == "GCS":
                continue
            if resources[ig]:
                if "gpu_utilization" in resources[ig]:
                    messages += [f"`{ig}`: `{format_uptime(resources[ig]['uptime'])}` (`{resources[ig]['gpu_utilization']:.2f}%` GPU utilization, `{resources[ig]['cpu_utilization']:.2f}%` CPU utilization), `{humanize.naturalsize(resources[ig]['received_bytes'])}` received, `{humanize.naturalsize(resources[ig]['sent_bytes'])}` sent"]
                else:
                    messages += [f"`{ig}`: `{format_uptime(resources[ig]['uptime'])}` (`{resources[ig]['cpu_utilization']:.2f}%` CPU utilization), `{humanize.naturalsize(resources[ig]['received_bytes'])}` received, `{humanize.naturalsize(resources[ig]['sent_bytes'])}` sent"]

        if resources["GCS"]:
            for b in resources["GCS"]:
                api_summary = ",".join([f"`{k}`: {humanize.intword(v)}" for k, v in resources["GCS"][b].items()])
                messages += [f"GCS api call to bucket `{b}`: {api_summary}"]

        if messages:
            slack_message("\n".join([f"*Compute resources used by* `{target_dag_id}` *in {pendulum.period(start_time, end_time).in_words()}*:"] + messages), broadcast=True)

    res_op = PythonOperator(
                task_id='resource_summary',
                python_callable=resource_summary,
                priority_weight=1000,
                queue="manager",
            )
