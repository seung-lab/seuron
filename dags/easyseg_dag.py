import pendulum
from airflow.decorators import dag, task

@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
)
def easyseg_dag():
    @task(queue="cluster", priority_weight=1000)
    def start_worker():
        from slack_message import slack_message
        from google_api_helper import toggle_easyseg_worker
        slack_message(":exclamation:*Turn on easyseg worker*")
        try:
            toggle_easyseg_worker(on=True)
        except Exception:
            pass

    start_worker()


easyseg_dag()
