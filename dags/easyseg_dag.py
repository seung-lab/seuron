import pendulum
from airflow.decorators import dag, task
from slack_message import slack_message
from google_api_helper import start_easyseg_worker

@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
)
def easyseg_dag():
    @task()
    def start_worker():
        slack_message(":exclamation:*Turn on easyseg worker*")
        start_easyseg_worker()

    start_worker()


easyseg_dag()
