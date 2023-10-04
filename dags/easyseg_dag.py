import pendulum
from airflow.decorators import dag, task

@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["maintenance, segmentation, inference"],
)
def easyseg_dag():
    @task(queue="cluster", priority_weight=1000)
    def start_worker():
        import os
        import redis
        from datetime import datetime
        from slack_message import slack_message
        from airflow.models import Variable
        from airflow.hooks.base_hook import BaseHook

        if Variable.get("vendor") == "Google":
            import google_api_helper as cluster_api
        else:
            cluster_api = None

        if cluster_api is None:
            return

        slack_message(":exclamation:*Turn on easyseg worker*")
        try:
            cluster_api.toggle_easyseg_worker(on=True)
        except Exception:
            pass

        ig_conn = BaseHook.get_connection("EasysegWorker")
        deployment = ig_conn.host
        instance = f"{deployment}-easyseg-worker"

        redis_host = os.environ['REDIS_SERVER']
        timestamp = datetime.now().timestamp()
        r = redis.Redis(redis_host)
        r.set(instance, timestamp)

    start_worker()


easyseg_dag()
