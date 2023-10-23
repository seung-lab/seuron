import time
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
        from dag_utils import get_connection
        from airflow.models import Variable

        if Variable.get("vendor") == "Google":
            import google_api_helper as cluster_api
        else:
            cluster_api = None

        if cluster_api is None:
            return

        ew_conn = get_connection("EasysegWorker")
        if not ew_conn:
            return

        deployment = ew_conn.host
        zone = ew_conn.login
        instance = f"{deployment}-easyseg-worker"

        redis_host = os.environ['REDIS_SERVER']
        timestamp = datetime.now().timestamp()
        r = redis.Redis(redis_host)
        r.set(instance, timestamp)

        slack_message(":exclamation:*Turn on easyseg worker*")
        retries = 0
        while True:
            try:
                cluster_api.toggle_easyseg_worker(on=True)
            except Exception:
                pass
            time.sleep(60)
            status = cluster_api.get_instance_property(zone, instance, "status")
            if status == "RUNNING":
                break
            retries += 1
            if retries > 5:
                slack_message(f":u7981:*ERROR: Failed to turn on {instance} for 5 minutes")
                break

    start_worker()


easyseg_dag()
