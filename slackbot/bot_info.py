from os import environ
import slack_sdk as slack


def get_botid():
    client = slack.WebClient(token=slack_token)
    auth_info = client.auth_test()
    return f'<@{auth_info["user_id"]}>'

slack_token = environ["SLACK_TOKEN"]
botid = get_botid()
workerid = "seuron-worker-"+environ["DEPLOYMENT"]
broker_url = environ['AIRFLOW__CELERY__BROKER_URL']
