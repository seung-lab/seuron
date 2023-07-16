from os import environ
import slack_sdk as slack


def get_botid():
    try:
        client = slack.WebClient(token=slack_token)
        auth_info = client.auth_test()
        return f'<@{auth_info["user_id"]}>'
    except Exception:
        return ""

slack_token = environ["SLACK_TOKEN"]
slack_notification_channel = environ["SLACK_NOTIFICATION_CHANNEL"]
botid = get_botid()
workerid = "seuron-worker-"+environ["DEPLOYMENT"]
broker_url = environ['AIRFLOW__CELERY__BROKER_URL']
