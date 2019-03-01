from airflow.hooks.base_hook import BaseHook
from slackclient import SlackClient
from param_default import SLACK_CONN_ID

def slack_message(msg):
    try:
        slack_workername = BaseHook.get_connection(SLACK_CONN_ID).host
        slack_username = BaseHook.get_connection(SLACK_CONN_ID).login
        slack_token = BaseHook.get_connection(SLACK_CONN_ID).password
        slack_channel = BaseHook.get_connection(SLACK_CONN_ID).extra
    except:
        return

    sc = SlackClient(slack_token)

    rc = sc.api_call(
        "chat.postMessage",
        username=slack_workername,
        channel=slack_channel,
        text="<@{username}>, {message}".format(
            username=slack_username,
            message=msg
        )
    )

    if not rc["ok"]:
        print("Failed to send slack message")

def slack_userinfo():
    try:
        slack_username = BaseHook.get_connection(SLACK_CONN_ID).login
        slack_token = BaseHook.get_connection(SLACK_CONN_ID).password
    except:
        return None

    sc = SlackClient(slack_token)
    rc = sc.api_call(
        "users.info",
        user=slack_username
    )
    if rc["ok"]:
        username = rc["user"]["profile"]["display_name"]
        return username

    return None

def slack_alert(msg, context):
    text="""
        {msg}
        *Task*: {task}
        *Dag*: {dag}
        """.format(msg=msg,
        task=context.get('task_instance').task_id,
        dag=context.get('task_instance').dag_id,
        ti=context.get('task_instance'))

    return slack_message(text)


def task_start_alert(context):
    return slack_alert(":arrow_forward: Task Started", context)


def task_retry_alert(context):
    try_number = context.get('task_instance').try_number
    if try_number > 4:
        return slack_alert(":exclamation: Task up for retry: {}".format(try_number-1), context)


def task_done_alert(context):
    return slack_alert(":heavy_check_mark: Task Finished", context)
