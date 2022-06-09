def slack_message(msg, notification=False, broadcast=False, attachment=None):
    from param_default import SLACK_CONN_ID
    from airflow.hooks.base_hook import BaseHook
    import slack_sdk as slack
    import json
    try:
        slack_workername = BaseHook.get_connection(SLACK_CONN_ID).login
        slack_token = BaseHook.get_connection(SLACK_CONN_ID).password
        slack_extra = json.loads(BaseHook.get_connection(SLACK_CONN_ID).extra)
    except:
        return

    sc = slack.WebClient(slack_token, timeout=300)
    slack_username = slack_extra['user']
    slack_channel = slack_extra['channel']
    slack_thread = slack_extra['thread_ts']
    slack_notification_channel = slack_extra['notification_channel']

    if notification:
        text="{message}".format(
            message=msg
        )
        sc.chat_postMessage(
            username=slack_workername,
            channel=slack_notification_channel,
            text=text
        )
    else:
        text="<@{username}>, {message}".format(
            username=slack_username,
            message=msg
        )

        if attachment is None:
            sc.chat_postMessage(
                username=slack_workername,
                channel=slack_channel,
                thread_ts=slack_thread,
                reply_broadcast=broadcast,
                text=text
            )
        else:
            response = sc.files_upload(
                username=slack_workername,
                channels=slack_channel,
                thread_ts=slack_thread,
                file=attachment,
                initial_comment=text
            )
            print(response)


def slack_userinfo():
    from param_default import SLACK_CONN_ID
    from airflow.hooks.base_hook import BaseHook
    import slack_sdk as slack
    import json
    try:
        slack_extra = json.loads(BaseHook.get_connection(SLACK_CONN_ID).extra)
        slack_token = BaseHook.get_connection(SLACK_CONN_ID).password
    except:
        return None

    slack_username = slack_extra['user']

    sc = slack.WebClient(slack_token, timeout=600)
    rc = sc.users_info(
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
    from airflow.models import Variable
    import urllib.parse
    ti = context.get('task_instance')
    last_try = ti.try_number - 1
    if last_try > 0 and last_try % 5 == 0:
        iso = urllib.parse.quote(ti.execution_date.isoformat())
        webui_ip = Variable.get("webui_ip")
        log_url = "https://"+webui_ip + (
            "/airflow/log"
            "?dag_id={ti.dag_id}"
            "&task_id={ti.task_id}"
            "&execution_date={iso}"
        ).format(**locals())
        slack_alert(":exclamation: Task up for retry {} times already, check the latest error log: `{}`".format(last_try, log_url), context)

def task_failure_alert(context):
    from airflow.models import Variable
    import urllib.parse
    ti = context.get('task_instance')
    iso = urllib.parse.quote(ti.execution_date.isoformat())
    webui_ip = Variable.get("webui_ip")
    log_url = "https://"+webui_ip + (
        "/airflow/log"
        "?dag_id={ti.dag_id}"
        "&task_id={ti.task_id}"
        "&execution_date={iso}"
    ).format(**locals())
    slack_alert(":exclamation: Task failed, check the latest error log: `{}`".format(log_url), context)


def task_done_alert(context):
    return slack_alert(":heavy_check_mark: Task Finished", context)
