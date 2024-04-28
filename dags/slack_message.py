def slack_message(msg, notification=False, broadcast=False, attachment=None):
    from airflow import configuration as conf
    import kombu_helper
    msg_payload = {
            'text': msg,
            'notification': notification,
            'broadcast': broadcast,
            'attachment': attachment,
    }
    broker_url = conf.get('celery', 'broker_url')
    kombu_helper.put_message(broker_url, 'bot-message-queue', msg_payload)


def slack_userinfo():
    from param_default import SLACK_CONN_ID
    from airflow.hooks.base_hook import BaseHook
    import slack_sdk as slack
    import json
    try:
        slack_extra = json.loads(BaseHook.get_connection(SLACK_CONN_ID).extra)
        slack_token = BaseHook.get_connection(SLACK_CONN_ID).password
        slack_username = slack_extra['user']

        sc = slack.WebClient(slack_token, timeout=600)
        rc = sc.users_info(
            user=slack_username
        )
        if rc["ok"]:
            username = rc["user"]["profile"]["display_name"]
            return username
        else:
            return None
    except:
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
        webui_ip = Variable.get("webui_ip", default_var="localhost")
        log_url = "https://"+webui_ip + (
            "/airflow/log"
            "?dag_id={ti.dag_id}"
            "&task_id={ti.task_id}"
            "&execution_date={iso}"
        ).format(**locals())
        slack_alert(f":exclamation: Task up for retry {last_try} times already, <{log_url}|check the latest error log>", context)


def interpret_error_message(error_message):
    from langchain_openai import ChatOpenAI
    from airflow.hooks.base_hook import BaseHook

    try:
        conn = BaseHook.get_connection("LLMServer")
        base_url = conn.host
        api_key = conn.password
        extra_args = conn.extra_dejson
    except Exception:
        return None

    model = ChatOpenAI(base_url=base_url, api_key=api_key, model=extra_args.get("model", "gpt-3.5-turbo"))
    messages = [
        ("system", "You are a helpful assistant, identify and explain the error message in a few words"),
        ("human", error_message),
    ]
    try:
        msg = model.invoke(messages)
        return msg.content
    except Exception:
        return None


def task_failure_alert(context):
    import urllib.parse
    from sqlalchemy import select
    from airflow.models import Variable
    from airflow.utils.log.log_reader import TaskLogReader
    ti = context.get('task_instance')
    last_try = ti.try_number - 1
    iso = urllib.parse.quote(ti.execution_date.isoformat())
    webui_ip = Variable.get("webui_ip", default_var="localhost")
    log_url = f"https://{webui_ip}/airflow/log?dag_id={ti.dag_id}&task_id={ti.task_id}&execution_date={iso}"
    slack_alert(f":exclamation: Task failed, <{log_url}|check the latest error log>", context)

    task_log_reader = TaskLogReader()

    if ti.queue == "manager":
        metadata = {}
        error_message = []
        for text in task_log_reader.read_log_stream(ti, last_try, metadata):
            lines = text.split("\n")
            targets = ['error', 'traceback', 'exception']
            for i, l in enumerate(lines):
                if any(x in l.lower() for x in targets):
                    error_message = "\n".join(lines[i:i+10])
                    break
        parsed_msg = interpret_error_message(error_message)
        if parsed_msg:
            slack_message(interpret_error_message(error_message))
        else:
            slack_message(f"Failed to use the LLM server to interpret the error message ```{error_message}```")


def task_done_alert(context):
    return slack_alert(":heavy_check_mark: Task Finished", context)
