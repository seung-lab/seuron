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
    from common.redis_utils import AdaptiveRateLimiter
    from airflow.utils.log.log_reader import TaskLogReader

    REDIS_LLM_DB = "LLM"
    ti = context.get("task_instance")
    last_try = ti.try_number - 1
    if last_try > 0:
        iso = urllib.parse.quote(ti.execution_date.isoformat())
        webui_ip = Variable.get("webui_ip", default_var="localhost")
        log_url = "https://"+webui_ip + (
            "/airflow/log"
            "?dag_id={ti.dag_id}"
            "&task_id={ti.task_id}"
            "&execution_date={iso}"
        ).format(**locals())

        limiter = AdaptiveRateLimiter(REDIS_LLM_DB)
        rate_limit_key = ti.dag_id

        is_allowed, rejections_count = limiter.is_allowed(rate_limit_key)

        if is_allowed:
            slack_alert(
                f":exclamation: Task up for retry {last_try} times already, <{log_url}|check the latest error log>",
                context,
            )
            summary = ""
            if rejections_count > 0:
                summary = f"\nThere have been {rejections_count} retries since the last analysis.\n"

            task_log_reader = TaskLogReader()
            metadata = {}
            current_error_message = " ".join([
                text
                for text in task_log_reader.read_log_stream(
                    ti, ti.try_number - 1, metadata
                )
            ])

            if current_error_message:
                if "No logs found in GCS" in current_error_message and "Found local files" not in current_error_message:
                    pass
                elif current_error_message.endswith("Task is not able to be run"):
                    pass
                else:
                    parsed_msg = interpret_error_message(current_error_message)
                    if parsed_msg:
                        slack_message(parsed_msg + summary)
            else:
                pass

def interpret_error_message(error_message):
    import re
    import os
    from langchain_google_vertexai import ChatVertexAI
    from langchain_core.prompts import ChatPromptTemplate
    from airflow.hooks.base_hook import BaseHook
    from airflow.models import Variable

    file_path_match = re.findall(r'File "([^"]+)", line \d+, in', error_message)
    if file_path_match:
        file_path = file_path_match[-1]
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                source_code = f.read()
            error_message += f"""\n\n<source_code file_path="{file_path}">
{source_code}
</source_code>\n"""

    for p in ["inference_param", "param", "synaptor_param.json", "training_param", "custom_script"]:
        param = Variable.get(p, default_var="")
        error_message += f"""\n\n<parameter name="{p}">
{param}
</parameter>\n"""

    error_parse_prompt = ChatPromptTemplate.from_messages([
        (
            "system",
            """You are an expert in Airflow and distributed systems debugging. I will provide you with raw Airflow job logs and the source code of the file that created the error. Your task is to:
Identify and extract the main error message(s), including relevant stack traces and error codes.
Summarize the error in plain English.
Suggest the most likely root causes.

Propose step-by-step fixes or debugging actions that could resolve the issue (e.g., DAG misconfiguration, dependency issue, missing package, environment/path problem, operator failure, connection misconfiguration, resource limits, etc.).
Please output your answer in the following structured format, use markdown syntax for slack:

Error Extracted:
[copy/paste of the key error lines]

Summary:
[short explanation in plain English]

Likely Root Causes:
[cause 1]
[cause 2]

Suggested Fixes:
[fix 1]
[fix 2]
[fix 3]
""",
        ),
        ("human", "Airflow error log: {input}"),
    ])

    llm_model = ChatVertexAI(model="gemini-2.5-pro", temperature=0.5)
    error_parse_chain = error_parse_prompt | llm_model
    try:
        msg = error_parse_chain.invoke({"input": error_message})
        return msg.content
    except Exception:
        return None


def task_failure_alert(context):
    import urllib.parse
    from sqlalchemy import select
    from airflow.models import Variable
    from airflow.utils.log.log_reader import TaskLogReader

    ti = context.get("task_instance")
    iso = urllib.parse.quote(ti.execution_date.isoformat())
    webui_ip = Variable.get("webui_ip", default_var="localhost")
    log_url = f"https://{webui_ip}/airflow/log?dag_id={ti.dag_id}&task_id={ti.task_id}&execution_date={iso}"
    slack_alert(f":exclamation: Task failed, <{log_url}|check the latest error log>", context)

    task_log_reader = TaskLogReader()

    if ti.queue == "manager":
        metadata = {}
        error_message = " ".join([
            text
            for text in task_log_reader.read_log_stream(ti, ti.try_number, metadata)
        ])
        if "No logs found in GCS" in error_message and "Found local files" not in error_message:
            pass
        elif error_message.endswith("Task is not able to be run"):
            pass
        else:
            parsed_msg = interpret_error_message(error_message)
            if parsed_msg:
                slack_message(parsed_msg)

def task_done_alert(context):
    return slack_alert(":heavy_check_mark: Task Finished", context)
