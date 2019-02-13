from airflow.operators.slack_operator import SlackAPIPostOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.base_hook import BaseHook

SLACK_CONN_ID = 'Slack'

def slack_message_op(dag, tid, msg):
    try:
        slack_username = BaseHook.get_connection(SLACK_CONN_ID).login
        slack_token = BaseHook.get_connection(SLACK_CONN_ID).password
        slack_channel = BaseHook.get_connection(SLACK_CONN_ID).extra
    except:
        return placeholder_op(dag, tid)

    if (not slack_username) or (not slack_token) or (not slack_channel):
        return placeholder_op(dag, tid)

    return SlackAPIPostOperator(
        task_id='slack_message_{}'.format(tid),
        username=slack_username,
        channel=slack_channel,
        token=slack_token,
        queue="manager",
        text=msg,
        dag=dag
    )

def placeholder_op(dag, tid):
    return DummyOperator(
        task_id = "dummy_{}".format(tid),
        dag=dag,
        queue = "manager"
    )


