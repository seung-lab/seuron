import string
from secrets import token_hex
import slack_sdk as slack
from bot_info import slack_token, botid, workerid
from airflow_api import update_slack_connection, set_variable

def extract_command(msg):
    cmd = msg["text"].replace(workerid, "").replace(botid, "")
    cmd = cmd.translate(str.maketrans('', '', string.punctuation))
    cmd = cmd.lower()
    return "".join(cmd.split())


def replyto(msg, reply, username=workerid, broadcast=False):
    sc = slack.WebClient(slack_token, timeout=300)
    channel = msg['channel']
    userid = msg['user']
    thread_ts = msg['thread_ts'] if 'thread_ts' in msg else msg['ts']
    reply_msg = f"<@{userid}> {reply}"
    rc = sc.chat_postMessage(
        username=username,
        channel=channel,
        thread_ts=thread_ts,
        reply_broadcast=broadcast,
        text=reply_msg
    )
    if not rc["ok"]:
        print("Failed to send slack message")
        print(rc)


def update_slack_thread(msg):
    payload = {
        'user': msg['user'],
        'channel': msg['channel'],
        'thread_ts': msg['thread_ts'] if 'thread_ts' in msg else msg['ts']
    }
    update_slack_connection(payload, slack_token)


def create_run_token(msg):
    token = token_hex(16)
    set_variable("run_token", token)
    sc = slack.WebClient(slack_token, timeout=300)
    userid = msg['user']
    reply_msg = "use `{}, cancel run {}` to cancel the current run".format(workerid, token)
    rc = sc.chat_postMessage(
        channel=userid,
        text=reply_msg
    )
    if not rc["ok"]:
        print("Failed to send direct message")
        print(rc)
