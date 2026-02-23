import os
import re
import functools
import json
import itertools
import difflib
import concurrent.futures
import time
import tenacity
from slack_sdk.rtm_v2 import RTMClient
from bot_info import botid, workerid, broker_url
from bot_utils import replyto, extract_command, update_slack_thread, create_run_token, send_message
from airflow_api import check_running, set_variable, get_variable
from kombu_helper import get_message, visible_messages
from docker_helper import get_registry_data


_help_trigger = ["help"]

def help_listener(context):
    cmd = extract_command(context['text'])
    if cmd in _help_trigger:
        replyto(context, "Here is a list of the commands supported:\n"+SeuronBot.generate_cmdlist())
        return True
    return False


def reset_run_metadata():
    default_run_metadata = {
            "track_resources": True,
            "manage_clusters": True,
    }
    set_variable("run_metadata", default_run_metadata, serialize_json=True)


def check_image_updates(context):
    if os.environ.get("VENDOR", None) != "Google":
        return
    image_info = get_variable("image_info", deserialize_json=True, default_var=None)
    if image_info:
        registry_data = get_registry_data(image_info["name"])
        if image_info["checksum"] != registry_data.attrs["Descriptor"]["digest"]:
            replyto(context, f":u7981:*WARNING*: Local image `{image_info['name']}` is outdated, your tasks may not run correctly, upgrade the bot ASAP!")


def run_cmd(func, context, exclusive=False, cancelable=False):
    if exclusive:
        reset_run_metadata()
        if check_running():
            replyto(context, "Busy right now")
            return
        if not context.get("from_jupyter", False):
            update_slack_thread(context)

    if cancelable:
        create_run_token(context)

    func(context)


class SeuronBot:

    message_listeners = [{
            "triggers": _help_trigger,
            "description": "Print help message",
            "exclusive": False,
            "extra_parameters": False,
            "file_inputs": False,
            "command": help_listener,
    }]

    hello_listeners = []

    def __init__(self, slack_token=None):
        self.task_owner = "seuronbot"
        self.slack_token = slack_token

        self.rtmclient = RTMClient(token=slack_token)
        self.rtmclient.on("message")(functools.partial(self.process_message.__func__, self))
        self.rtmclient.on("reaction_added")(functools.partial(self.process_reaction.__func__, self))
        self.rtmclient.on("hello")(functools.partial(self.process_hello.__func__, self))
        self.executor = concurrent.futures.ThreadPoolExecutor()

    def update_task_owner(self, msg):
        sc = self.rtmclient.web_client
        rc = sc.users_info(
            user=msg['user']
        )
        if rc["ok"]:
            self.task_owner = rc["user"]["profile"]["display_name"]

    @staticmethod
    def generate_command_description(cmd):
        command = " or ".join(f"`{x}`" for x in cmd["triggers"])
        if cmd['extra_parameters']:
            command += " + _parameters_"
        if cmd['file_inputs']:
            command += " + _input file/cell_"
        if cmd['description']:
            command += f"\n> {cmd['description']}"

        return command

    @classmethod
    def generate_cmdlist(cls):
        return "\n".join(cls.generate_command_description(listener) for listener in cls.message_listeners)

    def find_suggestions(self, context):
        cmd = extract_command(context['text'])
        candidates = list(itertools.chain(*[x['triggers'] for x in self.message_listeners]))

        return difflib.get_close_matches(cmd, candidates, n=2)

    def report(self, msg):
        print("preparing report!")
        if check_running():
            replyto(msg, f"{workerid}: busy running segmentation for {self.task_owner}", username="seuronbot", broadcast=True)
        else:
            replyto(msg, f"{workerid}: idle", username="seuronbot", broadcast=True)

    def filter_msg(self, msg):
        if 'subtype' in msg and msg['subtype'] != "file_share":
            return False
        if msg.get("hidden", False):
            return False
        if msg['text'] == "This message contains interactive elements.":
            if msg['blocks'][0]['elements'][0]['elements'][0]['type'] == 'text':
                msg['text'] = msg['blocks'][0]['elements'][0]['elements'][0]['text']
        text = msg["text"].strip('''_*~"'`''')

        if text.startswith(botid):
            cmd = extract_command(msg['text'])
            if cmd == "report":
                self.report(msg)
            return False

        if re.search(r"^{}[\s,:]".format(workerid), text, re.IGNORECASE):
            return True

    def process_message(self, client: RTMClient, event: dict):
        if self.filter_msg(event) or event.get("from_jupyter", False):
            handled = False
            check_image_updates(event)
            for listener in self.message_listeners:
                handled |= listener["command"](event)

            if not handled:
                reply_msg = "I do not understand the message"
                candidates = self.find_suggestions(event)
                if candidates:
                    reply_msg += "\nDo you mean "+ " or ".join(f"`{x}`" for x in candidates)
                replyto(event, reply_msg)
            else:
                if not event.get("from_jupyter", False):
                    self.update_task_owner(event)

    def process_reaction(self, client: RTMClient, event: dict):
        print("reaction added")
        print(json.dumps(event, indent=4))


    def process_hello(self, client: RTMClient, event: dict):
        for listener in self.hello_listeners:
            listener()

    @classmethod
    def on_hello(cls):
        def __call__(*args, **kwargs):
            func = args[0]

            def new_hello_listener():
                func()

            cls.hello_listeners.append(new_hello_listener)

            return func

        return __call__

    @classmethod
    def on_message(cls, trigger_phrases="", description="", exclusive=True, cancelable=True, extra_parameters=False, file_inputs=False):
        if isinstance(trigger_phrases, str):
            trigger_phrases = [trigger_phrases]

        def __call__(*args, **kwargs):
            func = args[0]
            trigger_cmds = ["".join(p.split()) for p in trigger_phrases]

            def new_message_listener(context):
                actual_cmd = extract_command(context['text'])
                if not extra_parameters:
                    if actual_cmd in trigger_cmds:
                        run_cmd(func, context, exclusive=exclusive, cancelable=cancelable)
                        return True
                else:
                    for c in trigger_cmds:
                        if actual_cmd.startswith(c):
                            run_cmd(func, context, exclusive=exclusive, cancelable=cancelable)
                            return True

                return False

            cls.message_listeners.append({
                "triggers": trigger_phrases,
                "description": description,
                "command": new_message_listener,
                "exclusive": exclusive,
                "cancelable": cancelable,
                "extra_parameters": extra_parameters,
                "file_inputs": file_inputs,
            })

            return func

        return __call__

    @tenacity.retry(
        retry=tenacity.retry_all(),
        wait=tenacity.wait_random_exponential(multiplier=1, max=60),
    )
    def fetch_bot_messages(self, queue="bot-message-queue"):
        client = self.rtmclient.web_client
        while True:
            msg_payload = get_message(broker_url, queue, timeout=30)
            if not msg_payload:
                continue

            while msg_count := visible_messages(broker_url, queue) > 0:
                for _ in range(msg_count):
                    next_msg = get_message(broker_url, queue)
                    if next_msg:
                        check_keys = ["notification", "broadcast", "attachment"]
                        if all(msg_payload.get(k, None) == next_msg.get(k, None) for k in check_keys):
                            msg_payload["text"] += "\n" + next_msg["text"]
                        else:
                            send_message(msg_payload, client=client)
                            time.sleep(1)
                            msg_payload = next_msg
                    else:
                        print("missing messages")
                time.sleep(5)

            send_message(msg_payload, client=client)


    def fetch_jupyter_messages(self, queue="jupyter-input-queue"):
        while True:
            msg_payload = get_message(broker_url, queue, timeout=30)
            if msg_payload:
                self.executor.submit(self.process_message, None, msg_payload)
                time.sleep(1)

    def start(self):
        futures = []
        futures.append(self.executor.submit(self.fetch_bot_messages))
        futures.append(self.executor.submit(self.fetch_jupyter_messages))
        try:
            self.rtmclient.start()
        except Exception:
            pass
        concurrent.futures.wait(futures)
