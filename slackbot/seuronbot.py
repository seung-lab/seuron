import re
import functools
import json
import itertools
import difflib
import concurrent.futures
import time
from slack_sdk.rtm_v2 import RTMClient
from bot_info import botid, workerid, broker_url
from bot_utils import replyto, extract_command, update_slack_thread, create_run_token, send_slack_message
from airflow_api import check_running
from kombu_helper import get_message


_help_trigger = ["help"]

def help_listener(context):
    cmd = extract_command(context['text'])
    if cmd in _help_trigger:
        replyto(context, "Here is a list of the commands supported:\n"+SeuronBot.generate_cmdlist())
        return True
    return False


def run_cmd(func, context, exclusive=False, cancelable=False):
    if exclusive:
        if check_running():
            replyto(context, "Busy right now")
            return
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
            command += " + _input file_"
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
        if self.filter_msg(event):
            print(json.dumps(event, indent=4))
            handled = False
            for listener in self.message_listeners:
                handled |= listener["command"](event)

            if not handled:
                reply_msg = "I do not understand the message"
                candidates = self.find_suggestions(event)
                if candidates:
                    reply_msg += "\nDo you mean "+ " or ".join(f"`{x}`" for x in candidates)
                replyto(event, reply_msg)
            else:
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

    def fetch_bot_messages(self, queue="bot-message-queue"):
        client = self.rtmclient.web_client
        while True:
            msg_payload = get_message(broker_url, queue, timeout=30)
            if msg_payload:
                try:
                    send_slack_message(msg_payload, client=client)
                except:
                    pass
                time.sleep(1)

    def start(self):
        self.executor.submit(self.fetch_bot_messages)
        self.rtmclient.start()
