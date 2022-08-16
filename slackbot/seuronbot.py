import re
import functools
import json
from secrets import token_hex
from slack_sdk.rtm_v2 import RTMClient
from bot_info import botid, workerid
from bot_utils import replyto, extract_command
from airflow_api import check_running, update_slack_connection, set_variable

class SeuronBot:
    def __init__(self, slack_token=None):
        self.help_listener = {
            "triggers": ["help"],
            "description": "Print help message",
            "exclusive": False,
            "extra_parameters": False,
            "file_inputs": False,
        }
        self.task_owner = "seuronbot"
        self.slack_token = slack_token

        trigger_cmds = ["".join(p.split()) for p in self.help_listener['triggers']]

        def help_listener(context):
            cmd = extract_command(context)
            if cmd in trigger_cmds:
                replyto(context, "Here is a list of the commands supported:\n"+self.generate_cmdlist())
                return True
            return False

        self.help_listener['command'] = help_listener

        self.message_listeners = [self.help_listener]

        self.hello_listeners = []

        self.rtmclient = RTMClient(token=slack_token)
        self.rtmclient.on("message")(functools.partial(self.process_message.__func__, self))
        self.rtmclient.on("reaction_added")(functools.partial(self.process_reaction.__func__, self))
        self.rtmclient.on("hello")(functools.partial(self.process_hello.__func__, self))

    def update_metadata(self, msg):
        sc = self.rtmclient.web_client
        payload = {
            'user': msg['user'],
            'channel': msg['channel'],
            'thread_ts': msg['thread_ts'] if 'thread_ts' in msg else msg['ts']
        }
        update_slack_connection(payload, self.slack_token)
        rc = sc.users_info(
            user=msg['user']
        )
        if rc["ok"]:
            self.task_owner = rc["user"]["profile"]["display_name"]

    def create_run_token(self, msg):
        token = token_hex(16)
        set_variable("run_token", token)
        sc = self.rtmclient.web_client
        userid = msg['user']
        reply_msg = "use `{}, cancel run {}` to cancel the current run".format(workerid, token)
        rc = sc.chat_postMessage(
            channel=userid,
            text=reply_msg
        )
        if not rc["ok"]:
            print("Failed to send direct message")
            print(rc)

    def generate_command_description(self, cmd):
        command = " or ".join(f"`{x}`" for x in cmd["triggers"])
        if cmd['extra_parameters']:
            command += " + _parameters_"
        if cmd['file_inputs']:
            command += " + _input file_"
        if cmd['description']:
            command += f"\n> {cmd['description']}"

        return command

    def generate_cmdlist(self):
        return "\n".join(self.generate_command_description(listener) for listener in self.message_listeners)

    def report(self, msg):
        print("preparing report!")
        if check_running():
            replyto(msg, f"{workerid}: busy running segmentation for {self.task_owner}", username="seuronbot", broadcast=True)
        else:
            replyto(msg, f"{workerid}: idle", username="seuronbot", broadcast=True)

    def filter_msg(self, msg):
        if 'subtype' in msg and msg['subtype'] != "file_share":
            return False
        text = msg["text"].strip('''_*~"'`''')

        if text.startswith(botid):
            cmd = extract_command(msg)
            if cmd == "report":
                self.report(msg)
            return False

        if re.search(r"^{}[\s,:]".format(workerid), text, re.IGNORECASE):
            return True

    def process_message(self, client: RTMClient, event: dict):
        print(json.dumps(event, indent=4))
        if self.filter_msg(event):
            handled = False
            for listener in self.message_listeners:
                handled |= listener["command"](event)

            if not handled:
                replyto(event, "Do not understand command")

    def process_reaction(self, client: RTMClient, event: dict):
        print("reaction added")
        print(json.dumps(event, indent=4))


    def process_hello(self, client: RTMClient, event: dict):
        for listener in self.hello_listeners:
            listener()

    def run_cmd(self, func, context, exclusive=False, cancelable=False):
        if exclusive:
            if check_running():
                replyto(context, "Busy right now")
                return
            self.update_metadata(context)

        func(context)

        if cancelable:
            self.create_run_token(context)

    def on_hello(self):
        def __call__(*args, **kwargs):
            func = args[0]

            def new_hello_listener():
                func()

            self.hello_listeners.append(new_hello_listener)

            return func

        return __call__

    def on_message(self, trigger_phrases="", description="", exclusive=True, cancelable=True, extra_parameters=False, file_inputs=False):
        if isinstance(trigger_phrases, str):
            trigger_phrases = [trigger_phrases]

        def __call__(*args, **kwargs):
            func = args[0]
            trigger_cmds = ["".join(p.split()) for p in trigger_phrases]

            def new_message_listener(context):
                actual_cmd = extract_command(context)
                if not extra_parameters:
                    if actual_cmd in trigger_cmds:
                        self.run_cmd(func, context, exclusive=exclusive, cancelable=cancelable)
                        return True
                else:
                    for c in trigger_cmds:
                        if actual_cmd.startswith(c):
                            self.run_cmd(func, context, exclusive=exclusive, cancelable=cancelable)
                            return True

                return False

            self.message_listeners.append({
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

    def start(self):
        self.rtmclient.start()
