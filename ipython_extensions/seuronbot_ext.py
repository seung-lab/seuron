import asyncio
import functools
import logging
import base64
import fasteners
from slack_down import slack_md
import markdown
from bs4 import BeautifulSoup
import contextvars

from IPython.core.magic import (Magics, magics_class, line_cell_magic)
from IPython.display import display, HTML, Image, IFrame

from kombu_helper import get_message, put_message, drain_messages

def embed_links(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    link_tags = soup.find_all('a', href=True)

    for link_tag in link_tags:
        href = link_tag['href']
        display(IFrame(src=href, width=1200, height=600))


@magics_class
class SeuronBot(Magics):
    def __init__(self, shell):
        super(SeuronBot, self).__init__(shell)
        self.broker_url = "amqp://rabbitmq"
        self.output_queue = "jupyter-output-queue"
        self.input_queue = "jupyter-input-queue"
        self.common_block = ""
        self.slack_conv = markdown.Markdown(extensions=[slack_md.SlackExtension()])
        self.last_cell_context = None
        drain_messages(self.broker_url, self.input_queue, verbose=False)
        drain_messages(self.broker_url, self.output_queue, verbose=False)
        self.forwarder_task = asyncio.create_task(self.forward_bot_message())

    def _display_message(self, msg_payload):
        # This function will be run inside the captured context
        html_content = self.slack_conv.convert(msg_payload.get("text", ""))
        display(HTML(html_content))
        embed_links(html_content)
        attachment = msg_payload.get("attachment", None)
        if attachment:
            if attachment.get("filetype") in ["png", "jpeg", "gif"]:
                display(Image(
                    data=base64.b64decode(attachment["content"]),
                    format=attachment["filetype"]
                ))

    @line_cell_magic
    def seuronbot(self, command, cell=None):
        if command == "define common block":
            if cell:
                self.common_block = self.shell.input_transformer_manager.transform_cell(cell)
                exec(self.common_block, globals())
            else:
                self.common_block = ""
                print("Emptying seuronbot common block")
            return

        # Capture the context of the current cell
        self.last_cell_context = contextvars.copy_context()

        msg_payload = {
                "text": command,
                "from_jupyter": True,
        }
        if cell:
            code_content = self.common_block + "\n" + self.shell.input_transformer_manager.transform_cell(cell)
            msg_payload["attachment"] = base64.b64encode(code_content.encode("utf-8")).decode("utf-8")

        put_message(self.broker_url, self.input_queue, msg_payload)

    async def forward_bot_message(self):
        loop = asyncio.get_event_loop()
        get_with_timeout = functools.partial(get_message, timeout=30)
        while True:
            try:
                msg_payload = await loop.run_in_executor(
                    None, get_with_timeout, self.broker_url, self.output_queue
                )
                if msg_payload:
                    if self.last_cell_context:
                        # Run _display_message within the captured context
                        self.last_cell_context.run(self._display_message, msg_payload)
                    else:
                        # Fallback if no magic has been run yet
                        self._display_message(msg_payload)
                await asyncio.sleep(0.1)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logging.error(f"Error in forward_bot_message: {e}")
                await asyncio.sleep(5)

    def __del__(self):
        if hasattr(self, 'forwarder_task'):
            self.forwarder_task.cancel()


for k in logging.Logger.manager.loggerDict:
    logging.getLogger(k).setLevel(logging.CRITICAL)

lock = fasteners.InterProcessLock('/run/lock/seuronbot.lock')
if lock.acquire(timeout=10):
    lock_acquired = True
else:
    lock_acquired = False


def load_ipython_extension(ipython):
    if lock_acquired:
        seuronbot = SeuronBot(ipython)
        ipython.register_magics(seuronbot)
        print("'seuronbot' magic loaded.")
        print('Use "%seuronbot help" to list all available commands')
        print('Use cell magic "%%seuronbot" for commands requiring additinoal cell input')
    else:
        print("Another seuronbot is already running")
