import sys
import traceback
import subprocess
from seuronbot import SeuronBot
from bot_utils import replyto, download_file
from airflow_api import set_variable


@SeuronBot.on_message(["update python package", "update python packages"],
                      description="Install extra python packages before starting the docker containers",
                      file_inputs=True,
                      cancelable=False)
def on_update_python_packages(msg):
    _, payload = download_file(msg)
    replyto(msg, "*WARNING:Extra python packages are available for workers only*")
    if payload:
        for l in payload.splitlines():
            replyto(msg, f"Testing python packages *{l}*")
            try:
                install_package(l)
            except:
                replyto(msg, f":u7981:Failed to install package *{l}*")
                replyto(msg, "{}".format(traceback.format_exc()))
                return

        set_variable('python_packages', payload)
        replyto(msg, "Packages are ready for *workers*")


def install_package(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])
