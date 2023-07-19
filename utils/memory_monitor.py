import sys
import socket
import logging
import psutil
from collections import namedtuple
from time import sleep
from kombu_helper import put_message
from google_metadata import gce_hostname


def run_oom_canary():
    ALERT_THRESHOLD = 0.95

    SleepCondition = namedtuple("SleepCondition", "threshold duration")

    SLEEP_CONDITIONS = [
            SleepCondition(0.5, 300),
            SleepCondition(0.8, 60),
            SleepCondition(0.9, 5),
            SleepCondition(0.95, 1),
    ]
    while True:
        mem_used = psutil.virtual_memory().percent
        logging.info(f"{mem_used}% memory used")
        mem_used /= 100
        if mem_used < 0 or mem_used > 1:
            sleep(1)
            continue
        if mem_used > ALERT_THRESHOLD:
            return
        for s in SLEEP_CONDITIONS:
            if mem_used < s.threshold:
                sleep(s.duration)
                break


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    try:
        hostname = gce_hostname().split(".")[0]
    except:
        hostname = socket.gethostname()

    msg_payload = {
        'text': f":u6e80: *OOM detected from instance* `{hostname}`!"
    }

    run_oom_canary()
    logging.warning("canary died")
    put_message(sys.argv[1], sys.argv[2], msg_payload)
