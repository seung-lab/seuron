import sys
import socket
import logging
import psutil
import os
import redis
from datetime import datetime
from collections import namedtuple
from time import sleep
from kombu_helper import put_message
from google_metadata import gce_hostname


# Algorithm similar to earlyoom
def sleep_time(mem_avail):
    max_mem_fill_rate = 6_000_000_000  # 6000MB/s
    min_sleep = 0.1
    sleep_time = mem_avail / max_mem_fill_rate
    return max(min_sleep, sleep_time)


def run_oom_canary():
    ALERT_THRESHOLD = 0.95

    loop_counter = 0
    while True:
        loop_counter += 1
        mem = psutil.virtual_memory()
        mem_used = mem.percent

        if loop_counter % 300 == 0:
            logging.info(f"{mem_used}% memory used")

        mem_used /= 100
        if mem_used < 0 or mem_used > 1:
            sleep(1)
            continue
        if mem_used > ALERT_THRESHOLD:
            return

        t = sleep_time(mem.available)
        if t > 1:
            if loop_counter % 60 == 0:
                cpu_usage = sum(psutil.cpu_percent(interval=1, percpu=True))
                if cpu_usage > 20:
                    logging.info(f"{cpu_usage}% cpu used, heartbeat")
                    redis_conn.set(hostname, datetime.now().timestamp())
            sleep(1)
        else:
            sleep(t)



if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    redis_conn = redis.Redis(os.environ["REDIS_SERVER"])
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
