import sys
import socket
import logging
import psutil
from enum import Enum
import os
import redis
from datetime import datetime
from time import sleep
from kombu_helper import put_message
from google_metadata import gce_hostname


class InstanceError(Enum):
    OOM = 1
    DISKFULL = 2

OOM_ALERT_PERCENT_THRESHOLD = 95
DISKFULL_ALERT_PERCENT_THRESHOLD = 90


def check_filesystems_full():
    partitions = psutil.disk_partitions()

    for partition in partitions:
        usage = psutil.disk_usage(partition.mountpoint)
        logging.info(f"Filesystem: {partition.device}")
        logging.info(f"  Mountpoint: {partition.mountpoint}")
        logging.info(f"  Total: {usage.total // (2**20)} MB")
        logging.info(f"  Used: {usage.used // (2**20)} MB")
        logging.info(f"  Free: {usage.free // (2**20)} MB")
        logging.info(f"  Usage: {usage.percent}%")
        if usage.percent > DISKFULL_ALERT_PERCENT_THRESHOLD:  # Change threshold as needed
            return True

    return False


# Algorithm similar to earlyoom
def sleep_time(mem_avail):
    max_mem_fill_rate = 6_000_000_000  # 6000MB/s
    min_sleep = 0.1
    sleep_time = mem_avail / max_mem_fill_rate
    return max(min_sleep, sleep_time)


def run_oom_canary():
    loop_counter = 0
    while True:
        loop_counter += 1
        mem = psutil.virtual_memory()
        mem_used_percent = mem.percent

        if loop_counter % 300 == 0:
            logging.info(f"{mem_used_percent}% memory used")

        if mem_used_percent < 0 or mem_used_percent > 100:
            sleep(1)
            continue
        if mem_used_percent > OOM_ALERT_PERCENT_THRESHOLD:
            return InstanceError.OOM

        t = sleep_time(mem.available)
        if t > 1:
            if loop_counter % 60 == 0:
                if check_filesystems_full():
                    return InstanceError.DISKFULL
                cpu_usage = sum(psutil.cpu_percent(interval=1, percpu=True))
                if cpu_usage > 20:
                    logging.info(f"{cpu_usage}% cpu used, heartbeat")
                    redis_conn.set(hostname, datetime.now().timestamp())
                    continue
                counters_start = psutil.net_io_counters()
                sleep(1)
                counters_end = psutil.net_io_counters()
                download_speed = counters_end.bytes_recv - counters_start.bytes_recv
                upload_speed = counters_end.bytes_sent - counters_start.bytes_sent
                if download_speed > 1e6 or upload_speed > 1e6:
                    logging.info(f"Significant network IO: {download_speed/1e6}MB/s, {upload_speed/1e6}MB/s, heartbeat")
                    redis_conn.set(hostname, datetime.now().timestamp())
                    continue
            else:
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

    error_message = {
        InstanceError.OOM:          {
                            'text': f":u6e80: *OOM detected from instance* `{hostname}`!"
                        },
        InstanceError.DISKFULL:     {
                            'text': f":u6e80: *instance* `{hostname}` *disk FULL!*"
                        },
    }

    exit_reason = run_oom_canary()
    logging.warning("canary died")
    put_message(sys.argv[1], sys.argv[2], error_message[exit_reason])
