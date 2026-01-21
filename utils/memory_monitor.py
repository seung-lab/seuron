import sys
import socket
import logging
import psutil
from enum import Enum
import os
import redis
from datetime import datetime, timedelta
from time import sleep
from kombu_helper import put_message
from common.google_api import gce_hostname, get_project_id, get_zone, delete_instances, get_created_by

class RedisHealthMonitor:
    def __init__(self, timeout=timedelta(seconds=1800)):
        self.last_success_time = datetime.now()
        self.timeout = timeout

    def record_success(self):
        self.last_success_time = datetime.now()

    def record_failure(self):
        if datetime.now() - self.last_success_time > self.timeout:
            logging.error("Redis has been unavailable for 30 minutes. Deleting instance from group.")
            delete_self_from_instance_group()
            sys.exit(1)

class InstanceError(Enum):
    OOM = 1
    DISKFULL = 2

OOM_ALERT_PERCENT_THRESHOLD = 95
DISKFULL_ALERT_PERCENT_THRESHOLD = 90


def delete_self_from_instance_group():
    try:
        hostname = gce_hostname().split('.')[0]
        zone = get_zone()
        project_id = get_project_id()

        created_by = get_created_by()
        igm_name = created_by.split('/')[-1]

        ig = {'zone': zone, 'name': igm_name}
        instance_url = f"https://www.googleapis.com/compute/v1/projects/{project_id}/zones/{zone}/instances/{hostname}"

        logging.info(f"Deleting instance {hostname} from instance group {igm_name}")
        delete_instances(ig, [instance_url])
        put_message(sys.argv[1], sys.argv[2], {
            "text": f"{hostname} failed to connect to the manager for 60 minutes. Deleting instance from group."
            }
        )
    except Exception as e:
        logging.error(f"Failed to delete instance from instance group: {e}")


def check_heartbeat(redis_conn, redis_monitor):
    try:
        last_heartbeat = redis_conn.get('heartbeat_dag_last_success_timestamp')
        if redis_monitor:
            redis_monitor.record_success()
        if last_heartbeat:
            last_heartbeat_time = datetime.fromtimestamp(float(last_heartbeat))
            if datetime.now() - last_heartbeat_time > timedelta(hours=1):
                logging.warning("Heartbeat is older than 1 hour. Deleting instance from group.")
                delete_self_from_instance_group()
    except Exception as e:
        logging.warning(f"Failed to read from Redis: {e}")
        if redis_monitor:
            redis_monitor.record_failure()


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


def run_oom_canary(redis_conn, hostname, redis_monitor):
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
                check_heartbeat(redis_conn, redis_monitor)
                if check_filesystems_full():
                    return InstanceError.DISKFULL
                cpu_usage = sum(psutil.cpu_percent(interval=1, percpu=True))
                if cpu_usage > 20:
                    logging.info(f"{cpu_usage}% cpu used, heartbeat")
                    try:
                        redis_conn.set(hostname, datetime.now().timestamp())
                        if redis_monitor:
                            redis_monitor.record_success()
                    except Exception as e:
                        logging.warning(f"Failed to write to Redis: {e}")
                        if redis_monitor:
                            redis_monitor.record_failure()
                    continue
                counters_start = psutil.net_io_counters()
                sleep(1)
                counters_end = psutil.net_io_counters()
                download_speed = counters_end.bytes_recv - counters_start.bytes_recv
                upload_speed = counters_end.bytes_sent - counters_start.bytes_sent
                if download_speed > 1e6 or upload_speed > 1e6:
                    logging.info(f"Significant network IO: {download_speed/1e6}MB/s, {upload_speed/1e6}MB/s, heartbeat")
                    try:
                        redis_conn.set(hostname, datetime.now().timestamp())
                        if redis_monitor:
                            redis_monitor.record_success()
                    except Exception as e:
                        logging.warning(f"Failed to write to Redis: {e}")
                        if redis_monitor:
                            redis_monitor.record_failure()
                    continue
            else:
                sleep(1)
        else:
            sleep(t)



if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    redis_conn = None
    start_time = datetime.now()

    try:
        hostname = gce_hostname().split(".")[0]
    except:
        hostname = socket.gethostname()

    while datetime.now() - start_time < timedelta(seconds=1800):  # 30 minutes
        try:
            redis_conn = redis.Redis(os.environ["REDIS_SERVER"], socket_connect_timeout=5, decode_responses=True)
            redis_conn.ping()  # Check if connection is alive
            logging.info("Successfully connected to Redis.")
            break
        except Exception as e:
            logging.error(f"An unexpected error occurred while connecting to Redis: {e}")
            sleep(10)

    if redis_conn is None:
        logging.error("Failed to connect to Redis for 30 minutes. Deleting instance from group.")
        delete_self_from_instance_group()
        sys.exit(1)

    redis_monitor = RedisHealthMonitor()

    error_message = {
        InstanceError.OOM:          {
                            'text': f":u6e80: *OOM detected from instance* `{hostname}`!"
                        },
        InstanceError.DISKFULL:     {
                            'text': f":u6e80: *instance* `{hostname}` *disk FULL!*"
                        },
    }

    exit_reason = run_oom_canary(redis_conn, hostname, redis_monitor)
    logging.warning("canary died")
    put_message(sys.argv[1], sys.argv[2], error_message[exit_reason])
