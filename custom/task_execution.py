from airflow.models import Variable
from airflow.configuration import conf
import os
import time
import traceback
import socket
import json
import base64
import requests
from datetime import datetime
import redis
from concurrent.futures import ProcessPoolExecutor
from collections import namedtuple

import click
from kombu import Connection
from kombu.simple import SimpleQueue
from statsd import StatsClient
import psutil
import custom_worker

METADATA_URL = "http://metadata.google.internal/computeMetadata/v1/instance/"
METADATA_HEADERS = {'Metadata-Flavor': 'Google'}

CustomTask = namedtuple("CustomTask", ["message", "future"])


def get_hostname():
    try:
        data = requests.get(METADATA_URL + 'hostname', headers=METADATA_HEADERS).text
        return data.split(".")[0]
    except:
        return socket.gethostname()


@click.command()
@click.option('--queue', default="",  help='Name of pull queue to use.')
@click.option('--timeout', default=60,  help='SQS Queue URL if using SQS')
@click.option('--concurrency', default=0,  help='Number of tasks to process at the same time')
def command(queue, timeout, concurrency):
    qurl = conf.get('celery', 'broker_url')
    statsd_host = conf.get('metrics', 'statsd_host')
    statsd_port = conf.get('metrics', 'statsd_port')

    statsd = StatsClient(host=statsd_host, port=statsd_port)

    cv_secrets_path = os.path.join(os.path.expanduser('~'), ".cloudvolume/secrets")
    if not os.path.exists(cv_secrets_path):
        os.makedirs(cv_secrets_path)

    mount_secrets = Variable.get("mount_secrets", deserialize_json=True, default_var=[])

    for k in mount_secrets:
        v = Variable.get(k)
        with open(os.path.join(cv_secrets_path, k), 'w') as value_file:
            value_file.write(v)

    try:
        timeout = custom_worker.task_timeout
    except AttributeError:
        pass

    if concurrency == 0:
        concurrency = len(os.sched_getaffinity(0))
    execute(qurl, timeout, queue, statsd, concurrency)


def process_output(conn, queue_name, task):
    if not task.future.done():
        raise RuntimeError("Task not finished")

    ret = task.future.result()

    if ret == "done":
        task.message.ack()
        return
    elif isinstance(ret, dict):
        ret_queue = conn.SimpleQueue(queue_name+"_ret")
        err_queue = conn.SimpleQueue(queue_name+"_err")
        if ret.get('msg', None) == "done":
            if ret.get('ret', None):
                try:
                    ret_queue.put(json.dumps(ret['ret']))
                except TypeError:
                    err_queue.put("Cannot jsonify the result from the worker")
            task.message.ack()
        elif ret.get('msg', None) == "error":
            if ret.get('ret', None):
                err_queue.put(json.dumps(ret['ret']))
                raise RuntimeError("Task failed, restart the worker")
        else:
            raise RuntimeError("Unknown message from worker: {ret}")
    else:
        raise RuntimeError("Unknown message from worker: {ret}")


def update_concurrency(running_tasks, max_concurrency):
    if len(running_tasks) == 0:
        return max_concurrency

    concurrencies = [t.message.payload['metadata'].get('concurrency', max_concurrency) for t in running_tasks]
    return min(*concurrencies, max_concurrency)


def execute(qurl, timeout, queue_name, statsd, max_concurrency):
    with Connection(qurl, heartbeat=timeout) as conn, ProcessPoolExecutor(max_concurrency) as executor:
        queue = conn.SimpleQueue(queue_name)

        running_tasks = []

        concurrency = max_concurrency

        while True:
            task = 'unknown'
            try:
                while len(running_tasks) < concurrency:
                    message = queue.get_nowait()
                    print("put message into queue: {}".format(message.payload))
                    future = executor.submit(handle_task, message.payload, statsd)
                    running_tasks.append(CustomTask(message, future))
                    concurrency = update_concurrency(running_tasks, max_concurrency)
            except SimpleQueue.Empty:
                pass

            try:
                worker_heartbeat(conn, running_tasks, interval=30)
                done_tasks = [t for t in running_tasks if t.future.done()]
                for t in done_tasks:
                    process_output(conn, queue_name, t)
                if done_tasks:
                    running_tasks = [t for t in running_tasks if t not in done_tasks]
                    concurrency = update_concurrency(running_tasks, max_concurrency)
            except Exception as e:
                print('ERROR', task, "raised {}\n {}".format(e, traceback.format_exc()))
                conn.release()
                executor.shutdown()
                raise  # this will restart the container in kubernetes


def handle_task(msg, statsd):
    print("run task: {}".format(msg))
    statsd_task_key = msg['metadata'].get('statsd_task_key', 'task')
    try:
        with statsd.timer(f'{statsd_task_key}.duration'):
            val = custom_worker.process_task(msg['task'])
    except BaseException:
        val = traceback.format_exc()
        ret = {
            'msg': "error",
            'ret': base64.b64encode(val.encode("UTF-8")).decode("UTF-8")
        }
        return ret

    if val:
        ret = {
            'msg': "done",
            'ret': val
        }
        return ret
    else:
        return "done"


def worker_heartbeat(conn, running_tasks, interval):
    try:
        conn.drain_events(timeout=1)
    except socket.timeout:
        conn.heartbeat_check()

    cpu_usage = sum(psutil.cpu_percent(interval=1, percpu=True))
    if cpu_usage < 5:
        print("task stalled: {}".format(cpu_usage))
    else:
        print("task busy: {}".format(cpu_usage))
        redis_conn.set(hostname, datetime.now().timestamp())

    for _ in range(interval):
        if any(t.future.done() for t in running_tasks):
            return
        time.sleep(1)


if __name__ == '__main__':
    redis_conn = redis.Redis(os.environ["REDIS_SERVER"])
    hostname = get_hostname()
    command()
