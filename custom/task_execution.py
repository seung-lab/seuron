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

import click
from kombu import Connection
from kombu.simple import SimpleQueue
from statsd import StatsClient
import psutil
import custom_worker

METADATA_URL = "http://metadata.google.internal/computeMetadata/v1/instance/"
METADATA_HEADERS = {'Metadata-Flavor': 'Google'}


def get_hostname():
    data = requests.get(METADATA_URL + 'hostname', headers=METADATA_HEADERS).text
    return data.split(".")[0]


@click.command()
@click.option('--queue', default="",  help='Name of pull queue to use.')
@click.option('--timeout', default=60,  help='SQS Queue URL if using SQS')
def command(queue, timeout):
    qurl = conf.get('celery', 'broker_url')
    statsd_host = conf.get('metrics', 'statsd_host')
    statsd_port = conf.get('metrics', 'statsd_port')

    statsd = StatsClient(host=statsd_host, port=statsd_port)

    param = Variable.get("param", deserialize_json=True)
    cv_secrets_path = os.path.join(os.path.expanduser('~'), ".cloudvolume/secrets")
    if not os.path.exists(cv_secrets_path):
        os.makedirs(cv_secrets_path)

    mount_secrets = param.get("MOUNT_SECRETS", [])

    for k in mount_secrets:
        v = Variable.get(k)
        with open(os.path.join(cv_secrets_path, k), 'w') as value_file:
            value_file.write(v)

    try:
        timeout = custom_worker.task_timeout
    except:
        pass

    conn = Connection(qurl, heartbeat=timeout)
    execute(conn, queue, qurl, statsd)
    conn.release()
    return


def execute(conn, queue_name, qurl, statsd):
    print("Pulling from {}".format(qurl))
    queue = conn.SimpleQueue(queue_name)
    ret_queue = conn.SimpleQueue(queue_name+"_ret")
    err_queue = conn.SimpleQueue(queue_name+"_err")
    executor = ProcessPoolExecutor()
    futures = []

    while True:
        task = 'unknown'
        try:
            message = queue.get_nowait()
            print("put message into queue: {}".format(message.payload))
            futures.append(executor.submit(handle_task, message.payload, statsd))
            if wait_for_task(futures, ret_queue, err_queue, conn):
                print("delete task in queue...")
                message.ack()
                print('INFO', task, "succesfully executed")
            else:
                break
            for f in futures[:]:
                if f.done():
                    futures.remove(f)
        except SimpleQueue.Empty:
            time.sleep(10)
            print("heart beat")
            conn.heartbeat_check()
            continue
        except Exception as e:
            print('ERROR', task, "raised {}\n {}".format(e, traceback.format_exc()))
            conn.release()
            executor.shutdown()
            raise  # this will restart the container in kubernetes

    executor.shutdown()


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


def wait_for_task(futures, ret_queue, err_queue, conn):
    idle_count = 0
    while True:
        for f in futures:
            if f.done():
                msg = f.result()
                print(msg)
                if msg == "done":
                    print("task done")
                    return True
                elif isinstance(msg, dict):
                    if msg.get('msg', None) == "done":
                        if msg.get('ret', None):
                            try:
                                ret_queue.put(json.dumps(msg['ret']))
                            except:
                                err_queue.put("Cannot jsonify the result from the worker")
                        print("task done")
                        return True
                    elif msg.get('msg', None) == "error":
                        if msg.get('ret', None):
                            err_queue.put(json.dumps(msg['ret']))
                        print("task error")
                        time.sleep(30)
                        return False
                    else:
                        print("message unknown: {}".format(msg))
                        return False
                else:
                    print("message unknown: {}".format(msg))
                    return False

        redis_conn.set(hostname, datetime.now().timestamp())
        cpu_usage = psutil.Process().cpu_percent(interval=1)
        if cpu_usage < 5:
            print("task stalled: {}".format(cpu_usage))
            idle_count += 1
        else:
            print("task busy: {}".format(cpu_usage))
            idle_count = 0

        if idle_count > 6:
            return False

        try:
            conn.drain_events(timeout=10)
        except socket.timeout:
            conn.heartbeat_check()


if __name__ == '__main__':
    redis_conn = redis.Redis(os.environ["REDIS_SERVER"])
    hostname = get_hostname()
    command()
