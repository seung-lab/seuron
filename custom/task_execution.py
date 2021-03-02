from airflow.models import Variable
import os
import time
import traceback
import threading
import queue
import socket
import json

import click
from kombu import Connection
from kombu.simple import SimpleQueue
import psutil
import custom_worker

@click.command()
@click.option('--tag', default='',  help='kind of task to execute')
@click.option('--queue', default="",  help='Name of pull queue to use.')
@click.option('--qurl', default="",  help='SQS Queue URL if using SQS')
@click.option('--timeout', default=60,  help='SQS Queue URL if using SQS')
@click.option('--loop/--no-loop', default=True, help='run execution in infinite loop or not', is_flag=True)
def command(tag, queue, qurl, timeout, loop):
    param = Variable.get("param", deserialize_json=True)
    cv_secrets_path = os.path.join(os.path.expanduser('~'),".cloudvolume/secrets")
    if not os.path.exists(cv_secrets_path):
        os.makedirs(cv_secrets_path)

    mount_secrets = param.get("MOUNT_SECRETES", [])

    for k in mount_secrets:
        v = Variable.get(k)
        with open(os.path.join(cv_secrets_path, k), 'w') as value_file:
            value_file.write(v)

    conn = Connection(qurl, heartbeat=timeout)
    worker = threading.Thread(target=handle_task, args=(q_task, q_state,))
    worker.daemon = True
    worker.start()
    execute(conn, tag, queue, qurl, timeout, loop)
    conn.release()
    return

def execute(conn, tag, queue_name, qurl, timeout, loop):
    print("Pulling from {}".format(qurl))
    queue = conn.SimpleQueue(queue_name)
    ret_queue = conn.SimpleQueue(queue_name+"_ret")

    while True:
        task = 'unknown'
        try:
            message = queue.get_nowait()
            print("put message into queue: {}".format(message.payload))
            q_task.put(message.payload)
            if wait_for_task(q_state, ret_queue, conn):
                print("delete task in queue...")
                message.ack()
                print('INFO', task , "succesfully executed")
            else:
                break
        except SimpleQueue.Empty:
            time.sleep(10)
            print("heart beat")
            conn.heartbeat_check()
            continue
        except KeyboardInterrupt:
            print('bye bye')
            break
        except Exception as e:
            print('ERROR', task, "raised {}\n {}".format(e , traceback.format_exc()))
            conn.release()
            raise #this will restart the container in kubernetes
        if not loop:
            print("not in loop mode, will break the loop and exit")
            break


def handle_task(q_task, q_state):
    while True:
        if q_task.qsize() == 0:
            time.sleep(1)
            continue
        if q_task.qsize() > 0:
            msg = q_task.get()
            print("run task: {}".format(msg))
            ret = custom_worker.process_task(msg)
            if isinstance(ret, dict):
                if ret.get('ret', None):
                    ret['msg'] = "done"
                    q_state.put(ret)
                else:
                    q_state.put("done")
            else:
                q_state.put("done")


def wait_for_task(q_state, ret_queue, conn):
    idle_count = 0
    while True:
        if q_state.qsize() > 0:
            msg = q_state.get()
            while q_state.qsize() > 0:
                msg = q_state.get()
            print(msg)
            if msg == "done":
                print("task done")
                return True
            elif isinstance(msg, dict):
                if msg.get('msg', None) == "done":
                    if msg.get('ret', None):
                        ret_queue.put(json.dumps(msg['ret']))
                    print("task done")
                    return True
                print("message unknown: {}".format(msg))
                return False
            else:
                print("message unknown: {}".format(msg))
                return False
        else: #busy
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
    q_state = queue.Queue()
    q_task = queue.Queue()
    command()
