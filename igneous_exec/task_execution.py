import random
import signal
import time
import traceback
import threading
import queue

import click
from taskqueue import totask
from igneous import EmptyVolumeException
from kombu import Connection
from kombu.simple import SimpleQueue
import psutil

LOOP = True

def int_handler(signum, frame):
    global LOOP
    print("Interrupted. Exiting.")
    LOOP = False

def timeout_handler(signum, frame):
    print("Task timeout")
    raise Exception("end of time")

signal.signal(signal.SIGALRM, timeout_handler)

signal.signal(signal.SIGINT, int_handler)

@click.command()
@click.option('--tag', default='',  help='kind of task to execute')
@click.option('--queue', default="",  help='Name of pull queue to use.')
@click.option('--qurl', default="",  help='SQS Queue URL if using SQS')
@click.option('--timeout', default=60,  help='SQS Queue URL if using SQS')
@click.option('--loop/--no-loop', default=LOOP, help='run execution in infinite loop or not', is_flag=True)
def command(tag, queue, qurl, timeout, loop):
    conn = Connection(qurl, heartbeat=180)
    qos_monitor = threading.Thread(target=handle_idle, args=(q_state, timeout, conn, ))
    qos_monitor.start()
    execute(conn, tag, queue, qurl, timeout, loop)
    qos_monitor.join()
    conn.close()
    return

def random_exponential_window_backoff(n):
    n = min(n, 30)
    # 120 sec max b/c on avg a request every ~250msec if 500 containers
    # in contention which seems like a quite reasonable volume of traffic
    # to handle
    high = min(2 ** n, 120)
    return random.uniform(0, high)


def execute(conn, tag, queue, qurl, timeout, loop):
    print("Pulling from {}".format(qurl))
    queue = conn.SimpleQueue(queue)

    tries = 0
    while True:
        task = 'unknown'
        try:
            message = queue.get_nowait()
            task = totask({'payload': message.payload, 'id': -1})
            tries += 1
            print(task)
            q_state.put("busy")
            task.execute()
            print("delete task in queue...")
            message.ack()
            q_state.put("idle")
            print('INFO', task , "succesfully executed")
            tries = 0
        except EmptyVolumeException:
            print('WARNING', task, "raised an EmptyVolumeException")
            message.ack(task)
            tries = 0
        except SimpleQueue.Empty:
            time.sleep(random_exponential_window_backoff(tries))
            conn.heartbeat_check()
            continue
        except KeyboardInterrupt:
            print('bye bye')
            break
        except Exception as e:
            print('ERROR', task, "raised {}\n {}".format(e , traceback.format_exc()))
            q_state.put("died")
            conn.release()
            raise #this will restart the container in kubernetes
        if (not loop) or (not LOOP):
            q_state.put("died")
            print("not in loop mode, will break the loop and exit")
            break

def handle_idle(q_state, timeout, conn):
    busy = False
    idle_count = 0
    while True:
        #logger.debug("check queue")
        if not busy and q_state.qsize() == 0:
            time.sleep(1)
            continue
        if q_state.qsize() > 0:
            msg = q_state.get()
            while q_state.qsize() > 0:
                msg = q_state.get()
            if msg == "died":
                print("parent died")
                return
            elif msg == "busy":
                print("task started")
                busy = True
            elif msg == "idle":
                print("task finished")
                idle_count = 0
                busy = False
                signal.alarm(0)
        if busy:
            cpu_usage = psutil.Process().cpu_percent(interval=1)
            if cpu_usage < 5:
                print("task stalled: {}".format(cpu_usage))
                idle_count += 1
            else:
                print("task busy: {}".format(cpu_usage))
                idle_count = 0

            if idle_count > 6:
                signal.alarm(1)

            time.sleep(10)
            conn.heartbeat_check()


if __name__ == '__main__':
    q_state = queue.Queue()
    command()
