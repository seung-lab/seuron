from kombu import Connection
from kombu.simple import SimpleQueue
from time import sleep
import requests

def check_queue(queue):
    ret = requests.get("http://rabbitmq:15672/api/queues/%2f/{}".format(queue), auth=('guest', 'guest'))
    if not ret.ok:
        raise RuntimeError("Cannot connect to rabbitmq management interface")
    queue_status = ret.json()
    return queue_status

def visible_messages(broker_url, queue):
    with Connection(broker_url) as conn:
        simple_queue = conn.SimpleQueue(queue)
        return simple_queue.qsize()

def peek_message(broker_url, queue):
    return get_message(broker_url, queue, ack=False)

def get_message(broker_url, queue, ack=True, timeout=None):
    with Connection(broker_url) as conn:
        try:
            simple_queue = conn.SimpleQueue(queue)
            msg = simple_queue.get(timeout=timeout)
            if ack:
                msg.ack()
            return msg.payload
        except:
            return None

def put_message(broker_url, queue, msg):
    with Connection(broker_url) as conn:
        simple_queue = conn.SimpleQueue(queue)
        return simple_queue.put(msg)

# Can't use the variable name "conn" as an argument bc it's reserved by airflow
def drain_messages(broker_url, queue):
    with Connection(broker_url) as conn:
        channel = conn.channel()
        ret = channel.queue_delete(queue)
        print(f"deleted {ret} messages")
        simple_queue = conn.SimpleQueue(queue)
        print(f"remaining {simple_queue.qsize()} messages")
