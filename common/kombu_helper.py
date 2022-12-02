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

def get_message(broker_url, queue, ack=True):
    with Connection(broker_url) as conn:
        simple_queue = conn.SimpleQueue(queue)
        if simple_queue.qsize() == 0:
            return None
        msg = simple_queue.get()
        if ack:
            msg.ack()
        return msg.payload

def put_message(broker_url, queue, msg):
    with Connection(broker_url) as conn:
        simple_queue = conn.SimpleQueue(queue)
        return simple_queue.put(msg)

# Can't use the variable name "conn" as an argument bc it's reserved by airflow
def drain_messages(broker_url, queue):
    with Connection(broker_url) as conn:
        simple_queue = conn.SimpleQueue(queue)
        while True:
            try:
                message = simple_queue.get_nowait()
            except SimpleQueue.Empty:
                status = check_queue(queue)
                if "messages" not in status:
                    sleep(30)
                    continue
                elif status["messages"] > 0:
                    print(f'still {status["messages"]} messages left in {queue}, sleep for 30s')
                    sleep(30)
                    continue
                else:
                    break
            #print(f'message: {message.payload}')
            message.ack()
        simple_queue.close()
