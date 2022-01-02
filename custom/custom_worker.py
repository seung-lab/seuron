from taskqueue import totask
from igneous import EmptyVolumeException

task_timeout = 600

def process_task(msg):
    task = totask(msg)
    task.execute()
    return None
