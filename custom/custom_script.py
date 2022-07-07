from taskqueue import totask
from igneous import EmptyVolumeException

import igneous.tasks

try:
    from igneous_script import *
except ImportError:
    print("import failed")

task_timeout = 600

def process_task(msg):
    task = totask(msg)
    task.execute()
    return None
