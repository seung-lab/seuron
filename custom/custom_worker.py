from taskqueue import totask

import igneous.tasks

task_timeout = 300

def process_task(msg):
    task = totask(msg)
    task.execute()
    return None
