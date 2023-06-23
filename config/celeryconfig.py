from airflow import configuration

# Broker settings.
CELERY_CONFIG = {
    'worker_prefetch_multiplier': 1,
    'task_acks_late': True,
    'broker_url': configuration.get('celery', 'broker_url'),
    'result_backend': configuration.get('celery', 'celery_result_backend'),
    'worker_concurrency':
        configuration.getint('celery', 'celeryd_concurrency'),
    'task_default_queue': configuration.get('operators', 'default_queue'),
    'task_default_exchange': configuration.get('operators', 'default_queue'),
    'worker_send_task_events': False
}
