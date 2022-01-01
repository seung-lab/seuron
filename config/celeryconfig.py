from airflow import configuration

# Broker settings.
CELERY_CONFIG = {
    'accept_content': ['json', 'pickle'],
    'event_serializer': 'json',
    'result_serializer': 'pickle',
    'task_serializer': 'pickle',
    #'task_time_limit': 7200,
    'worker_prefetch_multiplier': 1,
    'task_acks_late': True,
    'task_reject_on_worker_lost': True,
    'broker_url': configuration.get('celery', 'broker_url'),
    'result_backend': configuration.get('celery', 'CELERY_RESULT_BACKEND'),
    'worker_concurrency':
        configuration.getint('celery', 'CELERYD_CONCURRENCY'),
    'task_default_queue': configuration.get('operators', 'DEFAULT_QUEUE'),
    'task_default_exchange': configuration.get('operators', 'DEFAULT_QUEUE'),
    'worker_send_task_events': False
}
