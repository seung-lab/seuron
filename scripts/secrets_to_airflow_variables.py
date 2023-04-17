import os
from airflow.models import Variable

directory = '/run/secrets'
print('Searching %s' % directory)
if os.path.isdir(directory):
    for key in os.listdir(directory):
        with open(os.path.join(directory, key), 'r') as f:
            value = f.read()
        Variable.set(key, value)
        print('Setting key %s as airflow variable: %s.secret' % (key, key))
print('Finished setting secrets to airflow variables')
