from airflow.models import Variable
value = Variable.get("custom_script")
with open("custom/custom_worker.py", 'w', newline='\n') as script_file:
    script_file.write(value)

