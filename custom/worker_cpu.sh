#!/bin/sh
python custom/download_script.py custom_script
python custom/task_execution.py --queue "custom-cpu"
