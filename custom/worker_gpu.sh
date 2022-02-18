#!/bin/sh
python custom/download_script.py
python custom/task_execution.py --queue "custom-gpu"
