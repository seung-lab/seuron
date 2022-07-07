#!/bin/sh
python custom/download_script.py igneous_script
python custom/task_execution.py --queue "igneous"
