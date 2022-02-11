#!/bin/sh
conda install -y pytorch cudatoolkit=11.3 -c pytorch
python custom/download_script.py
python custom/task_execution.py --queue "custom-gpu"
