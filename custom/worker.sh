#!/bin/sh
python custom/download_script.py
python custom/task_execution.py --queue custom --qurl amqp://172.31.31.249:5672
