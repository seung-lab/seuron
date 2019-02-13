#!/bin/bash
source scripts/add-user-docker.sh

# this doesn't protect from docker but it's a little more secure
sudo sed -i "/$AIRFLOW_USER/d" /etc/sudoers

echo "start script with group $DOCKER_GROUP"
# DOCKER_GROUP from /add-user-docker.sh
if [ -z ${DOCKER_GROUP} ]; then
    exec "$@"
else
    exec sg ${DOCKER_GROUP} "$*"
fi
