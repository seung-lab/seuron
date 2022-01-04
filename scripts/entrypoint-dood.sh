#!/bin/bash
set -eo pipefail

source scripts/add-user-docker.sh

# this doesn't protect from docker but it's a little more secure
sudo sed -i "/$AIRFLOW_USER/d" /etc/sudoers

if [[ -n "${_AIRFLOW_DB_UPGRADE=}"  ]] ; then
    if [ -z ${DOCKER_GROUP} ]; then
        exec airflow db upgrade
    else
        exec sg ${DOCKER_GROUP} airflow db upgrade
    fi
fi

echo "start script with group $DOCKER_GROUP"

export PYTHONPATH=$AIRFLOW_HOME/common:$PYTHONPATH

python scripts/install_packages.py

# DOCKER_GROUP from /add-user-docker.sh
if [ -z ${DOCKER_GROUP} ]; then
    exec "$@"
else
    exec sg ${DOCKER_GROUP} "$*"
fi
