#!/bin/bash
set -eo pipefail

source scripts/add-user-docker.sh

# this doesn't protect from docker but it's a little more secure
sudo sed -i "/$AIRFLOW_USER/d" /etc/sudoers

export PYTHONPATH=$AIRFLOW_HOME/common:$PYTHONPATH

echo "start script with group $DOCKER_GROUP"

if [[ -z "${_AIRFLOW_DB_UPGRADE=}" ]] ; then
    python scripts/install_packages.py
fi

# DOCKER_GROUP from /add-user-docker.sh
if [ -z ${DOCKER_GROUP} ]; then
    exec "$@"
else
    exec sg ${DOCKER_GROUP} "$*"
fi
