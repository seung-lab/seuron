#!/bin/bash
set -eo pipefail

source scripts/add-user-docker.sh

# this doesn't protect from docker but it's a little more secure
export PYTHONPATH=$AIRFLOW_HOME/common:$PYTHONPATH

echo "start script with group $DOCKER_GROUP"

if [[ -z "${_AIRFLOW_DB_UPGRADE=}" ]] ; then
    python scripts/install_packages.py
else
    if [ ! -f .sudo.disabled  ]; then
        sudo chown -R $AIRFLOW_USER $AIRFLOW__LOGGING__BASE_LOG_FOLDER
    fi
fi

if [[ -n "${CONDA_INSTALL_PYTORCH=}" ]] ; then
    conda uninstall -y nomkl
    conda install -y pytorch cudatoolkit=11.3 -c pytorch
fi

if [[ "${ENABLE_JUPYTER_INTERFACE=}" == "True" ]] ; then
    export PYTHONPATH=$AIRFLOW_HOME/ipython_extensions:$PYTHONPATH
    conda install -y jupyterlab jupyterlab-lsp jupyterlab-git python-lsp-server
    if [ ! -f .sudo.disabled  ]; then
        sudo chown -R $AIRFLOW_USER $AIRFLOW_HOME/jupyterlab
    fi
    if [ ! -f $AIRFLOW_HOME/jupyterlab/local_pipeline_tutorial.ipynb ]; then
        cp $AIRFLOW_HOME/examples/local_pipeline_tutorial.ipynb $AIRFLOW_HOME/jupyterlab/local_pipeline_tutorial.ipynb
    fi
    if [ ! -f $AIRFLOW_HOME/jupyterlab/minnie65_local_pipeline.ipynb ]; then
        cp $AIRFLOW_HOME/examples/minnie65_local_pipeline.ipynb $AIRFLOW_HOME/jupyterlab/minnie65_local_pipeline.ipynb
    fi
fi

if [ ! -f .sudo.disabled  ]; then
    sudo sed -i "/$AIRFLOW_USER/d" /etc/sudoers
    touch .sudo.disabled
fi

rm -f /usr/local/airflow/airflow-worker.pid

# DOCKER_GROUP from /add-user-docker.sh
if [ -z ${DOCKER_GROUP} ]; then
    exec "$@"
else
    exec sg ${DOCKER_GROUP} "$*"
fi
