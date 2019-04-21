#!/bin/bash
DOCKER_SOCKET=/var/run/docker.sock

# In order run DooD (Docker outside of Docker) we need to make sure the
# container's docker group id matches the host's group id. If it doens't match,
# update the group id and then restart the script. (also remove sudoer privs)
if [ ! -S ${DOCKER_SOCKET} ]; then
    echo 'Docker socket not found!'
else
    DOCKER_GID=$(stat -c '%g' $DOCKER_SOCKET)
    DOCKER_GROUP=docker
    USER_GID=$(id -G ${AIRFLOW_USER})
    echo "User ${AIRFLOW_USER} belongs to the groups $USER_GID"
    if $(echo $USER_GID | grep -qw $DOCKER_GID); then
        echo "Host Docker Group ID $DOCKER_GID found on user"
    else
        echo "Host Docker Group ID $DOCKER_GID not found on user"
        echo "Updating docker group to host docker group"
        sudo groupmod -o -g ${DOCKER_GID} docker
        # add this for boot2docker
        sudo usermod -aG 100,50 $AIRFLOW_USER
    fi
fi

