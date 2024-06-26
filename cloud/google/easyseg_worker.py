from common import ZonalComputeUrl, GenerateBootDisk, GenerateNetworkInterface, GenerateAirflowVar

from common import INSTALL_DOCKER_CMD, INSTALL_NVIDIA_DOCKER_CMD, INSTALL_GPU_MONITORING

from workers import GenerateCeleryWorkerCommand, GenerateEnvironVar, GenerateDockerCommand


def GenerateEasysegWorker(context, hostname_manager, hostname_easyseg_worker):
    easyseg_param = context.properties["easysegWorker"]

    env_variables = GenerateAirflowVar(context, hostname_manager)

    if "gpuWorkerAcceleratorType" in easyseg_param:
        env_variables["HAVE_GPUS"] = "True"
    else:
        env_variables["HAVE_GPUS"] = "False"


    docker_env = [f'-e {k}' for k in env_variables]
    docker_image = context.properties['seuronImage']


    cmd = f"""
#!/bin/bash
set -e
mount -t tmpfs -o size=80%,noatime tmpfs /tmp
if [ ! -f "/etc/bootstrap_done" ]; then
mkdir -p /var/log/airflow/logs
chmod 777 /var/log/airflow/logs
mkdir -p /share
chmod 777 /share
DEBIAN_FRONTEND=noninteractive apt-get -y -o Dpkg::Options::="--force-confdef" -o Dpkg::Options::="--force-confold" dist-upgrade
{INSTALL_DOCKER_CMD}
{INSTALL_NVIDIA_DOCKER_CMD if "gpuWorkerAcceleratorType" in easyseg_param else ""}
{INSTALL_GPU_MONITORING if "gpuWorkerAcceleratorType" in easyseg_param else ""}
touch /etc/bootstrap_done
sleep 60
shutdown -h now
fi
{GenerateEnvironVar(context, env_variables)}
    """
    oom_canary_cmd = GenerateDockerCommand(docker_image, docker_env) + ' ' + "python utils/memory_monitor.py ${AIRFLOW__CELERY__BROKER_URL} bot-message-queue >& /dev/null"
    chunkflow_cmd = GenerateCeleryWorkerCommand(docker_image, docker_env+['-p 8793:8793'], queue="gpu", concurrency=2)
    abiss_cmd = GenerateCeleryWorkerCommand(docker_image, docker_env, queue="atomic", concurrency=1)
    igneous_cmd = GenerateDockerCommand(docker_image, docker_env + ["--restart on-failure",]) + ' ' + "python custom/task_execution.py --queue igneous --concurrency 0 >& /dev/null"
    synaptor_cpu_cmd = GenerateCeleryWorkerCommand(docker_image, docker_env, queue="synaptor-cpu", concurrency=1)
    synaptor_gpu_cmd = GenerateCeleryWorkerCommand(docker_image, docker_env, queue="synaptor-gpu", concurrency=1)
    cmd += " & \n".join([oom_canary_cmd, chunkflow_cmd, abiss_cmd, igneous_cmd, synaptor_cpu_cmd, synaptor_gpu_cmd])

    diskType = ZonalComputeUrl(
        context.env['project'],
        easyseg_param['zone'],
        'diskTypes', 'pd-ssd')

    instance_resource = {
        'zone': easyseg_param['zone'],
        'machineType': ZonalComputeUrl(
                      context.env['project'], easyseg_param['zone'],
                      'machineTypes', easyseg_param['machineType']
        ),
        'disks': [
                  GenerateBootDisk(diskSizeGb=50, diskType=diskType),
                  ],
        "scheduling": {
            "automaticRestart": True,
            "onHostMaintenance": "TERMINATE",
            "provisioningModel": "STANDARD"
        },
        'labels': {
            'vmrole': 'easyseg-worker',
            'location': easyseg_param['zone'],
            'deployment': context.env['deployment'],
        },
        'tags': {
            'items': ['princeton-access'],
        },
        'metadata': {
            'items': [
                {
                    'key': 'startup-script',
                    'value': cmd,
                }
            ],
        },
        'networkInterfaces': [GenerateNetworkInterface(context, easyseg_param['subnetwork'])],
        'serviceAccounts': [{
            'scopes': [
                'https://www.googleapis.com/auth/logging.write',
                'https://www.googleapis.com/auth/monitoring.write',
                'https://www.googleapis.com/auth/devstorage.read_write',
            ],
        }],
    }

    if "gpuWorkerAcceleratorType" in easyseg_param:
        instance_resource["guestAccelerators"] = [
            {
                'acceleratorCount': 1,
                'acceleratorType': ZonalComputeUrl(
                      context.env['project'], easyseg_param['zone'],
                      'acceleratorTypes', easyseg_param['gpuWorkerAcceleratorType'],
                ),
            }
        ]


    easyseg_worker_resource = {
        'name': hostname_easyseg_worker.split('.')[0],
        'type': 'compute.v1.instance',
        'properties': instance_resource,
    }

    return [easyseg_worker_resource,]
