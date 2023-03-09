from common import GlobalComputeUrl, ZonalComputeUrl, GenerateBootDisk, GenerateNetworkInterface, GenerateAirflowVar
from common import INSTALL_DOCKER_CMD, INSTALL_NVIDIA_DOCKER_CMD, INSTALL_GPU_MONITORING, DOCKER_CMD, CELERY_CMD, PARALLEL_CMD


GPU_TYPES = ['gpu', 'custom-gpu', 'synaptor-gpu']
SYNAPTOR_TYPES = ['synaptor-cpu', 'synaptor-gpu', 'synaptor-seggraph']

def checkConsecutiveWorkers(concurrencies):
    l = [d['layer'] for d in sorted(concurrencies, key=lambda x: x['layer'])]
    if l == list(range(l[0], l[-1]+1)):
        return True
    else:
        missing = set(range(l[0], l[-1]+1)) - set(l)
        raise ValueError(f"missing worker for layer {','.join(str(x) for x in missing)}")


def GenerateEnvironVar(context, env_variables):
    export_variables = "\n".join([f'''export {e}={env_variables[e]}''' for e in env_variables])

    save_variables = "\n".join([f'''echo {e}=${e} >> /etc/environment''' for e in env_variables])

    return "\n".join([export_variables, save_variables])


def GenerateWorkerStartupScript(context, env_variables, cmd, use_gpu=False, use_hugepages=False):
    startup_script = f'''
#!/bin/bash
set -e
mount -t tmpfs -o size=80%,noatime tmpfs /tmp
mkdir -p /var/log/airflow/logs
chmod 777 /var/log/airflow/logs
DEBIAN_FRONTEND=noninteractive apt-get -y -o Dpkg::Options::="--force-confdef" -o Dpkg::Options::="--force-confold" dist-upgrade
{INSTALL_DOCKER_CMD}
'''

    if use_hugepages:
        startup_script += "echo $(free -g|grep Mem|awk '{print int($2/2)}') > /sys/kernel/mm/hugepages/hugepages-1048576kB/nr_hugepages"

    if use_gpu:
        startup_script += INSTALL_NVIDIA_DOCKER_CMD
        startup_script += INSTALL_GPU_MONITORING

    startup_script += f'''
{GenerateEnvironVar(context, env_variables)}
{cmd}
'''

    return startup_script


def GenerateDockerCommand(image, args):
    return DOCKER_CMD % {
        'args': ' '.join(args),
        'image': image,
    }


def GenerateCeleryWorkerCommand(image, docker_env, queue, concurrency):
    return GenerateDockerCommand(image, docker_env) + ' ' + CELERY_CMD % {
        'queue': queue,
        'concurrency': concurrency
    }


def GenerateWorkers(context, hostname_manager, worker):
    env_variables = GenerateAirflowVar(context, hostname_manager)

    docker_env = [f'-e {k}' for k in env_variables]

    docker_image = worker.get('workerImage', context.properties['seuronImage'])

    oom_canary_cmd = GenerateDockerCommand(docker_image, docker_env) + ' ' + "python utils/memory_monitor.py ${AIRFLOW__CELERY__BROKER_URL} worker-message-queue >& /dev/null"

    if worker['type'] == 'gpu':
        cmd = GenerateCeleryWorkerCommand(docker_image, docker_env+['-p 8793:8793'], queue=worker['type'], concurrency=worker['concurrency'])
    elif worker['type'] == 'atomic':
        cmd = GenerateCeleryWorkerCommand(docker_image, docker_env+['-p 8793:8793'], queue=worker['type'], concurrency=1)
    elif worker['type'] == 'composite':
        atomic_cmd = GenerateCeleryWorkerCommand(docker_image, docker_env+['-p 8793:8793'], queue='atomic', concurrency=1)
        concurrencies = worker['workerConcurrencies']
        if checkConsecutiveWorkers(concurrencies):
            cmd = " & \n".join([atomic_cmd] + [GenerateCeleryWorkerCommand(docker_image, docker_env, queue=worker['type']+'_'+str(c['layer']), concurrency=c['concurrency']) for c in concurrencies])
    elif worker['type'] == 'igneous':
        cmd = GenerateDockerCommand(docker_image, docker_env) + ' ' + f"python custom/task_execution.py --queue igneous --concurrency {worker['concurrency']} >& /dev/null"
    elif worker['type'] == 'custom-cpu':
        cmd = GenerateDockerCommand(docker_image, docker_env) + ' ' + f"custom/worker_cpu.sh {worker['concurrency']} >& /dev/null"
    elif worker['type'] == 'custom-gpu':
        cmd = GenerateDockerCommand(docker_image, docker_env+['-e CONDA_INSTALL_PYTORCH="true"']) + ' ' + f"custom/worker_gpu.sh {worker['concurrency']} >& /dev/null"
    elif worker['type'] in SYNAPTOR_TYPES:
        cmd = GenerateCeleryWorkerCommand(docker_image, docker_env+['-p 8793:8793'], queue=worker['type'], concurrency=1)
    else:
        raise ValueError(f"unknown worker type: {worker['type']}")

    use_gpu = worker['type'] in GPU_TYPES and worker['gpuWorkerAcceleratorType']
    use_hugepages = worker.get('reserveHugePages', False)

    startup_script = GenerateWorkerStartupScript(context, env_variables, oom_canary_cmd + "& \n" + cmd, use_gpu=use_gpu, use_hugepages=use_hugepages)

    provisioning_model = 'SPOT' if worker.get('preemptible', False) else 'STANDARD'

    instance_template = {
        'zone': worker['zone'],
        'machineType': worker['machineType'],
        'disks': [GenerateBootDisk(diskSizeGb=worker["diskSizeGb"])
                  if "diskSizeGb" in worker
                  else GenerateBootDisk(diskSizeGb=20)],
        'tags': {
            'items': ['princeton-access'],
        },
        'labels': {
            'vmrole': worker['type'],
            'location': worker['zone'],
            'deployment': context.env['deployment'],
        },
        'scheduling': {
            'provisioningModel': provisioning_model,
        },
        'metadata': {
            'items': [{
                'key': 'startup-script',
                'value': startup_script,
            }],
        },
        'networkInterfaces': [ GenerateNetworkInterface(context, worker['subnetwork']) ],
        'serviceAccounts': [{
            'scopes': [
                'https://www.googleapis.com/auth/logging.write',
                'https://www.googleapis.com/auth/monitoring.write',
                'https://www.googleapis.com/auth/devstorage.read_write',
            ]
        }],
    }

    if worker['type'] in GPU_TYPES:
        instance_template['guestAccelerators'] = [{
                'acceleratorCount': 1,
                'acceleratorType': worker['gpuWorkerAcceleratorType'],
        }]

    if worker['type'] == "atomic":
        instance_template['advancedMachineFeatures'] = {
            'threadsPerCore': 1
        }

    template_name = f"{context.env['deployment']}-template-{worker['type']}-worker-{worker['zone']}"
    template_resource = {
        'name': template_name,
        'type': 'compute.v1.instanceTemplates',
        'properties': {
            'project': context.env['project'],
            'properties': instance_template,
        },
    }

    ig_resource = {
        'name': f"{context.env['deployment']}-{worker['type']}-workers-{worker['zone']}",
        'type': 'compute.v1.instanceGroupManagers',
        'properties': {
            'instanceTemplate': f'$(ref.{template_name}.selfLink)',
            'targetSize': 0,
            'zone': worker['zone'],
        }
    }

    return [template_resource, ig_resource]
