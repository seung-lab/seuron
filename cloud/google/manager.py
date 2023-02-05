from common import GlobalComputeUrl, ZonalComputeUrl, GenerateBootDisk, GenerateNetworkInterface, GenerateAirflowVar

from common import INSTALL_DOCKER_CMD, INSTALL_NVIDIA_DOCKER_CMD, CELERY_CMD, PARALLEL_CMD

def GenerateEnvironVar(context, hostname_manager):
    env_variables = {
        'VENDOR': 'Google',
        'SLACK_TOKEN': context.properties['slack']['botToken'],
        'SLACK_NOTIFICATION_CHANNEL': context.properties['slack']['notificationChannel'],
        'DEPLOYMENT': context.env['deployment'],
        'ZONE': context.properties['zone'],
        'SEURON_TAG': context.properties['seuronImage'],
        '_AIRFLOW_WWW_USER_USERNAME': context.properties['airflow']['user'],
        '_AIRFLOW_WWW_USER_PASSWORD': context.properties['airflow']['password'],
        'POSTGRES_USER': context.properties['postgres']['user'],
        'POSTGRES_PASSWORD': context.properties['postgres']['password'],
        'POSTGRES_DB': context.properties['postgres']['database'],
        'POSTGRES_MEM': """$(free -m|grep Mem|awk '{print int($2/4)}')""",
        'POSTGRES_MAX_CONN': """$(free -m|grep Mem|awk '{print int($2/32)}')""",
        'GRAFANA_USERNAME': context.properties['grafana']['user'],
        'GRAFANA_PASSWORD': context.properties['grafana']['password'],
        'BASIC_AUTH_USERNAME': context.properties['nginx'].get('user', ''),
        'BASIC_AUTH_PASSWORD': context.properties['nginx'].get('password', ''),
    }

    env_variables.update(GenerateAirflowVar(context, hostname_manager))

    export_variables = "\n".join([f'''export {e}={env_variables[e]}''' for e in env_variables])

    save_variables = "\n".join([f'''echo {e}=${e} >> /etc/environment''' for e in env_variables])

    return "\n".join([export_variables, save_variables])


def GenerateManagerStartupScript(context, hostname_manager):
    startup_script = f'''
#!/bin/bash
set -e
mkdir -p /var/lib/postgresql/data /var/lib/rabbitmq

{GenerateEnvironVar(context, hostname_manager)}

if [ ! -f "/etc/bootstrap_done" ]; then

{INSTALL_DOCKER_CMD}

systemctl enable cron.service
systemctl start cron.service
echo "0 0 * * * docker system prune -f"|crontab -

docker swarm init

wget -O compose.yml {context.properties["composeLocation"]}

echo "[rabbitmq_management,rabbitmq_prometheus]." > /enabled_plugins

touch /etc/bootstrap_done

fi

docker stack deploy --with-registry-auth -c compose.yml {context.env["deployment"]}

while true
do
    if [ $(curl "http://metadata/computeMetadata/v1/instance/attributes/redeploy" -H "Metadata-Flavor: Google") == "true"  ]; then
        docker stack rm {context.env["deployment"]}
        sleep 120
        docker stack deploy --with-registry-auth -c compose.yml {context.env["deployment"]}
        sleep 300
    else
        sleep 60
    fi
done
'''
    return startup_script


def GenerateManager(context, hostname_manager, worker_metadata):
    """Generate configuration."""

    startup_script = GenerateManagerStartupScript(context, hostname_manager)

    instance_resource= {
        'zone': context.properties['zone'],
        'machineType': ZonalComputeUrl(
                      context.env['project'], context.properties['zone'], 'machineTypes', context.properties['managerMachineType']
        ),
        'disks': [GenerateBootDisk(diskSizeGb=100)],
        'labels': {
            'vmrole': 'manager',
            'location': context.properties['zone'],
            'deployment': context.env['deployment'],
        },
        'tags': {
            'items': ['princeton-access',
                      'http-server',
                      'https-server'],
        },
        'metadata': {
            'items': [
                {
                    'key': 'startup-script',
                    'value': startup_script,
                },
                {
                    'key': 'redeploy',
                    'value': False,
                },
            ] + worker_metadata,
        },
        'networkInterfaces': [ GenerateNetworkInterface(context, context.properties['subnetwork']) ],
        'serviceAccounts': [{
            'scopes': [
                'https://www.googleapis.com/auth/cloud-platform',
                'https://www.googleapis.com/auth/compute',
                'https://www.googleapis.com/auth/servicecontrol',
                'https://www.googleapis.com/auth/service.management.readonly',
                'https://www.googleapis.com/auth/logging.write',
                'https://www.googleapis.com/auth/monitoring.write',
                'https://www.googleapis.com/auth/trace.append',
                'https://www.googleapis.com/auth/devstorage.read_only',
                'https://www.googleapis.com/auth/cloud.useraccounts.readonly',
            ],
        }],
    }

    manager_resource = {
        'name': hostname_manager,
        'type': 'compute.v1.instance',
        'properties': instance_resource,
    }

    return [manager_resource]
