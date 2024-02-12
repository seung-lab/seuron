from common import GlobalComputeUrl, ZonalComputeUrl, GenerateBootDisk, GenerateNetworkInterface, GenerateAirflowVar

from common import INSTALL_DOCKER_CMD, INSTALL_NVIDIA_DOCKER_CMD, CELERY_CMD, PARALLEL_CMD

def GenerateEnvironVar(context, hostname_manager):
    if "postgres" in context.properties:
        postgres_user = context.properties['postgres'].get('user', "airflow")
        postgres_password = context.properties['postgres'].get('password', "airflow")
        postgres_db = context.properties['postgres'].get('database', "airflow")
    else:
        postgres_user = "airflow"
        postgres_password = "airflow"
        postgres_db = "airflow"

    if "nginx" in context.properties:
        basic_auth_username = context.properties['nginx'].get('user', "")
        basic_auth_password = context.properties['nginx'].get('password', "")
    else:
        basic_auth_username = ""
        basic_auth_password = ""

    if "grafana" in context.properties:
        grafana_username = context.properties['grafana'].get('user', "airflow")
        grafana_password = context.properties['grafana'].get('password', "airflow")
    else:
        grafana_username = "airflow"
        grafana_password = "airflow"

    env_variables = {
        'VENDOR': 'Google',
        'SLACK_TOKEN': context.properties['slack']['botToken'],
        'SLACK_NOTIFICATION_CHANNEL': context.properties['slack']['notificationChannel'],
        'DEPLOYMENT': context.env['deployment'],
        'ZONE': context.properties['zone'],
        'SEURON_TAG': context.properties['seuronImage'],
        '_AIRFLOW_WWW_USER_USERNAME': context.properties['airflow'].get('user', "airflow"),
        '_AIRFLOW_WWW_USER_PASSWORD': context.properties['airflow'].get('password', "airflow"),
        'POSTGRES_USER': postgres_user,
        'POSTGRES_PASSWORD': postgres_password,
        'POSTGRES_DB': postgres_db,
        'POSTGRES_MEM_MB': """$(free -m|grep Mem|awk '{print int($2/4)}')""",
        'POSTGRES_MAX_CONN': """$(free -m|grep Mem|awk '{print int($2/32)}')""",
        'GRAFANA_USERNAME': grafana_username,
        'GRAFANA_PASSWORD': grafana_password,
    }

    if basic_auth_username and basic_auth_password:
        env_variables['BASIC_AUTH_USERNAME'] = basic_auth_username
        env_variables['BASIC_AUTH_PASSWORD'] = basic_auth_password

    if context.properties.get('enableJupyterInterface', False):
        env_variables['ENABLE_JUPYTER_INTERFACE'] = "True"

    env_variables.update(GenerateAirflowVar(context, hostname_manager))

    export_variables = "\n".join([f'''export {e}="{env_variables[e]}"''' for e in env_variables])

    save_variables = "\n".join([f'''echo -e {e}=\\"${e}\\" >> /etc/environment''' for e in env_variables])

    return "\n".join([export_variables, save_variables])


def GenerateManagerStartupScript(context, hostname_manager, hostname_nfs_server):
    startup_script = f'''
#!/bin/bash
set -e

{GenerateEnvironVar(context, hostname_manager)}

if [ ! -f "/etc/bootstrap_done" ]; then

{INSTALL_DOCKER_CMD}

mkdir -p /share
apt-get install nfs-common -y

systemctl enable cron.service
systemctl start cron.service
echo "0 0 * * * docker system prune -f"|crontab -

docker swarm init

wget -O compose.yml {context.properties["composeLocation"]}

echo "[rabbitmq_management,rabbitmq_prometheus]." > /enabled_plugins

touch /etc/bootstrap_done

fi'''

    if hostname_nfs_server:
        startup_script+=f'''
until mount {hostname_nfs_server}:/share /share; do sleep 60; done'''

    startup_script +=f'''
docker stack deploy --with-registry-auth -c compose.yml {context.env["deployment"]}

iptables -I INPUT -p tcp --dport 6379 -j DROP
iptables -I INPUT -p tcp --dport 6379 -s 172.16.0.0/12 -j ACCEPT
iptables -I DOCKER-USER -p tcp --dport 6379 -j DROP
iptables -I DOCKER-USER -p tcp --dport 6379 -s 172.16.0.0/12 -j ACCEPT

while true
do
    if [ $(curl -s "http://metadata/computeMetadata/v1/instance/attributes/redeploy" -H "Metadata-Flavor: Google") == "true" ]; then
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


def GenerateManager(context, hostname_manager, hostname_nfs_server, worker_metadata):
    """Generate configuration."""

    startup_script = GenerateManagerStartupScript(context, hostname_manager, hostname_nfs_server)

    diskType = ZonalComputeUrl(
        context.env['project'],
        context.properties['zone'],
        'diskTypes', 'pd-ssd')

    instance_resource= {
        'zone': context.properties['zone'],
        'machineType': ZonalComputeUrl(
                      context.env['project'], context.properties['zone'], 'machineTypes', context.properties['managerMachineType']
        ),
        'disks': [GenerateBootDisk(diskSizeGb=50, diskType=diskType)],
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
        'name': hostname_manager.split('.')[0],
        'type': 'compute.v1.instance',
        'properties': instance_resource,
    }

    return [manager_resource]
