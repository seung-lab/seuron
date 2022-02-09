COMPUTE_URL_BASE = 'https://www.googleapis.com/compute/v1/'

INSTALL_DOCKER_CMD = '''
echo ##### Set up Docker #############################################################
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add -
apt-key fingerprint 0EBFCD88
add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
apt-get update -y
apt-get install docker-ce -y
usermod -aG docker ubuntu
mkdir -p /etc/docker
systemctl restart docker
gcloud auth --quiet configure-docker
'''

INSTALL_NVIDIA_DOCKER_CMD = '''
echo ##### Set up NVidia #############################################################
# Add the package repositories
distribution=$(. /etc/os-release;echo $ID$VERSION_ID)
curl -s -L https://nvidia.github.io/nvidia-docker/gpgkey | apt-key add -
curl -s -L https://nvidia.github.io/nvidia-docker/$distribution/nvidia-docker.list | tee /etc/apt/sources.list.d/nvidia-docker.list
add-apt-repository -y ppa:graphics-drivers/ppa
apt-get update -y
DEBIAN_FRONTEND=noninteractive apt-get -y -o Dpkg::Options::="--force-confdef" -o Dpkg::Options::="--force-confold" install nvidia-headless-470 nvidia-container-toolkit nvidia-container-runtime
cat << EOF > /etc/docker/daemon.json
{
  "default-runtime": "nvidia",
  "runtimes": {
    "nvidia": {
      "path": "nvidia-container-runtime",
      "runtimeArgs": []
    }
  }
}
EOF
systemctl restart docker
'''

DOCKER_CMD = 'docker run --restart unless-stopped -v /var/run/docker.sock:/var/run/docker.sock -v /tmp:/tmp -v ${AIRFLOW__LOGGING__BASE_LOG_FOLDER}:/var/log/airflow/logs %(args)s %(image)s'

CELERY_CMD = 'airflow celery worker --without-gossip --without-mingle -c %(concurrency)s -q %(queue)s'

PARALLEL_CMD = 'parallel --retries 100 -j%(jobs)d -N0 %(cmd)s ::: {0..%(jobs)d} &'


def GlobalComputeUrl(project, collection, name):
    return ''.join([COMPUTE_URL_BASE, 'projects/', project,
                  '/global/', collection, '/', name])


def ZonalComputeUrl(project, zone, collection, name):
  return ''.join([COMPUTE_URL_BASE, 'projects/', project,
                  '/zones/', zone, '/', collection, '/', name])


def GenerateAirflowVar(context, hostname_manager):
    postgres_user = context.properties['postgresUser']
    postgres_password = context.properties['postgresPassword']
    postgres_db = context.properties['postgresDB']
    sqlalchemy_conn = f'''postgresql+psycopg2://{postgres_user}:{postgres_password}@{hostname_manager}/{postgres_db}'''
    airflow_variable = {
        'AIRFLOW__CORE__HOSTNAME_CALLABLE': 'google_metadata.gce_internal_ip',
        'AIRFLOW__CORE__SQL_ALCHEMY_CONN': sqlalchemy_conn,
        'AIRFLOW__CORE__FERNET_KEY': context.properties['fernetKey'],
        'AIRFLOW__CELERY__BROKER_URL': f'amqp://{hostname_manager}',
        'AIRFLOW__CELERY__CELERY_RESULT_BACKEND': f'db+{sqlalchemy_conn}',
        'AIRFLOW__WEBSERVER__SECRET_KEY': context.properties['secretKey'],
        'AIRFLOW__LOGGING__BASE_LOG_FOLDER': '/usr/local/airflow/logs',
        'AIRFLOW__LOGGING__REMOTE_BASE_LOG_FOLDER': f'{context.properties["remoteLogFolder"]}/{context.env["deployment"]}',
        'AIRFLOW__METRICS__STATSD_ON': 'False',
        'AIRFLOW__METRICS__STATSD_HOST': hostname_manager,
    }

    return airflow_variable


def GenerateBootDisk(diskSizeGb):
    ubuntu_release = 'family/ubuntu-2004-lts'
    return {
            'type': 'PERSISTENT',
            'autoDelete': True,
            'boot': True,
            'initializeParams': {
                'sourceImage': GlobalComputeUrl(
                    'ubuntu-os-cloud', 'images', ubuntu_release
                    ),
                'diskSizeGb': diskSizeGb,
            },
        }


def GenerateNetworkInterface(context, subnetwork, ipAddr=None):
    network_interface = {
        'network': f'$(ref.{context.env["deployment"]}-network.selfLink)',
        'subnetwork': f'$(ref.{context.env["deployment"]}-{subnetwork}-subnetwork.selfLink)',
        'accessConfigs': [{
            'name': 'External NAT',
            'type': 'ONE_TO_ONE_NAT',
        }],
    }
    if ipAddr:
        network_interface['networkIP'] = ipAddr

    return network_interface
