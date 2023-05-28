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
DEBIAN_FRONTEND=noninteractive apt-get -y -o Dpkg::Options::="--force-confdef" -o Dpkg::Options::="--force-confold" install nvidia-headless-515 nvidia-utils-515 nvidia-container-toolkit
systemctl restart docker
'''

# https://cloud.google.com/compute/docs/gpus/monitor-gpus
INSTALL_GPU_MONITORING = '''
apt-get update -y
DEBIAN_FRONTEND=noninteractive apt-get -y -o Dpkg::Options::="--force-confdef" -o Dpkg::Options::="--force-confold" install python3-venv
mkdir -p /opt/google
cd /opt/google
git clone https://github.com/GoogleCloudPlatform/compute-gpu-monitoring.git
cd /opt/google/compute-gpu-monitoring/linux
python3 -m venv venv
venv/bin/pip install wheel
venv/bin/pip install -Ur requirements.txt
cp /opt/google/compute-gpu-monitoring/linux/systemd/google_gpu_monitoring_agent_venv.service /lib/systemd/system
systemctl daemon-reload
systemctl --no-reload --now enable /lib/systemd/system/google_gpu_monitoring_agent_venv.service
'''

DOCKER_CMD = 'docker run --restart on-failure -v /var/run/docker.sock:/var/run/docker.sock -v /tmp:/tmp -v /var/log/airflow/logs:${AIRFLOW__LOGGING__BASE_LOG_FOLDER} %(args)s %(image)s'

CELERY_CMD = 'airflow celery worker --without-gossip --without-mingle -c %(concurrency)s -q %(queue)s'

PARALLEL_CMD = 'parallel --retries 100 -j%(jobs)d -N0 %(cmd)s ::: {0..%(jobs)d} &'


def GlobalComputeUrl(project, collection, name):
    return ''.join([COMPUTE_URL_BASE, 'projects/', project,
                  '/global/', collection, '/', name])


def ZonalComputeUrl(project, zone, collection, name):
  return ''.join([COMPUTE_URL_BASE, 'projects/', project,
                  '/zones/', zone, '/', collection, '/', name])


def GenerateAirflowVar(context, hostname_manager):
    postgres_user = context.properties['postgres']['user']
    postgres_password = context.properties['postgres']['password']
    postgres_db = context.properties['postgres']['database']
    sqlalchemy_conn = f'''postgresql+psycopg2://{postgres_user}:{postgres_password}@{hostname_manager}/{postgres_db}'''
    airflow_variable = {
        'AIRFLOW__CORE__HOSTNAME_CALLABLE': 'google_metadata.gce_hostname',
        'AIRFLOW__DATABASE__SQL_ALCHEMY_CONN': sqlalchemy_conn,
        'AIRFLOW__CORE__FERNET_KEY': context.properties['airflow']['fernetKey'],
        'AIRFLOW__CELERY__BROKER_URL': f'amqp://{hostname_manager}',
        'AIRFLOW__CELERY__CELERY_RESULT_BACKEND': f'db+{sqlalchemy_conn}',
        'AIRFLOW__WEBSERVER__SECRET_KEY': context.properties['airflow']['secretKey'],
        'AIRFLOW__LOGGING__REMOTE_LOGGING': 'True',
        'AIRFLOW__LOGGING__REMOTE_LOG_CONN_ID': 'GCSConn',
        'AIRFLOW__LOGGING__BASE_LOG_FOLDER': '/usr/local/airflow/logs',
        'AIRFLOW__LOGGING__REMOTE_BASE_LOG_FOLDER': f'{context.properties["airflow"]["remoteLogFolder"]}/{context.env["deployment"]}',
        'AIRFLOW__METRICS__STATSD_ON': 'True',
        'AIRFLOW__METRICS__STATSD_HOST': hostname_manager,
        'AIRFLOW__METRICS__STATSD_PORT': 9125,
        'REDIS_SERVER': hostname_manager,
    }

    return airflow_variable


def GenerateBootDisk(diskSizeGb):
    ubuntu_release = 'family/ubuntu-2204-lts'
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
