from common import ZonalComputeUrl, GenerateDisk, GenerateBootDisk, GenerateScratchDisk, GenerateNetworkInterface, INSTALL_DOCKER_CMD, GenerateAirflowVar
from workers import GenerateDockerCommand, GenerateCeleryWorkerCommand, GenerateEnvironVar


def GenerateNFSServerStartupScript(context, hostname_manager):
    nfs_server_param = context.properties["nfsServer"]
    env_variables = GenerateAirflowVar(context, hostname_manager)

    docker_env = [f'-e {k}' for k in env_variables]
    docker_image = context.properties['seuronImage']

    oom_canary_cmd = GenerateDockerCommand(docker_image, docker_env) + ' ' + "python utils/memory_monitor.py ${AIRFLOW__CELERY__BROKER_URL} bot-message-queue >& /dev/null"
    worker_cmd = GenerateCeleryWorkerCommand(docker_image, docker_env+['-p 8793:8793'], queue="nfs", concurrency=1)
    nginx_conf = '''worker_processes auto;
worker_rlimit_nofile 2048;
events {
    worker_connections 1024;
    use epoll;
}

http {
    sendfile on;
    tcp_nopush on;
    tcp_nodelay on;
    keepalive_timeout 65;
    access_log  /dev/null;

    server {
        listen 80;
        root /var/www/html;
        location / {
            try_files \$uri \$uri/ =404;
        }
        location /share/ {
            alias /share/;
        }
        location /nginx_status {
            stub_status;
        }
    }
}
'''
    scratch_nvme_config = '''
DRIVES=($(lsblk | grep -oE 'nvme[a-z0-9A-Z]*' | cut -d' ' -f1 | awk '{ print "/dev/"$1 }'))
if [ ${#DRIVES[@]} -ne 0 ]; then
mdadm --create /dev/md0 --level=0 --force --raid-devices=${#DRIVES[@]} ${DRIVES[@]}
mkfs.ext4 -F /dev/md0
mount /dev/md0 /share
chmod 777 /share
mkdir -p /share/mariadb
mkdir -p /share/postgresql/data
fi
'''
    startup_script = f'''
#!/bin/bash
set -e

mkdir -p /share
mkdir -p /var/log/airflow/logs
chmod 777 /var/log/airflow/logs

DEBIAN_FRONTEND=noninteractive apt-get -y -o Dpkg::Options::="--force-confdef" -o Dpkg::Options::="--force-confold" dist-upgrade
{INSTALL_DOCKER_CMD}
{scratch_nvme_config}

{GenerateEnvironVar(context, env_variables)}

if [ ! -f "/etc/bootstrap_done" ]; then
if ! mountpoint -q "/share"; then
mkfs.ext4 -F /dev/sdb
mount /dev/sdb /share
chmod 777 /share
mkdir -p /share/mariadb
mkdir -p /share/postgresql/data
fi
apt-get install nfs-kernel-server nginx -y
echo "/share 172.31.0.0/16(insecure,rw,async,no_subtree_check)" >> /etc/exports
echo "ALL: 172.31.0.0/16" >> /etc/hosts.allow
systemctl start nfs-kernel-server.service
cat << EOF > /etc/nfs.conf.d/local.conf
[nfsd]
threads = 64
EOF
cat << EOF > /etc/nginx/nginx.conf
{nginx_conf}
EOF
systemctl restart nfs-kernel-server.service
touch /etc/bootstrap_done
sleep 300
shutdown -h now

fi

sysctl -w net.netfilter.nf_conntrack_max=$(awk '/MemAvailable/ {{print int($2/16)}}' /proc/meminfo)
echo $(awk '/MemAvailable/ {{print int($2/64)}}' /proc/meminfo) > /sys/module/nf_conntrack/parameters/hashsize
if ! mountpoint -q "/share"; then
mount /dev/sdb /share
chmod 777 /share
fi
systemctl restart nfs-kernel-server.service
export POSTGRES_MEM_GB=$(awk '/MemAvailable/ {{print int($2/1024/1024/4)}}' /proc/meminfo)
export POSTGRES_MAX_CONN=$(awk '/MemAvailable/ {{print int($2/1024/32)}}' /proc/meminfo)
docker network create airflow-net || true
docker run --rm --name postgres --network airflow-net --shm-size=2g --tmpfs /tmp:rw -v /share/postgresql/data:/var/lib/postgresql/data --env POSTGRES_PASSWORD=airflow postgres:15-alpine -c max_connections=${{POSTGRES_MAX_CONN}} -c shared_buffers=${{POSTGRES_MEM_GB}}GB > /var/log/airflow/logs/postgres.log 2>&1 &
docker run --rm --name pgbouncer --network airflow-net -p 5432:5432 -e DB_USER=postgres -e DB_PASSWORD=airflow -e DB_NAME='*' -e DB_HOST=postgres -e DB_PORT=5432 -e AUTH_TYPE=scram-sha-256 -e MAX_CLIENT_CONN=100000 -e DEFAULT_POOL_SIZE=${{POSTGRES_MAX_CONN}} -e POOL_MODE=transaction ranlu/pgbouncer:1.24.1 > /var/log/airflow/logs/pgbouncer.log 2>&1 &
{oom_canary_cmd} &
{worker_cmd}

'''
    return startup_script


def GenerateNFSServer(context, hostname_manager, hostname_nfs_server):
    nfs_server_param = context.properties["nfsServer"]

    if ('pdSSDSizeGB' in nfs_server_param) == ('numLocalScratchSSD' in nfs_server_param):
        raise ValueError("You must set one and only one of 'pdSSDSizeGB' and 'numLocalScratchSSD'")

    startup_script = GenerateNFSServerStartupScript(context, hostname_manager)

    disks = [GenerateBootDisk(diskSizeGb=100),]

    if 'pdSSDSizeGB' in nfs_server_param:
        diskType = ZonalComputeUrl(
            context.env['project'],
            nfs_server_param['zone'],
            'diskTypes', 'pd-ssd')
        disks.append(GenerateDisk(diskSizeGb=nfs_server_param['pdSSDSizeGB'], diskType=diskType))
    elif 'numLocalScratchSSD' in nfs_server_param:
        diskType = ZonalComputeUrl(
            context.env['project'],
            nfs_server_param['zone'],
            'diskTypes', 'local-ssd')
        disks += GenerateScratchDisk(numDisks=nfs_server_param['numLocalScratchSSD'], diskType=diskType)


    instance_resource = {
        'zone': nfs_server_param['zone'],
        'machineType': ZonalComputeUrl(
                      context.env['project'], nfs_server_param['zone'],
                      'machineTypes', nfs_server_param['machineType']
        ),
        'disks': disks,
        'labels': {
            'vmrole': 'nfs-server',
            'location': nfs_server_param['zone'],
            'deployment': context.env['deployment'],
        },
        'tags': {
            'items': ['princeton-access'],
        },
        'metadata': {
            'items': [
                {
                    'key': 'startup-script',
                    'value': startup_script,
                }
            ],
        },
        'networkInterfaces': [GenerateNetworkInterface(context, nfs_server_param['subnetwork'])],
        'serviceAccounts': [{
            'scopes': [
                'https://www.googleapis.com/auth/compute',
                'https://www.googleapis.com/auth/logging.write',
                'https://www.googleapis.com/auth/monitoring.write',
                'https://www.googleapis.com/auth/devstorage.read_write',
            ],
        }],
    }

    nfs_resource = {
        'name': hostname_nfs_server.split('.')[0],
        'type': 'compute.v1.instance',
        'properties': instance_resource,
    }

    return [nfs_resource,]
