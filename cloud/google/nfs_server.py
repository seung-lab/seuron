from common import ZonalComputeUrl, GenerateDisk, GenerateBootDisk, GenerateNetworkInterface, INSTALL_DOCKER_CMD, GenerateAirflowVar
from workers import GenerateDockerCommand, GenerateCeleryWorkerCommand, GenerateEnvironVar


def GenerateNFSServerStartupScript(context, hostname_manager):
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

    startup_script = f'''
#!/bin/bash
set -e

mkdir -p /share
mkdir -p /var/log/airflow/logs
chmod 777 /var/log/airflow/logs

DEBIAN_FRONTEND=noninteractive apt-get -y -o Dpkg::Options::="--force-confdef" -o Dpkg::Options::="--force-confold" dist-upgrade
{INSTALL_DOCKER_CMD}

{GenerateEnvironVar(context, env_variables)}

if [ ! -f "/etc/bootstrap_done" ]; then

mkfs.ext4 -F /dev/sdb
mount /dev/sdb /share
chmod 777 /share
mkdir -p /share/mariadb
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

sysctl -w net.netfilter.nf_conntrack_max=2097152
echo 524288 > /sys/module/nf_conntrack/parameters/hashsize
mount /dev/sdb /share
chmod 777 /share
systemctl restart nfs-kernel-server.service
docker run --rm -p 3306:3306 --tmpfs /tmp:rw -v /share/mariadb:/var/lib/mysql --env MARIADB_ROOT_PASSWORD=igneous --env MARIADB_USER=igneous --env MARIADB_PASSWORD=igneous mariadb:latest --max-connections=10000 --innodb-buffer-pool-size=64G --innodb-log-file-size=10G >& /dev/null &
{oom_canary_cmd} &
{worker_cmd}

'''
    return startup_script


def GenerateNFSServer(context, hostname_manager, hostname_nfs_server):
    nfs_server_param = context.properties["nfsServer"]

    startup_script = GenerateNFSServerStartupScript(context, hostname_manager)

    diskType = ZonalComputeUrl(
        context.env['project'],
        nfs_server_param['zone'],
        'diskTypes', 'pd-ssd')

    instance_resource = {
        'zone': nfs_server_param['zone'],
        'machineType': ZonalComputeUrl(
                      context.env['project'], nfs_server_param['zone'],
                      'machineTypes', nfs_server_param['machineType']
        ),
        'disks': [
                  GenerateBootDisk(diskSizeGb=10),
                  GenerateDisk(diskSizeGb=nfs_server_param['nfsVolumeSizeGB'], diskType=diskType),
        ],
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
