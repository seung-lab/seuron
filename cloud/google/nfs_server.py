from common import ZonalComputeUrl, GenerateDisk, GenerateBootDisk, GenerateNetworkInterface


def GenerateNFSServerStartupScript():
    startup_script = '''
#!/bin/bash
set -e

mkdir -p /share
#DRIVES=($(lsblk | grep -oE 'nvme[a-z0-9A-Z]*' | cut -d' ' -f1 | awk '{ print "/dev/"$1 }'))
#mdadm --create /dev/md0 --level=0 --raid-devices=${#DRIVES[@]} ${DRIVES[@]}
#mkfs.ext4 -F /dev/md0
#mount /dev/md0 /share

if [ ! -f "/etc/bootstrap_done" ]; then

mkfs.ext4 -F /dev/sdb
mount /dev/sdb /share
chmod 777 /share
apt-get install nfs-kernel-server -y
echo "/share 172.31.0.0/16(insecure,rw,async,no_subtree_check)" >> /etc/exports
echo "ALL: 172.31.0.0/16" >> /etc/hosts.allow
systemctl start nfs-kernel-server.service
cat << EOF > /etc/nfs.conf.d/local.conf
[nfsd]
threads = 64
EOF
systemctl restart nfs-kernel-server.service
touch /etc/bootstrap_done
sleep 300
shutdown -h now

fi

mount /dev/sdb /share
chmod 777 /share
systemctl restart nfs-kernel-server.service
'''
    return startup_script


def GenerateNFSServer(context, hostname_nfs_server):
    nfs_server_param = context.properties["nfsServer"]

    startup_script = GenerateNFSServerStartupScript()

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
                'https://www.googleapis.com/auth/logging.write',
                'https://www.googleapis.com/auth/monitoring.write',
            ],
        }],
    }

    nfs_resource = {
        'name': hostname_nfs_server.split('.')[0],
        'type': 'compute.v1.instance',
        'properties': instance_resource,
    }

    return [nfs_resource,]
