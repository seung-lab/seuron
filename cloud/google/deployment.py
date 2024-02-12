import json
from workers import GenerateWorkers
from manager import GenerateManager
from easyseg_worker import GenerateEasysegWorker
from nfs_server import GenerateNFSServer
from networks import GenerateNetworks

def GenerateConfig(context):

    common_ext = f"{context.properties['zone']}.c.{context.env['project']}.internal"
    hostname_manager = f"{context.env['deployment']}-bootstrap.{common_ext}"
    if "nfsServer" in context.properties:
        hostname_nfs_server = f"{context.env['deployment']}-nfs-server.{common_ext}"
    else:
        hostname_nfs_server = None

    workers = context.properties['workerInstanceGroups']
    worker_resource = []
    worker_metadata = []
    worker_subnetworks = set()
    clusters = workers.copy()
    for w in clusters:
        resource = GenerateWorkers(context, hostname_manager, hostname_nfs_server, w)
        worker_resource += resource
        w['name'] = resource[1]['name']
        worker_subnetworks.add(w['subnetwork'])

    worker_metadata = [{
        'key': 'cluster-info',
        'value': json.dumps(clusters)
    }]

    if "easysegWorker" in context.properties:
        worker_subnetworks.add(context.properties['easysegWorker']['subnetwork'])
        worker_metadata.append({
            'key': 'easyseg-worker',
            'value': json.dumps(context.properties['easysegWorker'])
        })

    if "nfsServer" in context.properties:
        worker_subnetworks.add(context.properties['nfsServer']['subnetwork'])
        worker_metadata.append({
            'key': 'nfs-server',
            'value': json.dumps(context.properties['nfsServer'])
        })

    manager_resource = GenerateManager(context, hostname_manager, hostname_nfs_server, worker_metadata)

    network_resource = GenerateNetworks(context, list(worker_subnetworks))

    resources = {
        'resources': worker_resource+manager_resource+network_resource
    }

    if "easysegWorker" in context.properties:
        hostname_easyseg_worker = f"{context.env['deployment']}-easyseg-worker.{common_ext}"
        easyseg_worker_resource = GenerateEasysegWorker(context, hostname_manager, hostname_easyseg_worker)
        resources['resources'] += easyseg_worker_resource

    if "nfsServer" in context.properties:
        nfs_server_resource = GenerateNFSServer(context, hostname_manager, hostname_nfs_server)
        resources['resources'] += nfs_server_resource

    return resources
