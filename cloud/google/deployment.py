import json
from workers import GenerateWorkers
from manager import GenerateManager
from networks import GenerateNetworks

def GenerateConfig(context):

    common_ext = f"{context.properties['zone']}.c.{context.env['project']}.internal"
    hostname_manager = f"{context.env['deployment']}-bootstrap.{common_ext}"
    workers = context.properties['workerInstanceGroups']
    worker_resource = []
    worker_metadata = []
    worker_subnetworks = set()
    clusters = workers.copy()
    for w in clusters:
        resource = GenerateWorkers(context, hostname_manager, w)
        worker_resource += resource
        w['name'] = resource[1]['name']
        worker_subnetworks.add(w['subnetwork'])

    worker_metadata = [{
        'key': 'cluster-info',
        'value': json.dumps(clusters)
    }]

    manager_resource = GenerateManager(context, hostname_manager, worker_metadata)

    network_resource = GenerateNetworks(context, list(worker_subnetworks))

    resources = {
        'resources': worker_resource+manager_resource+network_resource
    }

    return resources


