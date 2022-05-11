from workers import GenerateWorkers
from manager import GenerateManager
from networks import GenerateNetworks

def GenerateConfig(context):

    hostname_manager = f"{context.env['deployment']}-bootstrap"
    workers = context.properties['workerInstanceGroups']
    worker_resource = []
    worker_metadata = []
    worker_subnetworks = set()
    for w in workers:
        resource = GenerateWorkers(context, hostname_manager, w)
        worker_resource += resource
        worker_metadata.append({
            'key': resource[1]['name'],
            'value': w['sizeLimit']
        })
        worker_subnetworks.add(w['subnetwork'])

    manager_resource = GenerateManager(context, hostname_manager, worker_metadata)

    network_resource = GenerateNetworks(context, list(worker_subnetworks))

    resources = {
        'resources': worker_resource+manager_resource+network_resource
    }

    return resources


