def GenerateInternalFirewallRules(context, network, sourceRanges):
    return [{
        'name': f'{context.env["deployment"]}-firewall-internal',
        'type': 'compute.v1.firewall',
        'properties': {
            'allowed': [
                {
                    'IPProtocol': 'tcp',
                },
                {
                    'IPProtocol': 'udp',
                },
            ],
            'sourceRanges': sourceRanges,
            'network': f'$(ref.{network}.selfLink)',
        }
    }]


def GenerateExternalFirewallRules(context, network):
    return [
        {
            'name': f'{context.env["deployment"]}-ssh-firewall',
            'type': 'compute.v1.firewall',
            'properties': {
                'allowed': [
                    {
                        'IPProtocol': 'tcp',
                        'ports': [
                            22,
                        ]
                    }
                ],
                'sourceRanges': context.properties['firewallIPRanges'],
                'network': f'$(ref.{network}.selfLink)',
            }
        },
        {
            'name': f'{context.env["deployment"]}-firewall-bootstrap-https',
            'type': 'compute.v1.firewall',
            'properties': {
                'allowed': [
                    {
                        'IPProtocol': 'tcp',
                        'ports': [
                            443,
                        ]
                    }
                ],
                'sourceRanges': context.properties['firewallIPRanges'],
                'network': f'$(ref.{network}.selfLink)',
            }
        },
    ]


def GenerateNetworks(context, subnetworks):
    """Creates the network."""

    network_name = f'{context.env["deployment"]}-network'
    resources = [{
        'name': network_name,
        'type': 'compute.v1.network',
        'properties': {
            'autoCreateSubnetworks': False
        }
    }]

    sourceRanges = []
    for i, region in enumerate(subnetworks):
        ip_range = f'172.31.{i*16}.0/20'
        res = {
            'name': f'{context.env["deployment"]}-{region}-subnetwork',
            'type': 'compute.v1.subnetwork',
            'properties': {
                'region': region,
                'ipCidrRange': ip_range,
                'privateIpGoogleAccess': True,
                'network': f'$(ref.{network_name}.selfLink)',
            }
        }
        sourceRanges.append(ip_range)
        resources.append(res)

    resources += GenerateInternalFirewallRules(context, network_name, sourceRanges)

    resources += GenerateExternalFirewallRules(context, network_name)

    return resources
