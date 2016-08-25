import json
import base64

import pytest

from botocore.exceptions import ClientError

from zmon_aws_agent.aws import get_hash


ACCOUNT = 'aws:1234'
REGION = 'eu-central-1'


class ThrottleError(ClientError):
    def __init__(self, throttling=True):
        self.throttling = throttling
        self.response = {'Error': {'Code': 'Throttling' if throttling else 'BadRequest'}}


def get_elc_cluster():
    cluster = {
        'CacheClusterStatus': 'available',
        'CacheClusterId': 'elc-1',
        'Engine': 'redis',
        'EngineVersion': '1.0.5',
        'NumCacheNodes': 2,
        'CacheNodeType': 'redis',
        'ReplicationGroupId': 'elc-1-replica',
        'CacheNodes': [
            {
                'CacheNodeStatus': 'available', 'CacheNodeId': 'elc-n-1',
                'Endpoint': {'Port': 2727, 'Address': '0.0.0.0'}
            },
            {'CacheNodeStatus': 'unknown'}
        ]
    }

    resp = {
        'CacheClusters': [cluster.copy() for i in range(4)]
    }

    statuses = ('available', 'modifying', 'snapshotting', 'unknown')
    for idx, c in enumerate(resp['CacheClusters']):
        c['CacheClusterStatus'] = statuses[idx]

    node = {
        'id': 'elc-elc-1-elc-n-1[{}:{}]'.format(ACCOUNT, REGION),
        'region': REGION,
        'created_by': 'agent',
        'infrastructure_account': ACCOUNT,
        'type': 'elc',
        'cluster_id': 'elc-1',
        'node_id': 'elc-n-1',
        'engine': 'redis',
        'version': '1.0.5',
        'cluster_num_nodes': 2,
        'host': '0.0.0.0',
        'port': 2727,
        'instance_type': 'redis',
        'replication_group': 'elc-1-replica',
    }

    return resp, [node] * 3


def get_autoscaling():
    asg = {
        'AutoScalingGroupName': 'asg-1',
        'AvailabilityZones': ['zone-1', 'zone-2'],
        'DesiredCapacity': '3',
        'MaxSize': 10,
        'MinSize': 3,
        'Instances': [
            {'InstanceId': 'ins-1', 'LifecycleState': 'InService'},
            {'InstanceId': 'ins-2', 'LifecycleState': 'InService'},
            {'InstanceId': 'ins-3', 'LifecycleState': 'InService'},
            {'InstanceId': 'ins-4', 'LifecycleState': 'unknown'},
        ]
    }

    reservations = {
        'Reservations': [
            {
                'Instances': [
                    {'PrivateIpAddress': '192.168.20.16', 'InstanceId': 'ins-1'},
                    {'InstanceId': 'ins-2'}
                ]
            }
        ]
    }

    instance_ids = ['ins-1', 'ins-2', 'ins-3']

    resp = {
        'AutoScalingGroups': [asg]
    }

    result = [
        {
            'id': 'asg-asg-1[{}:{}]'.format(ACCOUNT, REGION),
            'type': 'asg',
            'infrastructure_account': ACCOUNT,
            'region': REGION,
            'created_by': 'agent',
            'name': 'asg-1',
            'availability_zones': ['zone-1', 'zone-2'],
            'desired_capacity': '3',
            'max_size': 10,
            'min_size': 3,
            'instances': [{'aws_id': 'ins-1', 'ip': '192.168.20.16'}],
        }
    ]

    return resp, reservations, instance_ids, result


def get_elbs():
    resp = {
        'LoadBalancerDescriptions': [
            {
                'LoadBalancerName': 'elb-1',
                'DNSName': 'elb-1.example.org',
                'Scheme': 'https',
                'Instances': ['ins-1', 'ins-2', 'ins-3'],
            },
        ]
    }

    tags = {'TagDescriptions': [{'LoadBalancerName': 'elb-1'}]}

    health = {
        'InstanceStates': [
            {'State': 'InService'},
            {'State': 'InService'},
            {'State': 'OutOfService'},
        ]
    }

    result = [
        {
            'id': 'elb-elb-1[{}:{}]'.format(ACCOUNT, REGION),
            'type': 'elb',
            'infrastructure_account': ACCOUNT,
            'region': REGION,
            'created_by': 'agent',
            'dns_name': 'elb-1.example.org',
            'host': 'elb-1.example.org',
            'name': 'elb-1',
            'scheme': 'https',
            'url': 'https://elb-1.example.org',
            'members': 3,
            'active_members': 2,
        }
    ]

    return resp, tags, health, result


def get_apps():
    resp = {
        'Reservations': [
            {
                'OwnerId': '1234',
                'Instances': [
                    {
                        'State': {'Name': 'running'},
                        'PrivateIpAddress': '192.168.20.16', 'PublicIpAddress': '194.194.20.16',
                        'InstanceType': 't2.medium', 'InstanceId': 'ins-1', 'StateTransitionReason': 'state',
                        'Tags': [
                            {'Key': 'Name', 'Value': 'stack-1'}, {'Key': 'StackVersion', 'Value': 'stack-1-1.0'},
                            {'Key': 'aws:cloudformation:logical-id', 'Value': 'cd-app'}
                        ]
                    },
                    {
                        'State': {'Name': 'running'},
                        'PrivateIpAddress': '192.168.20.16',
                        'InstanceType': 't2.medium', 'InstanceId': 'ins-2', 'StateTransitionReason': 'state'
                    },
                    {
                        'State': {'Name': 'terminated'},
                    }
                ],
            }
        ]
    }

    status_resp = {'InstanceStatuses': [{'Events': ['ev-1', 'ev-2']}]}

    user_data = [
        {
            'application_id': 'app-1', 'source': 'https://src', 'ports': [2222], 'runtime': 'docker',
            'application_version': '1.0',
        },
        {
            'no-appliacation-id': 'dummy'
        }
    ]

    user_resp = [{'UserData': {'Value': base64.encodebytes(bytes(json.dumps(u), 'utf-8'))}} for u in user_data]

    result = [
        {
            'id': 'app-1-stack-1-1.0-{}[{}:{}]'.format(get_hash('192.168.20.16'), ACCOUNT, REGION),
            'type': 'instance', 'created_by': 'agent', 'region': REGION, 'infrastructure_account': 'aws:1234',
            'ip': '192.168.20.16', 'host': '192.168.20.16', 'public_ip': '194.194.20.16',
            'instance_type': 't2.medium', 'aws_id': 'ins-1',
            'state_reason': 'state', 'stack': 'stack-1', 'stack_version': 'stack-1-1.0',
            'resource_id': 'cd-app', 'application_id': 'app-1', 'application_version': '1.0', 'source': 'https://src',
            'ports': [2222], 'runtime': 'docker', 'aws:cloudformation:logical_id': 'cd-app', 'name': 'stack-1',
            'events': ['ev-1', 'ev-2'],
        },
        {
            'id': 'ins-2-{}[{}:{}]'.format(get_hash('192.168.20.16'), ACCOUNT, REGION),
            'type': 'instance', 'created_by': 'agent', 'region': REGION, 'infrastructure_account': 'aws:1234',
            'ip': '192.168.20.16', 'host': '192.168.20.16',
            'instance_type': 't2.medium', 'aws_id': 'ins-2',
        }
    ]

    return resp, status_resp, user_resp, result


@pytest.fixture(params=[
    (
        {
            'DBInstances': [
                {'DBInstanceIdentifier': 'db-1', 'Engine': 'e-1', 'Endpoint': {'Port': 5432, 'Address': '0.0.0.0'}},
                {
                    'DBInstanceIdentifier': 'db-2', 'Engine': 'e-1', 'Endpoint': {'Port': 5432, 'Address': '0.0.0.0'},
                    'EngineVersion': '1.0.2', 'DBName': 'db-2-name',
                },
            ]
        },
        [
            {
                'id': 'rds-db-1[{}]', 'name': 'db-1', 'engine': 'e-1', 'port': 5432, 'host': '0.0.0.0',
                'type': 'database', 'shards': {'db-1': '0.0.0.0:5432/db-1'}
            },
            {
                'id': 'rds-db-2[{}]', 'name': 'db-2', 'engine': 'e-1', 'port': 5432, 'host': '0.0.0.0',
                'type': 'database', 'version': '1.0.2', 'shards': {'db-2-name': '0.0.0.0:5432/db-2-name'}
            },
        ]
    ),
    (
        RuntimeError,
        []
    )
])
def fx_rds(request):
    return request.param


@pytest.fixture(params=[
    (
        {
            'TableNames': ['t-1', 't-2', 't-3']  # paginator
        },
        [
            {'Table': {'TableStatus': 'ACTIVE', 'TableName': 't-1', 'TableArn': 'aws.t-1'}},
            {'Table': {'TableStatus': 'UPDATING', 'TableName': 't-2', 'TableArn': 'aws.t-2'}},
            {'Table': {'TableStatus': 'INACTIVE', 'TableName': 't-3', 'TableArn': 'aws.t-3'}},  # describe table
        ],
        [
            {'id': 'dynamodb-t-1[{}:{}]', 'type': 'dynamodb', 'name': 't-1', 'arn': 'aws.t-1'},
            {'id': 'dynamodb-t-2[{}:{}]', 'type': 'dynamodb', 'name': 't-2', 'arn': 'aws.t-2'},  # result
        ]
    ),
    (
        RuntimeError,
        [],
        []
    )
])
def fx_dynamodb(request):
    return request.param
