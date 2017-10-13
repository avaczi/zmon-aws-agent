from mock import MagicMock
from test_aws import get_boto_client
import conftest

import zmon_aws_agent.postgresql as postgresql


def test_get_databases_from_clusters():
    pgclusters = [
        {
            'id': 'test-1',
            'dnsname': 'test-1.db.zalan.do'
        }
    ]
    acc = 'aws:1234567890'
    region = 'eu-xxx-1'

    postgresql.list_postgres_databases = MagicMock()
    postgresql.list_postgres_databases.return_value = ['db1', 'db2']

    databases = postgresql.get_databases_from_clusters(pgclusters, acc, region,
                                                       'pguser', 'pgpass')
    assert databases == [
        {
            'id': 'db1-test-1.db.zalan.do[aws:1234567890:eu-xxx-1]',
            'type': 'postgresql_database',
            'created_by': 'agent',
            'infrastructure_account': acc,
            'region': region,
            'postgresql_cluster': 'test-1',
            'database_name': 'db1',
            'shards': {
                'db1': 'test-1.db.zalan.do:5432/db1'
            }
        },
        {
            'id': 'db2-test-1.db.zalan.do[aws:1234567890:eu-xxx-1]',
            'type': 'postgresql_database',
            'created_by': 'agent',
            'infrastructure_account': acc,
            'region': region,
            'postgresql_cluster': 'test-1',
            'database_name': 'db2',
            'shards': {
                'db2': 'test-1.db.zalan.do:5432/db2'
            }
        }
    ]


def test_collect_addresses(monkeypatch, fx_addresses):
    ec2_client = MagicMock()
    ec2_client.describe_addresses.return_value = fx_addresses
    boto = get_boto_client(monkeypatch, ec2_client)

    res = postgresql.collect_addresses(conftest.pg_infrastructure_account)

    assert res == [{'NetworkInterfaceOwnerId': '12345678',
                    'InstanceId': 'i-1234',
                    'PublicIp': '12.23.34.45'}]

    boto.assert_called_with('ec2')


def test_collect_asgs(monkeypatch, fx_asgs):
    asg_client = MagicMock()
    asg_client.get_paginator.return_value.paginate.return_value.build_full_result.return_value = fx_asgs
    boto = get_boto_client(monkeypatch, asg_client)

    res = postgresql.collect_asgs(conftest.pg_infrastructure_account)

    assert res == [{'AutoScalingGroupARN': 'arn:aws:autoscaling:eu-central-1:12345678:autoScalingGroup:aaa:bla',
                    'Tags': [
                        {'Key': 'Name',
                         'Value': 'spilo-bla',
                         'ResourceId': 'bla-AppServer-1A',
                         'ResourceType': 'auto-scaling-group',
                         'PropagateAtLaunch': 'true'},
                        {'Key': 'SpiloCluster',
                         'Value': 'bla',
                         'ResourceId': 'bla-AppServer-1A',
                         'ResourceType': 'auto-scaling-group',
                         'PropagateAtLaunch': 'true'}]}]

    asg_client.get_paginator.assert_called_with('describe_auto_scaling_groups')
    boto.assert_called_with('autoscaling')


def test_collect_instances(monkeypatch, fx_pg_instances):
    ec2_client = MagicMock()
    ec2_client.get_paginator.return_value.paginate.return_value.build_full_result.return_value = fx_pg_instances
    boto = get_boto_client(monkeypatch, ec2_client)

    res = postgresql.collect_instances(conftest.pg_infrastructure_account)

    assert res == [{'InstanceId': 'i-1234',
                    'PrivateIpAddress': '192.168.1.1',
                    'Tags': [
                        {'Key': 'Role',
                         'Value': 'master'},
                        {'Key': 'StackName',
                         'Value': 'spilo'}]}]

    ec2_client.get_paginator.assert_called_with('describe_instances')
    boto.assert_called_with('ec2')
