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
    ec2 = MagicMock()
    ec2.describe_addresses.return_value = fx_addresses
    boto = get_boto_client(monkeypatch, ec2)

    res = postgresql.collect_addresses(conftest.pg_infrastructure_account)

    assert res == [{'NetworkInterfaceOwnerId': '12345678',
                    'InstanceId': 'i-1234',
                    'PublicIp': '12.23.34.45'}]

    boto.assert_called_with('ec2')


def test_collect_asgs(monkeypatch, fx_asgs):
    asg = MagicMock()
    asg.get_paginator.return_value.paginate.return_value.build_full_result.return_value = fx_asgs
    boto = get_boto_client(monkeypatch, asg)

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
                         'PropagateAtLaunch': 'true'}],
                    'Instances': [
                        {'ProtectedFromScaleIn': 'false',
                         'HealthStatus': 'Healthy',
                         'LifecycleState': 'InService',
                         'InstanceId': 'i-1234',
                         'AvailabilityZone': 'eu-central-1b'},
                        {'ProtectedFromScaleIn': 'false',
                         'HealthStatus': 'Healthy',
                         'LifecycleState': 'InService',
                         'InstanceId': 'i-02e0',
                         'AvailabilityZone': 'eu-central-1a'}]}]

    asg.get_paginator.assert_called_with('describe_auto_scaling_groups')
    boto.assert_called_with('autoscaling')


def test_collect_instances(monkeypatch, fx_pg_instances):
    ec2 = MagicMock()
    ec2.get_paginator.return_value.paginate.return_value.build_full_result.return_value = fx_pg_instances
    boto = get_boto_client(monkeypatch, ec2)

    res = postgresql.collect_instances(conftest.pg_infrastructure_account)

    assert res == [{'InstanceId': 'i-1234',
                    'PrivateIpAddress': '192.168.1.1',
                    'Tags': [
                        {'Key': 'Role',
                         'Value': 'master'},
                        {'Key': 'StackName',
                         'Value': 'spilo'}]},
                   {'InstanceId': 'i-02e0',
                    'PrivateIpAddress': '192.168.1.3',
                    'Tags': [
                        {'Key': 'Role',
                         'Value': 'replica'},
                        {'Key': 'StackName',
                         'Value': 'spilo'}]}]

    ec2.get_paginator.assert_called_with('describe_instances')
    boto.assert_called_with('ec2')


def test_get_postgresql_clusters(monkeypatch, fx_asgs_expected, fx_pg_instances_expected):
    def addresses(i):
        return [{'NetworkInterfaceOwnerId': '12345678',
                 'InstanceId': 'i-1234',
                 'PublicIp': '12.23.34.45'}]
    monkeypatch.setattr(postgresql, 'collect_addresses', addresses)

    def asgs(i):
        return fx_asgs_expected
    monkeypatch.setattr(postgresql, 'collect_asgs', asgs)

    def insts(i):
        return fx_pg_instances_expected
    monkeypatch.setattr(postgresql, 'collect_instances', insts)

    entities = postgresql.get_postgresql_clusters(conftest.REGION, conftest.pg_infrastructure_account)

    assert entities == [{'type': 'postgresql_cluster',
                         'id': 'pg-bla[aws:12345678:eu-central-1]',
                         'region': conftest.REGION,
                         'spilo_cluster': 'bla',
                         'public_ip': '12.23.34.45',
                         'public_ip_instance_id': 'i-1234',
                         'instances': [{'instance_id': 'i-1234',
                                        'private_ip': '192.168.1.1',
                                        'role': 'master'},
                                       {'instance_id': 'i-02e0',
                                        'private_ip': '192.168.1.3',
                                        'role': 'replica'}],
                         'infrastructure_account': conftest.pg_infrastructure_account}]
