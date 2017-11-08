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
                    'PublicIp': '12.23.34.45',
                    'AllocationId': 'eipalloc-12345678'},
                   {'NetworkInterfaceOwnerId': '12345678',
                    'PublicIp': '22.33.44.55',
                    'AllocationId': 'eipalloc-22334455'}]

    boto.assert_called_with('ec2')


def test_filter_asgs(fx_asgs, fx_asgs_expected):
    assert postgresql.filter_asgs(conftest.pg_infrastructure_account, fx_asgs) == fx_asgs_expected


def test_filter_instances(fx_pg_instances, fx_pg_instances_expected):
    assert postgresql.filter_instances(conftest.pg_infrastructure_account, fx_pg_instances) == fx_pg_instances_expected


def test_collect_launch_configurations(monkeypatch, fx_launch_configuration, fx_launch_configuration_expected):
    asg = MagicMock()
    asg.get_paginator.return_value.paginate.return_value.build_full_result.return_value = fx_launch_configuration
    boto = get_boto_client(monkeypatch, asg)

    res = postgresql.collect_launch_configurations(conftest.pg_infrastructure_account)

    assert res == fx_launch_configuration_expected

    asg.get_paginator.assert_called_with('describe_launch_configurations')
    boto.assert_called_with('autoscaling')


def test_extract_eipalloc_from_lc(monkeypatch, fx_eip_allocation, fx_launch_configuration_expected):
    def lcs(i):
        return fx_launch_configuration_expected
    monkeypatch.setattr(postgresql, 'collect_launch_configurations', lcs)

    res = postgresql.extract_eipalloc_from_lc(
        postgresql.collect_launch_configurations(conftest.pg_infrastructure_account), conftest.PG_CLUSTER)

    assert res == fx_eip_allocation


def test_get_postgresql_clusters(
        monkeypatch, fx_addresses_expected, fx_asgs_expected, fx_pg_instances_expected,
        fx_eip_allocation, fx_launch_configuration_expected
):
    def addresses(i):
        return fx_addresses_expected
    monkeypatch.setattr(postgresql, 'collect_addresses', addresses)

    def lcs(i):
        return fx_launch_configuration_expected
    monkeypatch.setattr(postgresql, 'collect_launch_configurations', lcs)

    def allocs(i, j):
        return fx_eip_allocation
    monkeypatch.setattr(postgresql, 'extract_eipalloc_from_lc', allocs)

    entities = postgresql.get_postgresql_clusters(conftest.REGION, conftest.pg_infrastructure_account,
                                                  fx_asgs_expected, fx_pg_instances_expected)

    assert entities == [{'type': 'postgresql_cluster',
                         'id': 'pg-bla[aws:12345678:eu-central-1]',
                         'region': conftest.REGION,
                         'spilo_cluster': 'bla',
                         'elastic_ip': '12.23.34.45',
                         'elastic_ip_instance_id': 'i-1234',
                         'allocation_error': '',
                         'instances': [{'instance_id': 'i-1234',
                                        'private_ip': '192.168.1.1',
                                        'role': 'master'},
                                       {'instance_id': 'i-02e0',
                                        'private_ip': '192.168.1.3',
                                        'role': 'replica'}],
                         'infrastructure_account': conftest.pg_infrastructure_account},
                        {'type': 'postgresql_cluster',
                         'id': 'pg-malm[aws:12345678:eu-central-1]',
                         'region': conftest.REGION,
                         'spilo_cluster': 'malm',
                         'elastic_ip': '22.33.44.55',
                         'elastic_ip_instance_id': '',
                         'allocation_error': 'There is a public IP defined but not attached to any instance',
                         'instances': [{'instance_id': 'i-4444',
                                        'private_ip': '192.168.13.32',
                                        'role': 'master'},
                                       {'instance_id': 'i-5555',
                                        'private_ip': '192.168.31.154',
                                        'role': 'replica'}],
                         'infrastructure_account': conftest.pg_infrastructure_account}]
