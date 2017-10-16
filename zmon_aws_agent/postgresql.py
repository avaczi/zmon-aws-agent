import logging
import psycopg2
import boto3

# better move that one to common?
from zmon_aws_agent.aws import entity_id
from zmon_aws_agent.common import call_and_retry


logger = logging.getLogger(__name__)

POSTGRESQL_DEFAULT_PORT = 5432


def list_postgres_databases(*args, **kwargs):
    logger.info("Trying to list DBs on host: {}".format(kwargs.get('host')))
    try:
        conn = psycopg2.connect(*args, **kwargs)
        cur = conn.cursor()
        cur.execute("""
            SELECT datname
              FROM pg_database
             WHERE datname NOT IN('postgres', 'template0', 'template1')
        """)
        return [row[0] for row in cur.fetchall()]
    except:
        logger.exception("Failed to list DBs!")
        return []


def get_databases_from_clusters(pgclusters, infrastructure_account, region,
                                postgresql_user, postgresql_pass):
    entities = []

    try:
        for pg in pgclusters:
            dnsname = pg['dnsname']
            dbnames = list_postgres_databases(host=dnsname,
                                              port=POSTGRESQL_DEFAULT_PORT,
                                              user=postgresql_user,
                                              password=postgresql_pass,
                                              dbname='postgres',
                                              sslmode='require')
            for db in dbnames:
                entity = {
                    'id': entity_id('{}-{}[{}:{}]'.format(db, dnsname, infrastructure_account, region)),
                    'type': 'postgresql_database',
                    'created_by': 'agent',
                    'infrastructure_account': infrastructure_account,
                    'region': region,

                    'postgresql_cluster': pg['id'],
                    'database_name': db,
                    'shards': {
                        db: '{}:{}/{}'.format(dnsname, POSTGRESQL_DEFAULT_PORT, db)
                    }
                }
                entities.append(entity)
    except:
        logger.exception("Failed to make Database entities for PostgreSQL clusters!")

    return entities


def collect_addresses(infrastructure_account):
    ec2 = boto3.client('ec2')

    addresses = call_and_retry(ec2.describe_addresses)['Addresses']

    return [a for a in addresses if a['NetworkInterfaceOwnerId'] == infrastructure_account.split(':')[1]]
# FIXME: depend on region, too?


def collect_asgs(infrastructure_account):
    asg = boto3.client('autoscaling')

    asg_paginator = asg.get_paginator('describe_auto_scaling_groups')
    all_groups = call_and_retry(lambda: asg_paginator.paginate().build_full_result()['AutoScalingGroups'])

    return [gr for gr in all_groups
            if gr['AutoScalingGroupARN'].split(':')[4] == infrastructure_account.split(':')[1]
            and ('Key', 'SpiloCluster') in [i for t in [g.items() for g in gr['Tags']] for i in t]]


def collect_instances(infrastructure_account):
    ec2 = boto3.client('ec2')

    inst_paginator = ec2.get_paginator('describe_instances')
    instances = inst_paginator.paginate().build_full_result()['Reservations']

    # we assume only one instance per reservation
    return [i['Instances'][0] for i in instances if i['OwnerId'] == infrastructure_account.split(':')[1]]


def get_postgresql_clusters(region, infrastructure_account):
    entities = []

    addresses = collect_addresses(infrastructure_account)
    spilo_asgs = collect_asgs(infrastructure_account)
    instances = collect_instances(infrastructure_account)

    # we will use the ASGs as a skeleton for building the entities
    for cluster in spilo_asgs:
        cluster_name = [t['Value'] for t in cluster['Tags'] if t['Key'] == 'SpiloCluster'][0]

        cluster_instances = []
        eip = []

        for i in cluster['Instances']:
            instance_id = i['InstanceId']

            try:
                i_data = [inst for inst in instances if inst['InstanceId'] == instance_id][0]
            except IndexError:
                raise Exception(str(cluster_instances))
            private_ip = i_data['PrivateIpAddress']
            role = [d['Value'] for d in i_data['Tags'] if d['Key'] == 'Role'][0]

            cluster_instances.append({'instance_id': instance_id,
                                      'private_ip': private_ip,
                                      'role': role})

            address = [a for a in addresses if a.get('InstanceId') == instance_id]
            if address:
                eip.append(address[0])  # we expect only one EIP per instance

        if len(eip) > 1:
            pass  # throw an error?  should not happen, but who knows
        elif not eip:
            public_ip = ''
            public_ip_instance_id = ''
        else:
            public_ip = eip[0]['PublicIp']
            public_ip_instance_id = eip[0]['InstanceId']

        entities.append({'type': 'postgresql_cluster',
                         'id': entity_id('pg-{}[{}:{}]'.format(cluster_name, infrastructure_account, region)),
                         'region': region,
                         'spilo_cluster': cluster_name,
                         'public_ip': public_ip,
                         'public_ip_instance_id': public_ip_instance_id,
                         'instances': cluster_instances,
                         'infrastructure_account': infrastructure_account})

    return entities
