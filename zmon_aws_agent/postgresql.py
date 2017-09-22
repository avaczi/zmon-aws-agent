import logging
import psycopg2
import boto3

# better move that one to common?
from .aws import entity_id


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

def get_postgresql_clusters(infrastructure_account, region):

    entities = []

    asg = boto3.client('autoscaling')
    ec2 = boto3.client('ec2')

    asg_paginator = asg.get_paginator('describe_auto_scaling_groups')
    all_groups = asg_paginator.paginate().build_full_result()['AutoScalingGroups']
    our_spilo_groups = [gr for gr in all_groups 
                            if gr['AutoScalingGroupARN'].split(':')[4] == infrastructure_account.split(':')[1]
                            and ('Key', 'SpiloCluster') in [i for t in [g.items() for g in gr['Tags']] for i in t]]

    addresses = ec2.describe_addresses()['Addresses']

    inst_paginator = ec2.get_paginator('describe_instances')
    instances = inst_paginator.paginate().build_full_result()['Reservations']
    instance_data = [i['Instances'][0] for i in instances]

    # we will use the ASGs as a skeleton for building the entities
    for group in our_spilo_groups:
        cluster_name = [t['Value'] for t in group['Tags'] if 'SpiloCluster' in t.values()][0]

        instances = []
        eip = []

        for i in group['Instances']:
            instance_id = i['InstanceId']

            i_data = [i for i in instances_data if i['InstanceId'] == instance_id][0]
            private_ip = i_data['PrivateIpAddress']
            role = [d['Value'] for d in i_data['Tags'] if d['Key'] == 'Role']

            instances.append({'instance_id': instance_id,
                              'private_ip': private_ip,
                              'role': role})
            
            address = [a for a in addresses if a['InstanceId'] == instance_id]
            eip.append(address)

        if len(eip) > 1:
            # throw an error?  should not happen, but who knows
        elif not eip:
            public_ip = ''
            public_ip_instance_id = ''
        else:
            public_ip = eip[0]['PublicIp']
            public_ip_instance_id = eip[0]['InstanceId']
