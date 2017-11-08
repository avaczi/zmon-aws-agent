import logging
import psycopg2
import boto3

# better move that one to common?
from zmon_aws_agent.aws import entity_id
from zmon_aws_agent.common import call_and_retry


logger = logging.getLogger(__name__)

POSTGRESQL_DEFAULT_PORT = 5432


def list_postgres_databases(*args, **kwargs):
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


def collect_eip_addresses(infrastructure_account):
    ec2 = boto3.client('ec2')

    addresses = call_and_retry(ec2.describe_addresses)['Addresses']

    return [a for a in addresses if a['NetworkInterfaceOwnerId'] == infrastructure_account.split(':')[1]]
# FIXME: depend on region, too?


def filter_asgs(infrastructure_account, asgs):
    return [gr for gr in asgs
            if gr['infrastructure_account'] == infrastructure_account
            and 'spilo_cluster' in gr.keys()]


def filter_instances(infrastructure_account, instances):
    return [i for i in instances if i['infrastructure_account'] == infrastructure_account]


def collect_launch_configurations(infrastructure_account):
    asg = boto3.client('autoscaling')
    lc_paginator = asg.get_paginator('describe_launch_configurations')
    lcs = lc_paginator.paginate().build_full_result()['LaunchConfigurations']

    user_data = {}

    for lc in lcs:
        # LaunchConfigurationName takes the form of spilo-your-cluster-AppServerInstanceProfile-66CCXX77EEPP
        lc_name = '-'.join(lc['LaunchConfigurationName'].split('-')[1:-2])
        user_data[lc_name] = lc['UserData']

    return user_data


def extract_eipalloc_from_lc(launch_configuration, cluster_name):
    import yaml
    import base64

    lc = launch_configuration.get(cluster_name, '')

    user_data = base64.decodebytes(lc.encode('utf-8')).decode('utf-8')
    user_data = yaml.safe_load(user_data)

    return user_data['environment'].get('EIP_ALLOCATION', '')


def get_postgresql_clusters(region, infrastructure_account, asgs, insts):
    entities = []

    addresses = collect_eip_addresses(infrastructure_account)
    spilo_asgs = filter_asgs(infrastructure_account, asgs)
    instances = filter_instances(infrastructure_account, insts)
    launch_configs = []

    # we will use the ASGs as a skeleton for building the entities
    for cluster in spilo_asgs:
        cluster_name = cluster['spilo_cluster']

        cluster_instances = []
        eip = []
        eip_allocation = []

        for i in cluster['instances']:
            instance_id = i['aws_id']

            try:
                i_data = [inst for inst in instances if inst['aws_id'] == instance_id][0]
            except IndexError:
                raise Exception(str(cluster_instances))
            private_ip = i_data['ip']
            role = i_data['role']

            cluster_instances.append({'instance_id': instance_id,
                                      'private_ip': private_ip,
                                      'role': role})

            address = [a for a in addresses if a.get('InstanceId') == instance_id]
            if address:
                eip.append(address[0])  # we currently expect only one EIP per instance

        if len(eip) > 1:
            pass  # in the future, this might be a valid case, when replicas also get public IPs
        elif not eip:
            # in this case we have to look at the cluster definition, to see if there was an EIP assigned,
            # but for some reason currently is not.

            # this is so for reducing boto3 call numbers
            if not launch_configs:
                launch_configs = collect_launch_configurations(infrastructure_account)

            eip_allocation = extract_eipalloc_from_lc(launch_configs, cluster_name)

            public_ip_instance_id = ''
            if eip_allocation:
                address = [a for a in addresses if a.get('AllocationId') == eip_allocation]
                if address:
                    public_ip = address[0]['PublicIp']
                    allocation_error = 'There is a public IP defined but not attached to any instance'
        else:
            public_ip = eip[0]['PublicIp']
            public_ip_instance_id = eip[0]['InstanceId']
            allocation_error = ''

        entities.append({'type': 'postgresql_cluster',
                         'id': entity_id('pg-{}[{}:{}]'.format(cluster_name, infrastructure_account, region)),
                         'region': region,
                         'spilo_cluster': cluster_name,
                         'elastic_ip': public_ip,
                         'elastic_ip_instance_id': public_ip_instance_id,
                         'allocation_error': allocation_error,
                         'instances': cluster_instances,
                         'infrastructure_account': infrastructure_account})

    return entities
