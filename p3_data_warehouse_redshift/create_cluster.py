from botocore.exceptions import ClientError
import boto3
import configparser
import json
import pandas as pd
from time import sleep



def create_role(iam, role_name):
    print("Creating IAM role...")
    try:
        role = iam.create_role(
            Path='/',
            RoleName=role_name,
            Description = 'Allows Redshift clusters to call AWS services on your behalf.',
            AssumeRolePolicyDocument=json.dumps(
                {
                    'Statement': [
                        {
                            'Action': 'sts:AssumeRole',
                            'Effect': 'Allow',
                            'Principal': {
                                'Service': 'redshift.amazonaws.com'
                            }
                        }
                    ],
                    'Version': '2012-10-17'
                }
            )
        )
        return role
    except ClientError as e:
        print(e)


def attach_role_policy(iam, role_name):
    print("Attaching policy...")
    return iam.attach_role_policy(
        RoleName=role_name,
        PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'
    )['ResponseMetadata']['HTTPStatusCode']


def create_cluster(redshift, roles, config_dict):
    try:
        response = redshift.create_cluster(
            ClusterType=config_dict['DWH']['cluster_type'],
            NodeType=config_dict['DWH']['node_type'],
            NumberOfNodes=int(config_dict['DWH']['num_nodes']),
            DBName=config_dict['DWH']['db'],
            ClusterIdentifier=config_dict['DWH']['cluster_identifier'],
            MasterUsername=config_dict['DWH']['db_user'],
            MasterUserPassword=config_dict['DWH']['db_password'],
            IamRoles=roles
        )
    except ClientError as e:
        print(e)
    props = redshift.describe_clusters(ClusterIdentifier=config_dict['DWH']['cluster_identifier'])['Clusters'][0]
    print('Waiting for cluster {} to be created...\n(this can take a few mins)'.format(config_dict['DWH']['cluster_identifier']))
    is_created = False
    while not is_created:
        sleep(1)
        props = redshift.describe_clusters(ClusterIdentifier=config_dict['DWH']['cluster_identifier'])['Clusters'][0]
        is_created = props['ClusterStatus'] == 'available'
    print('Cluster {} created.'.format(config_dict['DWH']['cluster_identifier']))
    return props


def prettify_redshift_props(props):
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ['ClusterIdentifier', 'NodeType', 'ClusterStatus', 'MasterUsername', 'DBName', 'Endpoint', 'NumberOfNodes', 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=['key', 'value'])


def authorize_ingress(ec2, props, port):
    '''
    Update cluster security group to allow access through Redshift port
    '''
    print("Authorizing Ingres...")
    try:
        vpc = ec2.Vpc(id=props['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        defaultSg.authorize_ingress(
            GroupName='default',
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(port),
            ToPort=int(port)
        )
    except ClientError as e:
        print(e)


def main():
    print('Parsing config file...')
    config = configparser.ConfigParser()
    config.read('dwh.cfg')  # Note: this transforms keys in your config file to lowercase

    # Creating dictionary from config object to make it easier to work with
    config_dict = {sect: dict(config.items(sect)) for sect in config.sections()}

    # Create variables from subset of dictionary
    AWS_KEY                = config_dict['AWS']['key']
    AWS_SECRET             = config_dict['AWS']['secret']

    DWH_CLUSTER_TYPE       = config_dict['DWH']['cluster_type']
    DWH_NUM_NODES          = int(config_dict['DWH']['num_nodes'])
    DWH_NODE_TYPE          = config_dict['DWH']['node_type']
    DWH_CLUSTER_IDENTIFIER = config_dict['DWH']['cluster_identifier']
    DWH_DB                 = config_dict['DWH']['db']
    DWH_DB_USER            = config_dict['DWH']['db_user']
    DWH_DB_PASSWORD        = config_dict['DWH']['db_password']
    DWH_PORT               = int(config_dict['DWH']['port'])

    DWH_IAM_ROLE_NAME      = config_dict['DWH']['iam_role_name']

    # Print a summary of key-values that will be used to create cluster
    df = pd.DataFrame({
        'Param':
            ['DWH_CLUSTER_TYPE', 'DWH_NUM_NODES', 'DWH_NODE_TYPE', \
            'DWH_CLUSTER_IDENTIFIER', 'DWH_DB', 'DWH_DB_USER', \
            'DWH_DB_PASSWORD', 'DWH_PORT', 'DWH_IAM_ROLE_NAME'],
        'Value':
            [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, \
            DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, \
            DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME]
    })
    print(df)

    # Initialize AWS Clients
    print("Initializing AWS clients...")
    ec2 = boto3.resource(
        'ec2',
        region_name='us-west-2',
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET
    )

    s3 = boto3.resource(
        's3',
        region_name='us-west-2',
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET
    )

    iam = boto3.client(
        'iam',
        region_name='us-west-2',
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET
    )

    redshift = boto3.client(
        'redshift',
        region_name='us-west-2',
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET
    )

    # Create cluster
    role = create_role(iam, DWH_IAM_ROLE_NAME)
    attach_role_policy(iam, DWH_IAM_ROLE_NAME)
    role_arn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
    redshift_props = create_cluster(redshift, [role_arn], config_dict)
    if redshift_props:
        print(prettify_redshift_props(redshift_props))
        DWH_ENDPOINT = redshift_props['Endpoint']['Address']
        DWH_ROLE_ARN = redshift_props['IamRoles'][0]['IamRoleArn']
        print('DWH_ENDPOINT :: ', DWH_ENDPOINT)
        config.set('CLUSTER', 'HOST', str(DWH_ENDPOINT))
        print("--> config['CLUSTER']['HOST'] updated with new endpoint")
        print('DWH_ROLE_ARN :: ', DWH_ROLE_ARN)
        config.set('IAM_ROLE', 'ARN', DWH_ROLE_ARN)
        print("--> config['IAM_ROLE']['ARN'] updated with new ARN")
        with open('dwh.cfg', 'w') as configfile:
            config.write(configfile)
        print("--> config file 'dwh.cfg' updated with new endpoint and ARN")
    authorize_ingress(ec2, redshift_props, DWH_PORT)


if __name__ == "__main__":
    main()
