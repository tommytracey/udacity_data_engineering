import json
from time import sleep

from botocore.exceptions import ClientError
import boto3
import pandas as pd

from data_warehouse.config import CONFIG


def create_role(iam, role_name):
    try:
        role = iam.create_role(
            Path='/',
            RoleName=role_name,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
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
    return iam.attach_role_policy(
        RoleName=role_name,
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
    )['ResponseMetadata']['HTTPStatusCode']


def create_cluster(redshift, roles, config):
    try:
        response = redshift.create_cluster(
            ClusterType=config["DWH"]["CLUSTER_TYPE"],
            NodeType=config["DWH"]["NODE_TYPE"],
            NumberOfNodes=int(config["DWH"]["NUM_NODES"]),
            DBName=config["DWH"]["DB_NAME"],
            ClusterIdentifier=config["DWH"]["CLUSTER_IDENTIFIER"],
            MasterUsername=config["DWH"]["DB_USER"],
            MasterUserPassword=config["DWH"]["DB_PASSWORD"],
            IamRoles=roles
        )
    except ClientError as e:
        print(e)
    cluster_identifier = config["DWH"]["CLUSTER_IDENTIFIER"]
    props = redshift.describe_clusters(ClusterIdentifier=cluster_identifier)["Clusters"][0]
    print("Waiting for cluster {} to be created...".format(cluster_identifier))
    is_created = False
    while not is_created:
        sleep(1)
        props = redshift.describe_clusters(ClusterIdentifier=cluster_identifier)["Clusters"][0]
        is_created = props["ClusterStatus"] == "available"
    print("Cluster {} created.".format(cluster_identifier))
    return props


def prettify_redshift_props(props):
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["key", "value"])


def authorize_ingress(ec2, props, port):
    try:
        vpc = ec2.Vpc(id=props['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(port),
            ToPort=int(port)
        )
    except ClientError as e:
        print(e)


def delete_cluster(redshift, cluster_identifier):
    redshift.delete_cluster(
        ClusterIdentifier=cluster_identifier,
        SkipFinalClusterSnapshot=True
    )


def delete_role(iam, role_name):
    iam.detach_role_policy(
        RoleName=role_name,
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
    )
    iam.delete_role(
        RoleName=role_name
    )


def main():

    ec2 = boto3.resource(
        'ec2',
        region_name="us-west-2",
        aws_access_key_id=CONFIG["AWS"]["KEY"],
        aws_secret_access_key=CONFIG["AWS"]["SECRET"]
    )

    s3 = boto3.resource(
        's3',
        region_name="us-west-2",
        aws_access_key_id=CONFIG["AWS"]["KEY"],
        aws_secret_access_key=CONFIG["AWS"]["SECRET"]
    )

    iam = boto3.client(
        'iam',
        region_name='us-west-2',
        aws_access_key_id=CONFIG["AWS"]["KEY"],
        aws_secret_access_key=CONFIG["AWS"]["SECRET"]
    )

    redshift = boto3.client(
        'redshift',
        region_name="us-west-2",
        aws_access_key_id=CONFIG["AWS"]["KEY"],
        aws_secret_access_key=CONFIG["AWS"]["SECRET"]
    )

    iam_role_name = CONFIG["DWH"]["IAM_ROLE_NAME"]
    role = create_role(iam, iam_role_name)
    attach_role_policy(iam, iam_role_name)
    role_arn = iam.get_role(RoleName=iam_role_name)['Role']['Arn']
    redshift_props = create_cluster(redshift, [role_arn], CONFIG)
    if redshift_props:
        print(prettify_redshift_props(redshift_props))
        DWH_ENDPOINT = redshift_props['Endpoint']['Address']
        DWH_ROLE_ARN = redshift_props['IamRoles'][0]['IamRoleArn']
        print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
        print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)
    authorize_ingress(ec2, redshift_props, CONFIG["DWH"]["DB_PORT"])
