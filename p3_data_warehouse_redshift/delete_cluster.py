from botocore.exceptions import ClientError
import boto3
import configparser
import json


def delete_cluster(redshift, cluster_identifier):
    print("Deleting cluster...")
    try:
        redshift.delete_cluster(
            ClusterIdentifier=cluster_identifier,
            SkipFinalClusterSnapshot=True
        )
    except ClientError as e:
        print(e)

def delete_role(iam, role_name):
    print("Deleting IAM role...")
    try:
        iam.detach_role_policy(
            RoleName=role_name,
            PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'
        )
        iam.delete_role(
            RoleName=role_name
        )
    except ClientError as e:
        print(e)


def main():
    print('Parsing config file...')
    config = configparser.ConfigParser()
    config.read('dwh.cfg')  # Note: this transforms keys in your config file to lowercase

    AWS_KEY                = config.get('AWS','key')
    AWS_SECRET             = config.get('AWS','secret')

    DWH_CLUSTER_IDENTIFIER = config.get('DWH','cluster_identifier')
    DWH_IAM_ROLE_NAME      = config.get('DWH','iam_role_name')

    # Initialize clients
    print('Initializing clients...')

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

    delete_cluster(redshift, DWH_CLUSTER_IDENTIFIER)
    delete_role(iam, DWH_IAM_ROLE_NAME)
    print("Cluster and IAM role deleted")


if __name__ == "__main__":
    main()
