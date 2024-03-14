import boto3
from internal_lib.aws.ssm import SSM

session = boto3.Session()
ecr_client = boto3.client("ecr")
ssm_client = SSM(session)
