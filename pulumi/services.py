import boto3

session = boto3.Session()
ecr_client = boto3.client("ecr")
