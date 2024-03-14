import json

import boto3
from google.cloud import firestore

from internal_lib.aws.s3 import S3
from internal_lib.aws.ssm import SSM

import f_partner_uploader.config as cfg

session = boto3.Session()
s3_client = S3(session)

ssm_client = SSM(session)
json_creds = json.loads(
    ssm_client.get_parameter(f"/firebase_admin/{cfg.ENV}", decrypt=True)
)
fire_client = firestore.Client.from_service_account_info(json_creds)
