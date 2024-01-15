import json

import boto3
from google.oauth2 import service_account

from aws_lib.s3 import S3
from aws_lib.ssm import SSM
from gcp_lib.gdrive import GDrive

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

session = boto3.Session()
ssm_client = SSM(session)
s3_client = S3(session)

json_creds = json.loads(
    ssm_client.get_parameter("/containers_gcp_service_acc", decrypt=True)
)
service_acc_creds = service_account.Credentials.from_service_account_info(
    json_creds, scopes=SCOPES
)
g_drive_client = GDrive(service_acc_creds)
