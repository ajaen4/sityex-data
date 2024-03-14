import json

import boto3
from google.oauth2 import service_account

from internal_lib.aws.s3 import S3
from internal_lib.aws.ssm import SSM
from internal_lib.gcp.gdrive import GDrive

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
