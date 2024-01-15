import json

import boto3
from google.oauth2 import service_account
from googleapiclient.discovery import build

from aws_lib.ssm import SSM

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

ssm_client = SSM(boto3.Session())
json_creds = json.loads(
    ssm_client.get_parameter("/containers_gcp_service_acc", decrypt=True)
)
service_acc_creds = service_account.Credentials.from_service_account_info(
    json_creds, scopes=SCOPES
)

g_drive_service = build("sheets", "v4", credentials=service_acc_creds)
