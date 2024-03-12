import json

import boto3
from google.cloud import firestore

from internal_lib.aws.s3 import S3
from internal_lib.aws.ssm import SSM

session = boto3.Session()
s3_client = S3(session)

ssm_client = SSM(session)
json_creds_dev = json.loads(
    ssm_client.get_parameter("/firebase_admin/dev", decrypt=True)
)
json_creds_prod = json.loads(
    ssm_client.get_parameter("/firebase_admin/prod", decrypt=True)
)
fire_client_dev = firestore.Client.from_service_account_info(json_creds_dev)
fire_client_prod = firestore.Client.from_service_account_info(json_creds_prod)

blogs_dev_ref = fire_client_dev.collection("blogs")
blogs_prod_ref = fire_client_prod.collection("blogs")

blogs_dev = [blog_dev.to_dict() for blog_dev in blogs_dev_ref.stream()]
blogs_prod = [blog_prod.to_dict() for blog_prod in blogs_prod_ref.stream()]

for blog_dev in blogs_dev:
    print(f"Uploading document ID: {blog_dev['id']}")
    blogs_prod_ref.document(blog_dev["id"]).set(blog_dev)

blogs_id_dev = [blog["id"] for blog in blogs_dev]
blogs_id_prod = [blog["id"] for blog in blogs_prod]


for blog_id_prod in blogs_id_prod:
    if blog_id_prod not in blogs_id_dev:
        print(f"Deleting document ID: {blog_id_prod}")
        blogs_prod_ref.document(blog_id_prod).delete()
