import boto3

from internal_lib.aws.s3 import S3
from internal_lib.files.content_writer import ContentWriter
from internal_lib.files.file_paths import FilePaths

from internal_lib.api.api_extractor import ApiExtractor, ExtractorArgs


import f_partner_downloader.config as cfg
from internal_lib.logger import logger
from .queries import get_group_query
from .transformers import extract_past_events
from .client import MeetupClient


def download_meetup():
    logger.info("Downloading Meetup data...")

    meetup_client = MeetupClient("api.meetup.com")
    api_extractor = ApiExtractor(meetup_client)
    s3_client = S3(boto3.Session())
    content_writer = ContentWriter(s3_client)

    extractor_args = ExtractorArgs(
        api_path="/gql",
        params={
            "query": get_group_query,
            "variables": {"urlname": cfg.MEETUP_GROUP_URL_NAME},
        },
        process_output_func=extract_past_events,
    )

    data = api_extractor.extract_content(extractor_args)

    file_paths = FilePaths(
        bucket_name=cfg.DATA_BUCKET_NAME,
        local_prefix="./outputs/partners/meetup/",
        s3_prefix=f"bronze/partners/meetup/{cfg.FORMATTED_DATE}/",
        file_name="events_catalog.csv",
    )
    content_writer = ContentWriter(s3_client)
    content_writer.write_content(data, file_paths)

    logger.info(f"Data written to {file_paths.s3_prefix}{file_paths.file_name}")
    logger.info("Finished downloading Meetup data")
