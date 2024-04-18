import boto3

from internal_lib.aws.s3 import S3
from internal_lib.files.content_writer import ContentWriter
from internal_lib.files.file_paths import FilePaths

from internal_lib.api.api_extractor import ApiExtractor, ExtractorArgs


import f_partner_downloader.config as cfg
from internal_lib.logger import logger
from .queries import past_events_query, curr_events_query
from .transformers import (
    extract_curr_events,
    extract_past_events,
)
from .client import MeetupClient


def download_meetup() -> None:
    logger.info("Downloading Meetup data...")

    meetup_client = MeetupClient("api.meetup.com")
    api_extractor = ApiExtractor(meetup_client)
    s3_client = S3(boto3.Session())
    content_writer = ContentWriter(s3_client)

    past_events_args = ExtractorArgs(
        api_path="/gql",
        params={
            "query": past_events_query,
            "variables": {"urlname": cfg.MEETUP_GROUP_URL_NAME},
        },
        process_output_func=extract_past_events,
    )

    curr_events_args = ExtractorArgs(
        api_path="/gql",
        params={
            "query": curr_events_query,
            "variables": {"urlname": cfg.MEETUP_GROUP_URL_NAME},
        },
        process_output_func=extract_curr_events,
    )

    past_events_data = api_extractor.extract_content(past_events_args)
    curr_events_data = api_extractor.extract_content(curr_events_args)
    data = curr_events_data + past_events_data

    file_paths = FilePaths(
        bucket_name=cfg.DATA_BUCKET_NAME,
        local_prefix="./outputs/partners/meetup/",
        s3_prefix=f"bronze/partners/meetup/{cfg.FORMATTED_DATE}/",
        file_name="events_catalog.csv",
    )
    content_writer = ContentWriter(s3_client)
    content_writer.write_content(data, file_paths)

    logger.info(
        f"Data written to {file_paths.s3_prefix}{file_paths.file_name}"
    )
    logger.info("Finished downloading Meetup data")
