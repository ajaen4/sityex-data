import requests

from files.content_writer import ContentWriter, ContentType
from files.file_paths import FilePaths

import f_partner_downloader.config as cfg
from f_partner_downloader.logger import logger
from f_partner_downloader.clients import s3_client


def download_uniplaces():
    logger.info("Downloading Uniplaces data...")

    listings = fetch_data(cfg.UNIPLACES_URL)

    uniplaces_feed_paths = FilePaths(
        bucket_name=cfg.DATA_BUCKET_NAME,
        local_prefix="./outputs/partners/uniplaces/",
        s3_prefix=f"bronze/partners/uniplaces/{cfg.FORMATTED_DATE}/",
        file_name="listings.xml",
    )

    content_writer = ContentWriter(s3_client)
    content_writer.write_content(
        listings, uniplaces_feed_paths, content_type=ContentType.XML
    )

    logger.info(
        f"Data written to {uniplaces_feed_paths.s3_prefix}{uniplaces_feed_paths.file_name}"
    )
    logger.info("Finished downloading Uniplaces data")


def fetch_data(url: str):
    response = requests.get(url)
    if response.status_code == 200:
        return response.content
    else:
        raise Exception(f"Failed to download XML. Status code: {response.status_code}")
