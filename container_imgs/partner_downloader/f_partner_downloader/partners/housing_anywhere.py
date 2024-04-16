import requests
from typing import Any

from internal_lib.files.content_writer import ContentWriter, ContentType
from internal_lib.files.file_paths import FilePaths

import f_partner_downloader.config as cfg
from internal_lib.logger import logger
from f_partner_downloader.clients import s3_client


def download_housing_anywhere() -> None:
    logger.info("Downloading HousingAnywhere data...")

    listings = fetch_data(cfg.HOUSING_ANYWHERE_URL)["listings"]

    housing_feed_paths = FilePaths(
        bucket_name=cfg.DATA_BUCKET_NAME,
        local_prefix="./outputs/partners/housing_anywhere/",
        s3_prefix=f"bronze/partners/housing_anywhere/{cfg.FORMATTED_DATE}/",
        file_name="listings.json",
    )
    content_writer = ContentWriter(s3_client)
    content_writer.write_content(
        listings, housing_feed_paths, content_type=ContentType.JSON
    )

    logger.info(
        f"Data written to {housing_feed_paths.s3_prefix}{housing_feed_paths.file_name}"
    )
    logger.info("Finished downloading HousingAnywhere data")


def fetch_data(url: str) -> Any:
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(
            f"Failed to download JSON. Status code: {response.status_code}"
        )
