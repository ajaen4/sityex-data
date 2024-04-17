import requests
from typing import Any

from internal_lib.files.content_writer import ContentWriter, ContentType
from internal_lib.files.file_paths import FilePaths
from internal_lib.files.file import xml_to_csv

import f_partner_downloader.config as cfg
from internal_lib.logger import logger
from f_partner_downloader.clients import s3_client

FIELDS = [
    "id",
    "property_id",
    "url",
    "title",
    "type",
    "content",
    "price",
    "currency_code",
    "rooms",
    "bathrooms",
    "address",
    "postcode",
    "city",
    "country",
    "latitude",
    "longitude",
    "is_furnished",
    "bills_included",
    "billing_cycle",
    "availability",
    "created_at",
    "updated_at",
    "picture_urls",
    "minimum_stay",
    "cancellation_policy",
    "maximum_guests",
]


def download_uniplaces() -> None:
    logger.info("Downloading Uniplaces data...")

    listings = fetch_data(cfg.UNIPLACES_URL)

    uniplaces_feed_paths = FilePaths(
        bucket_name=cfg.DATA_BUCKET_NAME,
        local_prefix="./outputs/partners/uniplaces/",
        s3_prefix=f"bronze/partners/uniplaces/{cfg.FORMATTED_DATE}/",
        file_name="listings.csv",
    )

    csv_content = xml_to_csv(FIELDS, listings)
    content_writer = ContentWriter(s3_client)
    content_writer.write_content(
        csv_content, uniplaces_feed_paths, content_type=ContentType.LIST_LISTS
    )

    logger.info(
        f"Data written to {uniplaces_feed_paths.s3_prefix}{uniplaces_feed_paths.file_name}"
    )
    logger.info("Finished downloading Uniplaces data")


def fetch_data(url: str) -> Any:
    response = requests.get(url)
    if response.status_code == 200:
        return response.content
    else:
        raise Exception(
            f"Failed to download XML. Status code: {response.status_code}"
        )
