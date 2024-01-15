from files.content_writer import ContentWriter
from files.file_paths import FilePaths

import f_partner_downloader.config as cfg
from f_partner_downloader.logger import logger
from f_partner_downloader.clients import g_drive_client, s3_client


def download_fever():
    logger.info("Downloading Fever data...")

    data = g_drive_client.fetch_gsheets_data(cfg.FEVER_GDRIVE_FILE_ID)
    if not data:
        raise Exception("No data fetched from Google Sheets.")

    fever_feed_paths = FilePaths(
        bucket_name=cfg.DATA_BUCKET_NAME,
        local_prefix="./outputs/partners/fever/",
        s3_prefix=f"bronze/partners/fever/{cfg.FORMATTED_DATE}/",
        file_name="events_catalog.csv",
    )
    content_writer = ContentWriter(s3_client)
    content_writer.write_content(data, fever_feed_paths)

    logger.info(
        f"Data written to {fever_feed_paths.s3_prefix}{fever_feed_paths.file_name}"
    )
    logger.info("Finished downloading Fever data")
