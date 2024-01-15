import boto3

from files.content_writer import ContentWriter
from files.file_paths import FilePaths
from aws_lib.s3 import S3

from f_partner_downloader.services import g_drive_service
import f_partner_downloader.config as cfg
from f_partner_downloader.logger import logger


def fetch_gsheets_data(spread_sheet_id):
    try:
        result = (
            g_drive_service.spreadsheets()
            .values()
            .get(spreadsheetId=spread_sheet_id, range="Feed")
            .execute()
        )
        values = result.get("values", [])
        return values
    except Exception as e:
        print(f"Error fetching data from Google Sheets: {str(e)}")
        return []


def main():
    logger.info("Starting partner_downloader...")

    s3_client = S3(boto3.Session())
    content_writer = ContentWriter(s3_client)

    data = fetch_gsheets_data(cfg.FEVER_GDRIVE_FILE_ID)
    if not data:
        logger.info("No data fetched from Google Sheets.")
        return

    fever_feed_paths = FilePaths(
        bucket_name=cfg.DATA_BUCKET_NAME,
        local_prefix="./outputs/partners/fever/",
        s3_prefix=f"bronze/partners/fever/{cfg.FORMATTED_DATE}/",
        file_name="events_catalog.csv",
    )
    content_writer.write_content(data, fever_feed_paths)

    logger.info(
        f"Data written to {fever_feed_paths.s3_prefix}{fever_feed_paths.file_name}"
    )
    logger.info("Finished partner_downloader")
