from datetime import datetime

from internal_lib.logger import logger
from f_partner_uploader.processes.events import upload_events
from f_partner_uploader.processes.housing import upload_housing


def main() -> None:
    logger.info("Starting partner uploader...")

    now = datetime.now()
    TODAY_DATE = now.strftime("%d-%m-%Y")

    logger.info("Starting Meetup events upload...")

    MEETUP_DATA_DIR = f"silver/partners/meetup/{TODAY_DATE}/"
    upload_events(events_data_dir=MEETUP_DATA_DIR)

    logger.info("Finished Meetup events upload")

    logger.info("Starting HousingAnywhere upload...")

    HOUSING_ANYWHERE_DIR = f"silver/partners/housing_anywhere/{TODAY_DATE}/"
    upload_housing(
        partner="housing_anywhere", housing_data_dir=HOUSING_ANYWHERE_DIR
    )

    logger.info("Finished HousingAnywhere upload")

    logger.info("Starting Uniplaces upload...")

    UNIPLACES_DIR = f"silver/partners/uniplaces/{TODAY_DATE}/"
    upload_housing(partner="uniplaces", housing_data_dir=UNIPLACES_DIR)

    logger.info("Finished Uniplaces upload")

    logger.info("Finished partner uploader")
