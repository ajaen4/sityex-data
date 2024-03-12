from datetime import datetime

from f_partner_uploader.logger import logger
from f_partner_uploader.processes.events import upload_events
from f_partner_uploader.processes.housing import upload_housing


def main():
    logger.info("Starting partner uploader...")

    now = datetime.now()
    TODAY_DATE = now.strftime("%d-%m-%Y")

    logger.info("Starting SityEx events upload...")

    SITYEX_DATA_DIR = "maps/sityex_events.csv"
    upload_events(partner="sityex", events_data_dir=SITYEX_DATA_DIR)

    logger.info("Finished SityEx events upload")

    logger.info("Starting HousingAnywhere events upload...")

    HOUSING_ANYWHERE_DIR = f"silver/partners/housing_anywhere/{TODAY_DATE}/"
    upload_housing(partner="housing_anywhere", housing_data_dir=HOUSING_ANYWHERE_DIR)

    logger.info("Finished HousingAnywhere events upload")

    logger.info("Starting Uniplaces events upload...")

    HOUSING_ANYWHERE_DIR = f"silver/partners/uniplaces/{TODAY_DATE}/"
    upload_housing(partner="uniplaces", housing_data_dir=HOUSING_ANYWHERE_DIR)

    logger.info("Finished Uniplaces events upload")

    logger.info("Finished partner uploader")
