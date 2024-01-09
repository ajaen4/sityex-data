from datetime import datetime

from f_partner_uploader.logger import logger
from f_partner_uploader.processes.events import upload_events


def main():
    logger.info("Starting upload to firestore...")

    now = datetime.now()
    TODAY_DATE = now.strftime("%d-%m-%Y")
    FEVER_DATA_DIR = f"silver/partners/fever/catalog/{TODAY_DATE}/"
    upload_events(partner="fever", events_data_dir=FEVER_DATA_DIR)

    SITYEX_DATA_DIR = "maps/sityex_events.csv"
    upload_events(partner="sityex", events_data_dir=SITYEX_DATA_DIR)

    logger.info("Finished upload to firestore")
