from f_partner_uploader.logger import logger
from f_partner_uploader.processes.fever import upload_fever_data
from f_partner_uploader.processes.own_events import upload_own_events


def main():
    logger.info("Starting upload to firestore...")
    upload_fever_data()
    upload_own_events()
    logger.info("Finished upload to firestore")
