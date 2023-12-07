from f_partner_uploader.logger import logger
from f_partner_uploader.processes.fever import upload_fever_data


def main():
    logger.info("Starting upload to firestore...")
    upload_fever_data()
    logger.info("Finished upload to firestore")
