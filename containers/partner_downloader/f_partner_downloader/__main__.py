from f_partner_downloader.partners.fever import download_fever
from f_partner_downloader.partners.housing_anywhere import download_housing_anywhere
from f_partner_downloader.logger import logger


def main():
    logger.info("Starting partner_downloader...")
    download_fever()
    download_housing_anywhere()
    logger.info("Finished partner_downloader")
