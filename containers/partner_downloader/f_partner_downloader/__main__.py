from f_partner_downloader.partners import (
    download_fever,
    download_housing_anywhere,
    download_uniplaces,
)
from f_partner_downloader.logger import logger


def main():
    logger.info("Starting partner_downloader...")
    download_fever()
    download_housing_anywhere()
    download_uniplaces()
    logger.info("Finished partner_downloader")
