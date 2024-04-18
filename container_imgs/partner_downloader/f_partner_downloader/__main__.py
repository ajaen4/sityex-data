from f_partner_downloader.partners import (
    download_housing_anywhere,
    download_uniplaces,
    download_meetup,
)
from internal_lib.logger import logger


def main():
    logger.info("Starting partner_downloader...")
    download_housing_anywhere()
    download_uniplaces()
    download_meetup()
    logger.info("Finished partner_downloader")
