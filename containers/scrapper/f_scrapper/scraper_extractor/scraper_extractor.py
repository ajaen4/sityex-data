from enum import Enum

from f_scrapper.logger import logger

from .scraper_client import ScraperClient


class ScraperExtractor:
    def __init__(
        self,
        base_url: str,
        headers: dict[str, str] = None,
    ):
        self.scrape_client = ScraperClient(base_url, headers)

    def get_content(self, scrape_cfg: Enum, extra_args: dict[str, str] = None):
        if extra_args:
            logger.info(f"Extra args: {extra_args}")

        logger.info("Starting scrapping...")

        url_path = ScraperExtractor.get_url_path(scrape_cfg.url_path, extra_args)
        scraped_content = self.scrape_client.scrape_content(scrape_cfg, url_path)

        logger.info("Finished scrapping")

        return scraped_content

    @staticmethod
    def get_file_name(file_name: str, extra_args: dict[str, str] = None):
        if extra_args:
            return file_name.format(**extra_args)
        else:
            return file_name

    @staticmethod
    def get_url_path(url_path: str, extra_args: dict[str, str] = None):
        if extra_args:
            return url_path.format(**extra_args)
        else:
            return url_path
