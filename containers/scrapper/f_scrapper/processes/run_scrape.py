from enum import Enum
from copy import deepcopy

from files.content_writer import ContentWriter

from f_scrapper.scraper_extractor import ScraperExtractor
from f_scrapper.logger import logger


def run_scrape(
    content_writer: ContentWriter,
    scraper_extractor: ScraperExtractor,
    scrape_config: Enum,
    extra_args: dict = None,
):
    logger.info(f"Starting file generation process for {scrape_config.name}...")

    content = scraper_extractor.get_content(scrape_config.value, extra_args)

    file_paths = deepcopy(scrape_config.value.file_paths)
    if extra_args:
        file_paths.file_name = file_paths.file_name.format(**extra_args)

    content_writer.write_content(content, file_paths)

    logger.info(f"Finished file generation process for {scrape_config.name}")

    return content
