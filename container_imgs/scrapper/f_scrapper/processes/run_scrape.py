from copy import deepcopy

from internal_lib.files.content_writer import ContentWriter
from internal_lib.logger import logger

from f_scrapper.scraper_extractor import ScraperExtractor
from f_scrapper.processes.scrape_schemas import (
    ScrapeConfig,
)


def run_scrape(
    content_writer: ContentWriter,
    scraper_extractor: ScraperExtractor,
    scrape_cfg: ScrapeConfig,
    extra_args: dict = {},
):
    logger.info(f"Starting file generation process for {scrape_cfg.name}...")

    content = scraper_extractor.get_content(scrape_cfg, extra_args)

    file_paths = deepcopy(scrape_cfg.file_paths)
    if extra_args:
        file_paths.file_name = file_paths.file_name.format(**extra_args)

    content_writer.write_content(content, file_paths)

    logger.info(f"Finished file generation process for {scrape_cfg.name}")

    return content
