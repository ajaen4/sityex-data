import boto3

from internal_lib.aws.s3 import S3
from internal_lib.files.content_writer import ContentWriter

from f_scrapper.scraper_extractor import ScraperExtractor
from f_scrapper.logger import logger
from f_scrapper.processes.run_scrape import run_scrape
from .scrape_cfg import NumbeoScrapeConfigs


def numbeo_process():
    logger.info("Starting numbeo process...")

    numbeo_extractor = ScraperExtractor(base_url="https://www.numbeo.com/")
    s3_client = S3(boto3.Session())
    content_writer = ContentWriter(s3_client)

    cost_r_country_cfg = NumbeoScrapeConfigs.COST_RANK_COUNTRY
    run_scrape(content_writer, numbeo_extractor, cost_r_country_cfg)

    cost_r_cities_cfg = NumbeoScrapeConfigs.COST_RANK_CITIES
    run_scrape(content_writer, numbeo_extractor, cost_r_cities_cfg)

    country_costs_index_cfg = NumbeoScrapeConfigs.COUNTRY_COSTS_IDS_INDEX
    country_costs_ids_index = run_scrape(
        content_writer, numbeo_extractor, country_costs_index_cfg
    )

    process_costs_per_id(
        numbeo_extractor,
        content_writer,
        NumbeoScrapeConfigs.COUNTRY_COSTS,
        country_costs_ids_index,
    )

    cities_cost_index_cfg = NumbeoScrapeConfigs.CITY_COSTS_IDS_INDEX
    cities_costs_ids_index = run_scrape(
        content_writer, numbeo_extractor, cities_cost_index_cfg
    )

    process_costs_per_id(
        numbeo_extractor,
        content_writer,
        NumbeoScrapeConfigs.CITY_COSTS,
        cities_costs_ids_index,
    )

    logger.info("Finished numbeo process")


def process_costs_per_id(
    numbeo_extractor: ScraperExtractor,
    content_writer: ContentWriter,
    scrape_cfg: NumbeoScrapeConfigs,
    costs_ids_index: list[dict[str, str]],
):
    for price_index in costs_ids_index:
        extra_args = {"id": price_index["id"]}
        run_scrape(content_writer, numbeo_extractor, scrape_cfg, extra_args)
