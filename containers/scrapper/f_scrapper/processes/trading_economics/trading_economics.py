import boto3

from internal_lib.aws.s3 import S3
from internal_lib.files.content_writer import ContentWriter

from f_scrapper.scraper_extractor import ScraperExtractor
from internal_lib.logger import logger
from f_scrapper.processes.run_scrape import run_scrape

from .scrape_cfg import TradingEconScrapeConfigs


def trading_econ_process():
    logger.info("Starting trading economics process...")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    trading_econ_extractor = ScraperExtractor(
        base_url="https://tradingeconomics.com/", headers=headers
    )
    s3_client = S3(boto3.Session())
    content_writer = ContentWriter(s3_client)

    country_income_cfg = TradingEconScrapeConfigs.COUNTRY_INCOME_TAX
    run_scrape(content_writer, trading_econ_extractor, country_income_cfg)

    social_security_cfg = TradingEconScrapeConfigs.COUNTRY_SOCIAL_SECURITY_RATE
    run_scrape(content_writer, trading_econ_extractor, social_security_cfg)

    sales_tax_cfg = TradingEconScrapeConfigs.COUNTRY_SALES_TAX_RATE
    run_scrape(content_writer, trading_econ_extractor, sales_tax_cfg)

    job_vacancies_cfg = TradingEconScrapeConfigs.JOB_VACANCIES
    run_scrape(content_writer, trading_econ_extractor, job_vacancies_cfg)

    logger.info("Finished trading economics process")
