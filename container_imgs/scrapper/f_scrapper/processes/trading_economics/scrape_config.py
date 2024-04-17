from enum import Enum

from internal_lib.files.file_paths import FilePaths

from f_scrapper.processes.scrape_schemas import (
    HTMLElem,
    ScrapeElems,
    ScrapeConfig,
)
from f_scrapper.processes.row_formatters import get_table_content
import f_scrapper.config as cfg


class TradingEconScrapeConfigs(Enum):
    COUNTRY_INCOME_TAX = ScrapeConfig(
        name="COUNTRY_INCOME_TAX",
        url_path="country-list/personal-income-tax-rate",
        file_paths=FilePaths(
            bucket_name=cfg.DATA_BUCKET_NAME,
            file_name="income_tax_rate.csv",
            local_prefix="./outputs/countries/taxes/",
            s3_prefix=f"bronze/countries/taxes/{cfg.FORMATTED_DATE}/",
        ),
        scrape_elems=ScrapeElems(
            row_search_elems=[
                HTMLElem(
                    type="table",
                    property_key="class",
                    property_value="table table-hover table-heatmap",
                ),
                HTMLElem(
                    type="tr",
                    is_find_all=True,
                ),
            ],
            row_extract_func=get_table_content,
        ),
    )

    COUNTRY_SOCIAL_SECURITY_RATE = ScrapeConfig(
        name="COUNTRY_SOCIAL_SECURITY_RATE",
        url_path="country-list/social-security-rate-for-employees",
        file_paths=FilePaths(
            bucket_name=cfg.DATA_BUCKET_NAME,
            file_name="social_security_rate.csv",
            local_prefix="./outputs/countries/taxes/",
            s3_prefix=f"bronze/countries/taxes/{cfg.FORMATTED_DATE}/",
        ),
        scrape_elems=ScrapeElems(
            row_search_elems=[
                HTMLElem(
                    type="table",
                    property_key="class",
                    property_value="table table-hover table-heatmap",
                ),
                HTMLElem(
                    type="tr",
                    is_find_all=True,
                ),
            ],
            row_extract_func=get_table_content,
        ),
    )

    COUNTRY_SALES_TAX_RATE = ScrapeConfig(
        name="COUNTRY_SALES_TAX_RATE",
        url_path="country-list/sales-tax-rate",
        file_paths=FilePaths(
            bucket_name=cfg.DATA_BUCKET_NAME,
            file_name="sales_tax_rate.csv",
            local_prefix="./outputs/countries/taxes/",
            s3_prefix=f"bronze/countries/taxes/{cfg.FORMATTED_DATE}/",
        ),
        scrape_elems=ScrapeElems(
            row_search_elems=[
                HTMLElem(
                    type="table",
                    property_key="class",
                    property_value="table table-hover table-heatmap",
                ),
                HTMLElem(
                    type="tr",
                    is_find_all=True,
                ),
            ],
            row_extract_func=get_table_content,
        ),
    )

    JOB_VACANCIES = ScrapeConfig(
        name="JOB_VACANCIES",
        url_path="country-list/job-vacancies",
        file_paths=FilePaths(
            bucket_name=cfg.DATA_BUCKET_NAME,
            file_name="job_vacancies.csv",
            local_prefix="./outputs/countries/indicators/",
            s3_prefix=f"bronze/countries/indicators/{cfg.FORMATTED_DATE}/",
        ),
        scrape_elems=ScrapeElems(
            row_search_elems=[
                HTMLElem(
                    type="table",
                    property_key="class",
                    property_value="table table-hover table-heatmap",
                ),
                HTMLElem(
                    type="tr",
                    is_find_all=True,
                ),
            ],
            row_extract_func=get_table_content,
        ),
    )
