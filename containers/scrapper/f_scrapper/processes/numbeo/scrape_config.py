from enum import Enum

from internal_lib.files.file_paths import FilePaths

from f_scrapper.processes.scrape_schemas import (
    HTMLElem,
    ScrapeElems,
    ScrapeConfig,
)
from f_scrapper.processes.row_formatters import (
    get_rows_country_ranking,
    get_rows_city_ranking,
    get_rows_price_id,
    get_rows_price_country,
    get_rows_price_city,
)
import f_scrapper.config as cfg


class NumbeoScrapeConfigs(Enum):
    COST_RANK_COUNTRY = ScrapeConfig(
        url_path="cost-of-living/rankings_by_country.jsp",
        file_paths=FilePaths(
            bucket_name=cfg.DATA_BUCKET_NAME,
            file_name="cost_ranking.csv",
            local_prefix="./outputs/countries/",
            s3_prefix=f"bronze/countries/cost_ranking/{cfg.FORMATTED_DATE}/",
        ),
        scrape_elems=ScrapeElems(
            row_search_elems=[
                HTMLElem(
                    type="table",
                    property_key="id",
                    property_value="t2",
                ),
                HTMLElem(
                    type="tbody",
                ),
                HTMLElem(
                    type="tr",
                    is_find_all=True,
                ),
            ],
            row_extract_func=get_rows_country_ranking,
        ),
    )

    COST_RANK_CITIES = ScrapeConfig(
        url_path="cost-of-living/rankings.jsp",
        file_paths=FilePaths(
            bucket_name=cfg.DATA_BUCKET_NAME,
            file_name="cost_ranking.csv",
            local_prefix="./outputs/cities/",
            s3_prefix=f"bronze/cities/cost_ranking/{cfg.FORMATTED_DATE}/",
        ),
        scrape_elems=ScrapeElems(
            row_search_elems=[
                HTMLElem(
                    type="table",
                    property_key="id",
                    property_value="t2",
                ),
                HTMLElem(
                    type="tbody",
                ),
                HTMLElem(
                    type="tr",
                    is_find_all=True,
                ),
            ],
            row_extract_func=get_rows_city_ranking,
        ),
    )

    COUNTRY_COSTS_IDS_INDEX = ScrapeConfig(
        url_path="cost-of-living/prices_by_country.jsp",
        file_paths=FilePaths(
            bucket_name=cfg.DATA_BUCKET_NAME,
            file_name="costs_ids_index.csv",
            local_prefix="./outputs/countries/",
            s3_prefix=f"bronze/countries/costs_ids_index/{cfg.FORMATTED_DATE}/",
        ),
        scrape_elems=ScrapeElems(
            row_search_elems=[
                HTMLElem(
                    type="table",
                    property_key="class",
                    property_value="table_for_selecting_items",
                ),
                HTMLElem(
                    type="tr",
                    is_find_all=True,
                    elem_position=1,
                ),
                HTMLElem(
                    type="option",
                    is_find_all=True,
                ),
            ],
            row_extract_func=get_rows_price_id,
        ),
    )

    COUNTRY_COSTS = ScrapeConfig(
        url_path="cost-of-living/prices_by_country.jsp?itemId={id}&displayCurrency=USD",
        file_paths=FilePaths(
            bucket_name=cfg.DATA_BUCKET_NAME,
            file_name="{id}.csv",
            local_prefix="./outputs/countries/costs_by_id/",
            s3_prefix=f"bronze/countries/costs_by_id/{cfg.FORMATTED_DATE}/",
        ),
        scrape_elems=ScrapeElems(
            row_search_elems=[
                HTMLElem(
                    type="table",
                    property_key="id",
                    property_value="t2",
                ),
                HTMLElem(
                    type="tbody",
                ),
                HTMLElem(
                    type="tr",
                    is_find_all=True,
                ),
            ],
            row_extract_func=get_rows_price_country,
        ),
    )

    CITY_COSTS_IDS_INDEX = ScrapeConfig(
        url_path="cost-of-living/prices_by_city.jsp",
        file_paths=FilePaths(
            bucket_name=cfg.DATA_BUCKET_NAME,
            file_name="costs_ids_index.csv",
            local_prefix="./outputs/cities/",
            s3_prefix=f"bronze/cities/costs_ids_index/{cfg.FORMATTED_DATE}/",
        ),
        scrape_elems=ScrapeElems(
            row_search_elems=[
                HTMLElem(
                    type="table",
                    property_key="class",
                    property_value="table_for_selecting_items",
                ),
                HTMLElem(
                    type="tr",
                    is_find_all=True,
                    elem_position=1,
                ),
                HTMLElem(
                    type="option",
                    is_find_all=True,
                ),
            ],
            row_extract_func=get_rows_price_id,
        ),
    )

    CITY_COSTS = ScrapeConfig(
        url_path="cost-of-living/prices_by_city.jsp?itemId={id}&displayCurrency=USD",
        file_paths=FilePaths(
            bucket_name=cfg.DATA_BUCKET_NAME,
            file_name="{id}.csv",
            local_prefix="./outputs/cities/costs_by_id/",
            s3_prefix=f"bronze/cities/costs_by_id/{cfg.FORMATTED_DATE}/",
        ),
        scrape_elems=ScrapeElems(
            row_search_elems=[
                HTMLElem(
                    type="table",
                    property_key="id",
                    property_value="t2",
                ),
                HTMLElem(
                    type="tbody",
                ),
                HTMLElem(
                    type="tr",
                    is_find_all=True,
                ),
            ],
            row_extract_func=get_rows_price_city,
        ),
    )
