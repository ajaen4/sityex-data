import json
import polars as pl
from polars import col
from copy import deepcopy

from internal_lib.logger import logger
import f_data_uploader.config as cfg
from f_data_uploader.services import fire_client, s3_client
from .common import get_file_path


def upload_countries_to_firestore():
    countries_info_path = f"bronze/countries/geographical/countries_details/{cfg.FORMATTED_DATE}/countries_details.csv"
    countries_info = s3_client.read_dics(cfg.DATA_BUCKET_NAME, countries_info_path)

    collection_ref = fire_client.collection("countries")
    for index, doc in enumerate(countries_info):
        logger.info(f'Uploading doc number {index}, country_name: {doc["name"]}...')

        country_doc = create_country_doc(doc)
        collection_ref.document(country_doc["country_3_code"]).set(country_doc)

        logger.info("Finished upload")


def create_country_doc(doc: dict):
    country_doc = dict()

    country_doc["country_id"] = doc["geonameid"]
    country_doc["population"] = int(doc["population"])
    country_doc["name"] = doc["name"]
    country_doc["country_3_code"] = doc["country_3_code"]
    country_doc["country_2_code"] = doc["country_2_code"]
    country_doc["capital_name"] = doc["capital_name"]
    country_doc["capital_geonameid"] = doc["capital_geonameid"]
    country_doc["area_size"] = doc["area_size"]

    languages = json.loads(doc["languages"].replace("'", '"'))
    country_doc["languages"] = languages

    country_doc["total_cities"] = int(doc["total_cities"])
    country_doc["currency_code"] = doc["currency_code"]
    country_doc["timezone_code"] = doc["timezone_code"]
    country_doc["continent_name"] = doc["continent_name"]

    country_prices_dir = f'silver/countries/costs/country_code={country_doc["country_3_code"]}/processed_date={cfg.FORMATTED_DATE}/'
    prices_file_path = get_file_path(
        s3_client,
        country_prices_dir,
    )

    if prices_file_path:
        country_prices = s3_client.read_dics(cfg.DATA_BUCKET_NAME, prices_file_path)
        country_doc["prices"] = dict()
        for country_prices in country_prices:
            country_doc["prices"][country_prices["cost_id"]] = country_prices["cost"]

    country_prices_dir = f'silver/countries/costs/country_code={country_doc["country_3_code"]}/processed_date={cfg.FORMATTED_DATE}/'
    prices_file_path = get_file_path(
        s3_client,
        country_prices_dir,
    )

    if prices_file_path:
        country_prices = s3_client.read_dics(cfg.DATA_BUCKET_NAME, prices_file_path)
        country_doc["prices"] = dict()
        for country_prices in country_prices:
            country_doc["prices"][country_prices["cost_id"]] = country_prices["cost"]

    if indicators := get_indicators(country_doc["country_3_code"]):
        country_doc["indicators"] = dict()
        for indicator in indicators:
            clean_ind = deepcopy(indicator)
            clean_ind.pop("indicator_type")
            country_doc["indicators"][indicator["indicator_type"]] = clean_ind

    if taxes := get_taxes(country_doc["country_3_code"]):
        country_doc["taxes"] = dict()
        for tax in taxes:
            clean_tax = deepcopy(tax)
            clean_tax.pop("tax_type")
            country_doc["taxes"][tax["tax_type"]] = clean_tax

    return country_doc


def get_indicators(country_code: str):
    country_ind_dir = f"silver/countries/indicators/country_code={country_code}/processed_date={cfg.FORMATTED_DATE}/"
    country_ind_path = get_file_path(
        s3_client,
        country_ind_dir,
    )

    if not country_ind_path:
        return {}

    indicators_details = s3_client.read_dics(cfg.DATA_BUCKET_NAME, country_ind_path)

    indicators_fmt = pl.DataFrame(indicators_details).with_columns(
        col("year").cast(pl.Int64).alias("year")
    )
    indicators_fmt = indicators_fmt.sort("year", descending=True)
    indicators_fmt = (
        indicators_fmt.filter(col("year") < 2024)
        .groupby("indicator_type")
        .agg(
            year=col("year").first(),
            value=col("value").first(),
            unit=col("unit").first(),
        )
    )

    return indicators_fmt.rows(named=True)


def get_taxes(
    country_code: str,
):
    country_ind_dir = f"silver/countries/taxes/country_code={country_code}/processed_date={cfg.FORMATTED_DATE}/"
    country_ind_path = get_file_path(
        s3_client,
        country_ind_dir,
    )

    if not country_ind_path:
        return {}

    taxes_details = s3_client.read_dics(cfg.DATA_BUCKET_NAME, country_ind_path)

    taxes_fmt = pl.DataFrame(taxes_details).with_columns(
        col("year").cast(pl.Int64).alias("year")
    )
    taxes_fmt = taxes_fmt.sort("year", descending=True)
    taxes_fmt = (
        taxes_fmt.filter(col("year") < 2024)
        .groupby("tax_type")
        .agg(
            year=col("year").first(),
            value=col("value").first(),
            unit=col("unit").first(),
        )
    )

    return taxes_fmt.rows(named=True)


def upload_countries_cost_map():
    cost_map_path = (
        f"bronze/countries/costs_ids_index/{cfg.FORMATTED_DATE}/costs_ids_index.csv"
    )
    cost_map = s3_client.read_dics(cfg.DATA_BUCKET_NAME, cost_map_path)

    cost_map_fmt = dict()

    for cost_map_item in cost_map:
        cost_map_fmt[cost_map_item["id"]] = cost_map_item
        cost_map_fmt[cost_map_item["id"]].pop("id")

    collection_ref = fire_client.collection("maps")
    logger.info("Uploading countries cost map...")

    collection_ref.document("countries_cost_map").set(cost_map_fmt)

    logger.info("Finished upload")
