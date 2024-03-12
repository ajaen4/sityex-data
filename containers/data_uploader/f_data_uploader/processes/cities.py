from copy import deepcopy

from internal_lib.aws.s3 import S3
from parser.strings import remove_diacritics

from f_data_uploader.logger import logger
import f_data_uploader.config as cfg
from f_data_uploader.services import fire_client, s3_client
from .common import get_file_path


def upload_cities_to_firestore():
    cities_file_dir = f"silver/cities/geographical/all_cities/{cfg.FORMATTED_DATE}/"
    cities_file_path = get_file_path(s3_client, cities_file_dir)
    cities_info = s3_client.read_dics(cfg.DATA_BUCKET_NAME, cities_file_path)

    cities_to_show = "maps/cities_to_show.csv"
    cities_to_show = s3_client.read_dics(cfg.DATA_BUCKET_NAME, cities_to_show)
    cities_to_show = [remove_diacritics(row["city_name"]) for row in cities_to_show]

    cities_community_links = "maps/community_links.csv"
    cities_community_links = s3_client.read_dics(
        cfg.DATA_BUCKET_NAME, cities_community_links
    )
    cities_ids_community_links = dict()
    for city_community_link in cities_community_links:
        city_id = city_community_link.pop("city_id")
        cities_ids_community_links[city_id] = city_community_link

    collection_ref = fire_client.collection("cities")
    cities_index = list()
    for index, doc in enumerate(cities_info):
        if (
            doc["country_3_code"] != "ESP"
            or remove_diacritics(doc["name"]) not in cities_to_show
        ):
            continue

        logger.info(f'Uploading doc number {index}, city_name: {doc["name"]}...')

        city_doc = create_city_doc(
            s3_client,
            doc,
        )
        city_doc["community_links"] = cities_ids_community_links.get(
            city_doc["city_id"]
        )
        collection_ref.document(city_doc["city_id"]).set(city_doc)

        logger.info("Finished upload")

        cities_index.append(create_index_entry(city_doc))

    collection_ref.document("_index").set({"cities": cities_index})


def create_city_doc(s3_client: S3, doc: dict):
    city_doc = dict()

    city_doc["coordinates"] = {
        "latitude": float(doc["latitude"]),
        "longitude": float(doc["longitude"]),
    }
    city_id = doc["geonameid"]
    city_doc["city_id"] = city_id
    city_doc["population"] = int(doc["population"])
    city_doc["name"] = doc["name"]
    city_doc["country_3_code"] = doc["country_3_code"]
    city_doc["country_2_code"] = doc["country_2_code"]
    city_doc["continent_name"] = doc["continent_name"]

    if doc["division_geonameid"]:
        city_doc["division_geonameid"] = int(doc["division_geonameid"])

    city_prices_direct = f'silver/cities/costs/country_3_code={doc["country_3_code"]}/city_id={city_doc["city_id"]}/processed_date={cfg.FORMATTED_DATE}/'
    prices_file_path = get_file_path(
        s3_client,
        city_prices_direct,
    )
    if prices_file_path:
        city_prices = s3_client.read_dics(cfg.DATA_BUCKET_NAME, prices_file_path)
        city_doc["prices"] = dict()
        for city_price in city_prices:
            city_doc["prices"][city_price["cost_id"]] = city_price["cost"]

    city_weather_direct = f'silver/cities/weather/city_id={city_doc["city_id"]}/processed_date={cfg.FORMATTED_DATE}/'
    weather_file_path = get_file_path(
        s3_client,
        city_weather_direct,
    )

    weather_infos = s3_client.read_dics(cfg.DATA_BUCKET_NAME, weather_file_path)
    city_doc["weather"] = dict()
    for weather_info in weather_infos:
        weather_info_no_month = deepcopy(weather_info)
        weather_info_no_month.pop("month")
        city_doc["weather"][weather_info["month"]] = weather_info_no_month

    return city_doc


def create_index_entry(doc: dict):
    index_entry = dict()
    index_entry["country_2_code"] = doc["country_2_code"]
    index_entry["name"] = doc["name"]
    index_entry["coordinates"] = doc["coordinates"]
    index_entry["city_id"] = doc["city_id"]

    return index_entry


def upload_cities_cost_map():
    cost_map_path = (
        f"bronze/cities/costs_ids_index/{cfg.FORMATTED_DATE}/costs_ids_index.csv"
    )
    cost_map = s3_client.read_dics(cfg.DATA_BUCKET_NAME, cost_map_path)

    cost_map_fmt = dict()

    for cost_map_item in cost_map:
        cost_map_fmt[cost_map_item["id"]] = cost_map_item
        cost_map_fmt[cost_map_item["id"]].pop("id")

    collection_ref = fire_client.collection("maps")
    logger.info("Uploading cities cost map...")

    collection_ref.document("cities_cost_map").set(cost_map_fmt)

    logger.info("Finished upload")
