import copy

import polars as pl
from polars import col
from google.cloud import firestore
from google.cloud.firestore_v1.base_query import FieldFilter

from f_partner_uploader.logger import logger
import f_partner_uploader.config as cfg
from f_partner_uploader.services import fire_client, s3_client


def upload_housing(partner: str, housing_data_dir: str):
    cities_file_dir = f"silver/cities/geographical/all_cities/{cfg.FORMATTED_DATE}/"
    cities_file_path = s3_client.list_files(
        cfg.DATA_BUCKET_NAME, cities_file_dir, suffix=".csv"
    )[0]
    cities_info = s3_client.read_dics(cfg.DATA_BUCKET_NAME, cities_file_path)

    cities_to_show = "maps/cities_to_show.csv"
    cities_to_show = s3_client.read_dics(cfg.DATA_BUCKET_NAME, cities_to_show)
    city_ids_to_show = [row["city_id"] for row in cities_to_show]

    collection_ref = fire_client.collection("cities")
    for index, doc in enumerate(cities_info):
        if doc["country_3_code"] != "ESP" or doc["geonameid"] not in city_ids_to_show:
            continue

        CITY_HOUSING_DIR = f"{housing_data_dir}city_id={doc['geonameid']}"
        housing_data_paths = s3_client.list_files(
            cfg.DATA_BUCKET_NAME, CITY_HOUSING_DIR, suffix=".json"
        )

        if len(housing_data_paths) == 0:
            logger.info(
                f"No housing data for city_id: {doc['geonameid']}, city_name: {doc['name']}"
            )
            continue

        housing_data_path = housing_data_paths[0]

        housing_data = s3_client.read_json(cfg.DATA_BUCKET_NAME, housing_data_path)

        logger.info(f'Uploading doc number {index}, city_name: {doc["name"]}...')

        housing_ref = collection_ref.document(doc["geonameid"]).collection("housing")
        upload_housing_city_data(housing_ref, housing_data, partner)


def upload_housing_city_data(
    housing_ref: firestore.CollectionReference, housing_data: list[dict], partner: str
):
    housing_ids_db = [
        doc.id
        for doc in housing_ref.where(
            filter=FieldFilter("partner", "==", partner)
        ).stream()
    ]

    housing_ids_input = [doc["housing_id"] for doc in housing_data]

    for housing_id in housing_ids_db:
        if housing_id not in housing_ids_input:
            logger.info(f"Deleting housing_id: {housing_id}")
            housing_ref.document(housing_id).delete()

    for doc in housing_data:
        new_doc = format_coordinates(doc)

        if doc["housing_id"] in housing_ids_db:
            housing_ref.document(doc["housing_id"]).set(new_doc, merge=True)
        else:
            housing_ref.document(doc["housing_id"]).set(new_doc)


def format_coordinates(doc: dict):
    new_doc = copy.deepcopy(doc)
    new_doc["coordinates"] = dict()
    new_doc["location"]["coordinates"]["latitude"] = float(
        doc["location"]["coordinates"]["latitude"]
    )
    new_doc["location"]["coordinates"]["longitude"] = float(
        doc["location"]["coordinates"]["longitude"]
    )

    return new_doc
