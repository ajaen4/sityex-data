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

        housing_data_path = s3_client.list_files(
            cfg.DATA_BUCKET_NAME, housing_data_dir, suffix=".json"
        )[0]
        housing_data = s3_client.read_json(cfg.DATA_BUCKET_NAME, housing_data_path)

        logger.info(f'Uploading doc number {index}, city_name: {doc["name"]}...')

        housing_ref = collection_ref.document(doc["geonameid"]).collection("housing")
        upload_housing_city_data(housing_ref, housing_data, partner)


def upload_housing_city_data(
    housing_ref: firestore.CollectionReference, housing_data: pl.DataFrame, partner: str
):
    event_ids = [
        doc.id
        for doc in housing_ref.where(
            filter=FieldFilter("partner", "==", partner)
        ).stream()
    ]

    for event_id in event_ids:
        if event_id not in housing_data["event_id"]:
            logger.info(f"Deleting event_id: {event_id}")
            housing_ref.document(event_id).delete()

    for doc in housing_data.to_dicts():
        new_doc = format_coordinates(doc)

        if doc["event_id"] in event_ids:
            housing_ref.document(doc["event_id"]).set(new_doc, merge=True)
        else:
            housing_ref.document(doc["event_id"]).set(new_doc)


def format_coordinates(doc: dict):
    new_doc = copy.deepcopy(doc)
    new_doc["coordinates"] = dict()
    new_doc["coordinates"]["latitude"] = float(doc["latitude"])
    new_doc["coordinates"]["longitude"] = float(doc["longitude"])

    new_doc.pop("latitude")
    new_doc.pop("longitude")

    return new_doc
