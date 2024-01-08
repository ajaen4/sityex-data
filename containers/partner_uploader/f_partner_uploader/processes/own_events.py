from firebase_admin import firestore
import polars as pl
from polars import col

from parser.strings import remove_diacritics

from f_partner_uploader.logger import logger
import f_partner_uploader.config as cfg
from f_partner_uploader.services import fire_client, s3_client

from .common import get_file_path


def upload_own_events():
    cities_file_dir = f"silver/cities/geographical/all_cities/{cfg.FORMATTED_DATE}/"
    cities_file_path = get_file_path(s3_client, cities_file_dir)
    cities_info = s3_client.read_dics(cfg.DATA_BUCKET_NAME, cities_file_path)

    OWN_EVENTS_DATA_PATH = "maps/sityex_events.csv"
    own_events = s3_client.read_dics(cfg.DATA_BUCKET_NAME, OWN_EVENTS_DATA_PATH)

    CITIES_TO_SHOW_DATA_PATH = "maps/cities_to_show.csv"
    cities_to_show = s3_client.read_dics(cfg.DATA_BUCKET_NAME, CITIES_TO_SHOW_DATA_PATH)
    cities_to_show = [
        remove_diacritics(row["cities_to_show"]) for row in cities_to_show
    ]

    collection_ref = fire_client.collection("cities")
    for index, doc in enumerate(cities_info):
        if (
            doc["country_3_code"] != "ESP"
            or remove_diacritics(doc["name"]) not in cities_to_show
        ):
            continue

        own_events_city_data = pl.DataFrame(own_events).filter(
            col("city_id") == doc["geonameid"]
        )

        logger.info(f'Uploading doc number {index}, city_name: {doc["name"]}...')

        events_ref = collection_ref.document(doc["geonameid"]).collection("events")
        upload_own_event(events_ref, own_events_city_data)


def upload_own_event(events_ref: firestore, own_events_city_data=pl.DataFrame):
    event_ids = [doc.id for doc in events_ref.stream()]

    for event_id in event_ids:
        if event_id not in own_events_city_data["event_id"]:
            logger.info(f"Deleting event_id: {event_id}")
            events_ref.document(event_id).delete()

    for doc in own_events_city_data.to_dicts():
        doc["coordinates"] = dict()
        doc["coordinates"]["latitude"] = float(doc["latitude"])
        doc["coordinates"]["longitude"] = float(doc["longitude"])

        events_ref.document(doc["event_id"]).set(doc)
