from google.cloud import firestore

from internal_lib.logger import logger
import f_partner_uploader.config as cfg
from f_partner_uploader.services import fire_client, s3_client


def upload_events(events_data_dir: str):
    events_data_path = s3_client.list_files(
        cfg.DATA_BUCKET_NAME, events_data_dir, suffix=".csv"
    )[0]
    events_data = s3_client.read_dics(cfg.DATA_BUCKET_NAME, events_data_path)

    collection_ref = fire_client.collection("cities")

    MADRID_CITY_ID = "3117735"
    logger.info(f"Uploading city_id {MADRID_CITY_ID}, city_name: Madrid...")

    events_ref = collection_ref.document(MADRID_CITY_ID).collection("events")
    upload_events_city_data(events_ref, events_data)


def upload_events_city_data(
    events_ref: firestore.CollectionReference, events_data: list[dict]
):
    event_ids = [doc.id for doc in events_ref.stream()]

    events_data_ids = [doc["event_id"] for doc in events_data]

    for event_id in event_ids:
        if event_id not in events_data_ids:
            logger.info(f"Deleting event_id: {event_id}")
            events_ref.document(event_id).delete()

    for doc in events_data:
        doc_fmt = format_coordinates(doc)
        if doc_fmt["event_id"] in event_ids:
            events_ref.document(doc_fmt["event_id"]).set(doc_fmt, merge=True)
        else:
            events_ref.document(doc_fmt["event_id"]).set(doc_fmt)


def format_coordinates(event_data: dict):
    event_fmt = event_data.copy()
    event_fmt["latitude"] = float(event_data["latitude"])
    event_fmt["longitude"] = float(event_data["longitude"])

    return event_fmt
