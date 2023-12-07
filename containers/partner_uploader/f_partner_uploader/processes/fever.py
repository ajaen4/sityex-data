from firebase_admin import firestore
from datetime import datetime
import polars as pl
from polars import col
import re

from parser.strings import remove_diacritics

from f_partner_uploader.logger import logger
import f_partner_uploader.config as cfg
from f_partner_uploader.services import fire_client, s3_client
from .common import get_file_path


def upload_fever_data():
    cities_file_dir = f"silver/cities/geographical/all_cities/{cfg.FORMATTED_DATE}/"
    cities_file_path = get_file_path(s3_client, cities_file_dir)
    cities_info = s3_client.read_dics(cfg.DATA_BUCKET_NAME, cities_file_path)

    now = datetime.now()
    TODAY_DATE = now.strftime("%d-%m-%Y")
    fever_data_dir = f"silver/partners/fever/catalog/{TODAY_DATE}/"
    fever_data_path = get_file_path(s3_client, fever_data_dir)
    fever_data = s3_client.read_dics(cfg.DATA_BUCKET_NAME, fever_data_path)

    cities_to_show = "maps/cities_to_show.csv"
    cities_to_show = s3_client.read_dics(cfg.DATA_BUCKET_NAME, cities_to_show)
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

        fever_city_data = pl.DataFrame(fever_data).filter(
            col("city_id") == doc["geonameid"]
        )

        logger.info(f'Uploading doc number {index}, city_name: {doc["name"]}...')

        events_ref = collection_ref.document(doc["geonameid"]).collection("events")
        upload_fever_city_data(events_ref, fever_city_data)


def upload_fever_city_data(events_ref: firestore, fever_data=pl.DataFrame):
    skus = [doc.id for doc in events_ref.stream()]

    for sku in skus:
        if sku not in fever_data["sku"]:
            logger.info(f"Deleting sku: {sku}")
            events_ref.document(sku).delete()

    for doc in fever_data.to_dicts():
        doc["coordinates"] = dict()
        doc["coordinates"]["latitude"] = float(doc["latitude"])
        doc["coordinates"]["longitude"] = float(doc["longitude"])
        doc["description"] = add_newlines(doc["description"])

        events_ref.document(doc["sku"]).set(doc)


def add_newlines(text):
    # Regular expression for emojis
    emoji_pattern = re.compile(
        "["
        "\U0001F600-\U0001F64F"  # emoticons
        "\U0001F300-\U0001F5FF"  # symbols & pictographs
        "\U0001F680-\U0001F6FF"  # transport & map symbols
        "\U0001F700-\U0001F77F"  # alchemical symbols
        "\U0001F780-\U0001F7FF"  # Geometric Shapes Extended
        "\U0001F800-\U0001F8FF"  # Supplemental Arrows-C
        "\U0001F900-\U0001F9FF"  # Supplemental Symbols and Pictographs
        "\U0001FA00-\U0001FA6F"  # Chess Symbols
        "\U0001FA70-\U0001FAFF"  # Symbols and Pictographs Extended-A
        "\U00002702-\U000027B0"  # Dingbats
        "\U000024C2-\U0001F251"
        "⏳"
        "]+",
        flags=re.UNICODE,
    )

    phrase_pattern = re.compile(
        r"Que vas a disfrutar|Información( adicional)?:?|Descripción|Intérpretes?:?|Programa Candlelight[a-zA-Z0-9 ]*:|Opiniones.*💬"
    )

    text = phrase_pattern.sub(r"\n\n\g<0>\n", text)
    text = emoji_pattern.sub(r"\n\g<0>", text)

    return text
