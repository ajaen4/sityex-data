import copy

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

        logger.info(f'Uploading doc number {index}, city_name: {doc["name"]}...')

        housing_data_path = housing_data_paths[0]

        housing_data = s3_client.read_json(cfg.DATA_BUCKET_NAME, housing_data_path)

        city_ref = collection_ref.document(doc["geonameid"])
        upload_housing_city_data(city_ref, housing_data, partner)
        # upload_housing_index(city_ref)


def upload_housing_city_data(
    city_ref: firestore.DocumentReference, housing_data: list[dict], partner: str
):
    housing_ref = city_ref.collection("housing")
    images_ref = city_ref.collection("housing_images")

    housing_ids_db = [
        doc.id
        for doc in housing_ref.where(
            filter=FieldFilter("partner", "==", partner)
        ).stream()
    ]

    housing_ids_images = [
        doc.id
        for doc in images_ref.where(
            filter=FieldFilter("partner", "==", partner)
        ).stream()
    ]

    housing_ids_input = [doc["housing_id"] for doc in housing_data]

    for housing_id in housing_ids_db:
        if housing_id not in housing_ids_input:
            logger.info(f"Deleting housing_id: {housing_id}")
            housing_ref.document(housing_id).delete()

        if housing_id not in housing_ids_images:
            logger.info(f"Deleting images for housing_id: {housing_id}")
            images_ref.document(housing_id).delete()

    for doc in housing_data:
        if "images" not in doc or len(doc["images"]) == 0:
            continue

        new_doc = format_coordinates(doc)
        images_doc = {
            "housing_id": doc["housing_id"],
            "partner": doc["partner"],
            "images": doc["images"],
        }

        if doc["housing_id"] in housing_ids_db:
            housing_ref.document(doc["housing_id"]).set(new_doc, merge=True)
        else:
            housing_ref.document(doc["housing_id"]).set(new_doc)

        if doc["housing_id"] in housing_ids_images:
            images_ref.document(doc["housing_id"]).set(images_doc, merge=True)
        else:
            images_ref.document(doc["housing_id"]).set(images_doc)


def format_coordinates(doc: dict):
    new_doc = copy.deepcopy(doc)
    new_doc["location"]["coordinates"]["latitude"] = float(
        doc["location"]["coordinates"]["latitude"]
    )
    new_doc["location"]["coordinates"]["longitude"] = float(
        doc["location"]["coordinates"]["longitude"]
    )
    new_doc.pop("images")
    new_doc.pop("localizedLinks")

    return new_doc


def upload_housing_index(city_ref: firestore.DocumentReference):
    housing_ref = city_ref.collection("housing")
    city_housing_index = dict()
    index = list()
    for doc in housing_ref.stream():
        index_entry = dict()
        entry = doc.to_dict()

        if doc.id == "_index" or "images" not in entry or len(entry["images"]) == 0:
            continue

        housing_id = entry["housing_id"]

        index_entry["housing_id"] = housing_id
        index_entry["partner"] = entry["partner"]
        index_entry["coordinates"] = entry["location"]["coordinates"]
        index_entry["costs"] = entry["costs"]
        index_entry["rank"] = entry["rank"]

        index.append(index_entry)

    city_housing_index["index"] = index

    city_ref.collection("housing_index").document("index").set(city_housing_index)
