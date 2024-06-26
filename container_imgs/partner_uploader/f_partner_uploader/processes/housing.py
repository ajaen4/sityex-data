import copy
from typing import Any

import google.cloud.firestore as firestore
from google.cloud.firestore_v1.base_query import FieldFilter

from internal_lib.logger import logger
import f_partner_uploader.config as cfg
from f_partner_uploader.services import fire_client, s3_client


def upload_housing(partner: str, housing_data_dir: str) -> None:
    cities_file_dir = (
        f"silver/cities/geographical/all_cities/{cfg.FORMATTED_DATE}/"
    )
    cities_file_path = s3_client.list_files(
        cfg.DATA_BUCKET_NAME, cities_file_dir, suffix=".csv"
    )[0]
    cities_info = s3_client.read_dics(cfg.DATA_BUCKET_NAME, cities_file_path)

    show_cities_file_dir = "maps/cities_to_show.csv"
    cities_to_show = s3_client.read_dics(
        cfg.DATA_BUCKET_NAME, show_cities_file_dir
    )
    city_ids_to_show = [row["city_id"] for row in cities_to_show]

    collection_ref = fire_client.collection("cities")
    for index, doc in enumerate(cities_info):
        if (
            doc["country_3_code"] != "ESP"
            or doc["geonameid"] not in city_ids_to_show
        ):
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

        logger.info(
            f'Uploading doc number {index}, city_name: {doc["name"]}...'
        )

        housing_data_path = housing_data_paths[0]

        housing_data = s3_client.read_json(
            cfg.DATA_BUCKET_NAME, housing_data_path
        )

        city_ref = collection_ref.document(doc["geonameid"])
        upload_housing_city_data(city_ref, housing_data, partner)


def upload_housing_city_data(
    city_ref: firestore.DocumentReference,
    housing_data: list[dict],
    partner: str,
) -> None:
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

        if housing_id not in housing_ids_input:
            logger.info(f"Deleting images for housing_id: {housing_id}")
            images_ref.document(housing_id).delete()

    for doc in housing_data:
        if "images" not in doc or len(doc["images"]) == 0:
            continue

        new_doc = create_db_doc(doc)

        if doc["housing_id"] in housing_ids_db:
            housing_ref.document(doc["housing_id"]).set(new_doc, merge=True)
        else:
            housing_ref.document(doc["housing_id"]).set(new_doc)

        images_doc = create_images_db_doc(doc)
        if doc["housing_id"] in housing_ids_images:
            images_ref.document(doc["housing_id"]).set(images_doc, merge=True)
        else:
            images_ref.document(doc["housing_id"]).set(images_doc)


def format_coordinates(coordinates: dict) -> dict[str, float]:
    coordinates_fmt = copy.deepcopy(coordinates)
    coordinates_fmt["latitude"] = float(coordinates["latitude"])
    coordinates_fmt["longitude"] = float(coordinates["longitude"])

    return coordinates_fmt


def create_db_doc(doc: dict) -> dict[str, Any]:
    if doc["partner"] == "housing_anywhere":
        return {
            "housing_id": doc["housing_id"],
            "partner": doc["partner"],
            "location": {
                "neighborhood": doc["location"]["neighborhood"],
                "coordinates": format_coordinates(
                    doc["location"]["coordinates"]
                ),
            },
            "costsFormatted": {
                "price": doc["costsFormatted"]["price"],
                "currency": doc["costsFormatted"]["currency"],
            },
            "availability": doc["availability"],
            "description": doc["description"][0:250],
            "facilities": {
                "totalSize": get_field(doc, ["facilities", "totalSize"]),
                "bedrooms": get_field(doc, ["facilities", "bedrooms"]),
                "airConditioning": get_field(
                    doc, ["facilities", "airConditioning"]
                ),
                "balconyTerrace": get_field(
                    doc, ["facilities", "balconyTerrace"]
                ),
                "bathroom": get_field(doc, ["facilities", "bathroom"]),
                "garden": get_field(doc, ["facilities", "garden"]),
                "kitchen": get_field(doc, ["facilities", "kitchen"]),
                "parking": get_field(doc, ["facilities", "parking"]),
                "pets": get_field(doc, ["facilities", "pets"]),
                "wheelchairAccessible": get_field(
                    doc, ["facilities", "wheelchairAccessible"]
                ),
                "basement": get_field(doc, ["facilities", "basement"]),
                "dishwasher": get_field(doc, ["facilities", "dishwasher"]),
                "washingMachine": get_field(
                    doc, ["facilities", "washingMachine"]
                ),
                "dryer": get_field(doc, ["facilities", "dryer"]),
                "heating": get_field(doc, ["facilities", "heating"]),
            },
            "kindLabel": doc["kindLabel"],
            "link": doc["link"],
            "title": doc["title"],
            "typeLabel": doc["typeLabel"],
            "isFurnished": doc["is_furnished"],
            "created_at": doc["published"],
        }

    elif doc["partner"] == "uniplaces":
        return {
            "housing_id": doc["housing_id"],
            "partner": doc["partner"],
            "location": {
                "coordinates": format_coordinates(
                    {
                        "latitude": doc["latitude"],
                        "longitude": doc["longitude"],
                    }
                ),
            },
            "costsFormatted": {
                "price": doc["costsFormatted_price"],
                "currency": doc["costsFormatted_currency"],
            },
            "availability": [{"from": doc["availability"]}],
            "description": doc["description"][0:250],
            "kindLabel": doc["kindLabel"],
            "link": doc["link"],
            "title": doc["title"],
            "isFurnished": doc["is_furnished"],
            "facilities": {
                "bedrooms": doc["rooms"],
            },
            "created_at": doc["created_at"],
        }
    else:
        raise ValueError(f"Partner {doc['partner']} not supported")


def get_field(doc: dict, fields: list[str]) -> Any:
    try:
        for field in fields:
            doc = doc[field]
        return doc
    except KeyError:
        return None


def create_images_db_doc(doc: dict) -> dict:
    if doc["partner"] == "housing_anywhere":
        images = [image["sizes"]["640x480"]["link"] for image in doc["images"]]
        return {
            "housing_id": doc["housing_id"],
            "partner": doc["partner"],
            "images": images,
        }

    elif doc["partner"] == "uniplaces":
        images = doc["images"].split(";")
        return {
            "housing_id": doc["housing_id"],
            "partner": doc["partner"],
            "images": images,
        }

    else:
        raise ValueError(f"Partner {doc['partner']} not supported")
