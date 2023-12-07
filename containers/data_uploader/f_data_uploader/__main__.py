from f_data_uploader.logger import logger
from f_data_uploader.processes.cities import (
    upload_cities_to_firestore,
    upload_cities_cost_map,
)
from f_data_uploader.processes.countries import (
    upload_countries_to_firestore,
    upload_countries_cost_map,
)


def main():
    logger.info("Starting upload to firestore...")
    upload_cities_to_firestore()
    upload_cities_cost_map()
    upload_countries_to_firestore()
    upload_countries_cost_map()
    logger.info("Finished upload to firestore")
