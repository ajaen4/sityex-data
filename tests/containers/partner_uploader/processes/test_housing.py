import json
import os

from helpers.files import (
    get_file_names,
)


class TestHousing:
    DIRECTORY_PATH = os.path.dirname(os.path.realpath(__file__))
    INPUT_PATH = f"{DIRECTORY_PATH}/data/input"
    OUTPUT_PATH = f"{DIRECTORY_PATH}/data/expected"

    def test_create_db_doc(self):
        from f_partner_uploader.processes.housing import (
            create_db_doc,
        )

        for file_name in get_file_names(self.INPUT_PATH):
            with open(f"{self.INPUT_PATH}/{file_name}", "r") as file:
                input = json.load(file)

            with open(f"{self.OUTPUT_PATH}/{file_name}", "r") as file:
                expected_output = json.load(file)

            assert create_db_doc(input) == expected_output
