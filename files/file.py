import csv
import json
from io import StringIO
from ordered_set import OrderedSet


def write(path, content: list[list[str]]):
    with open(path, "w", newline="") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerows(content)


def write_dics(path, content: list[dict[str, str]]):
    unique_keys = OrderedSet()
    for row in content:
        unique_keys.update(row.keys())

    with open(path, "w") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=unique_keys)
        writer.writeheader()
        writer.writerows(content)


def write_lists(path, content: list[dict[str, str]]):
    with open(path, "w") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerows(content)


def write_json(path, content: list):
    with open(path, "w") as multi_json_file:
        for entry in content:
            json.dump(entry, multi_json_file, indent=2)
            multi_json_file.write("\n")


def format_dics(content: str):
    return list(csv.DictReader(StringIO(content)))
