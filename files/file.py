import csv
import json
from io import StringIO
from ordered_set import OrderedSet
import xml.etree.ElementTree as ET


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
        json.dump(content, multi_json_file, indent=2)


def write_xml(path: str, content: str):
    root = ET.fromstring(content)
    tree = ET.ElementTree(root)
    tree.write(path, encoding="utf-8", xml_declaration=True)


def format_dics(content: str):
    return list(csv.DictReader(StringIO(content)))
