# mypy: ignore-errors

import csv
import json
from io import StringIO
from ordered_set import OrderedSet
import xml.etree.ElementTree as ET
from typing import Any


def write(path, content: list[list[str]]) -> None:
    with open(path, "w", newline="") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerows(content)


def write_dics(path, content: list[dict[str, str]]) -> None:
    unique_keys: OrderedSet = OrderedSet()
    for row in content:
        unique_keys.update(row.keys())

    with open(path, "w") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=unique_keys)
        writer.writeheader()
        writer.writerows(content)


def write_lists(path, content: list[dict[str, str]]) -> None:
    with open(path, "w") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerows(content)


def write_json(path, content: list) -> None:
    with open(path, "w") as multi_json_file:
        json.dump(content, multi_json_file, indent=2)


def write_xml(path: str, content: str) -> None:
    root = ET.fromstring(content)
    tree = ET.ElementTree(root)
    tree.write(path, encoding="utf-8", xml_declaration=True)


def format_dics(content: str) -> list[dict[str, Any]]:
    return list(csv.DictReader(StringIO(content)))


def xml_to_csv(fields: list[str], content: str) -> list[list]:
    root = ET.fromstring(content)
    rows = []
    rows.append(fields)

    for ad in root.findall("ad"):
        row = []
        for field in fields[:-4]:
            element = ad.find(field)
            if element is not None and element.text is not None:
                row.append(element.text.strip())
            else:
                row.append("")

        pictures = ad.find("pictures")
        if pictures is not None:
            picture_urls = ";".join(
                [
                    pic.find("picture_url").text.strip()
                    for pic in pictures.findall("picture")
                ]
            )
        else:
            picture_urls = ""
        row.append(picture_urls)

        requisites = ad.find("requisites")
        if requisites is not None:
            conditions = requisites.find("conditions")
            if conditions is not None:
                minimum_stay = conditions.find("minimum_stay")
                if minimum_stay is not None:
                    row.append(minimum_stay.text.strip())
                else:
                    row.append("")

                cancellation_policy = conditions.find("cancellation_policy")
                if cancellation_policy is not None:
                    row.append(cancellation_policy.text.strip())
                else:
                    row.append("")

                maximum_guests = conditions.find("maximum_guests")
                if maximum_guests is not None:
                    row.append(maximum_guests.text.strip())
                else:
                    row.append("")

        rows.append(row)

    return rows
