from itertools import chain
import re
import boto3
from typing import Any

from internal_lib.aws.s3 import S3
from internal_lib.parser.number import is_number
from internal_lib.parser.strings import (
    remove_parentheses_in,
    transform_to_db_name,
)
from internal_lib.parser.geo_codes import get_country_3_code

import f_scrapper.config as cfg


def get_rows_country_ranking(rows) -> list[dict[str, Any]]:
    data = []

    extra_codes_map = fetch_extra_codes()
    for row in rows:
        cols = row.find_all("td")
        cols = [
            float(ele.text.strip())
            if is_number(ele.text.strip())
            else ele.text.strip()
            for ele in cols
        ]
        country_name = remove_parentheses_in(cols[1])
        row_fmt = {
            "country_name": country_name,
            "country_code": get_country_3_code(
                country_name, extra_country_codes=extra_codes_map
            ),
            "cost_of_living_index": cols[2],
            "rent_index": cols[3],
            "cost_of_living_plus_rent_index": cols[4],
            "groceries_index": cols[5],
            "restaurant_price_index": cols[6],
            "local_purchasing_power_index": cols[7],
        }
        data.append(row_fmt)

    return data


def get_rows_city_ranking(rows) -> list[dict[str, Any]]:
    data = []
    extra_codes_map = fetch_extra_codes()

    for row in rows:
        cols = row.find_all("td")
        cols = [
            float(ele.text)
            if is_number(ele.text)
            else ele.text.strip('"').strip()
            for ele in cols
        ]

        country_city = [
            remove_parentheses_in(col).strip() for col in cols[1].split(",")
        ]
        if len(country_city) == 2:
            country_name = country_city[1]
        else:
            country_name = country_city[2]

        row_fmt = {
            "city_name": country_city[0],
            "country_name": country_name,
            "country_code": get_country_3_code(
                country_name, extra_country_codes=extra_codes_map
            ),
            "cost_of_living_index": cols[2],
            "rent_index": cols[3],
            "cost_of_living_plus_rent_index": cols[4],
            "groceries_index": cols[5],
            "restaurant_price_index": cols[6],
            "local_purchasing_power_index": cols[7],
        }
        data.append(row_fmt)

    return data


def get_rows_price_country(rows) -> list[dict[str, Any]]:
    data = []
    extra_codes_map = fetch_extra_codes()

    for row in rows:
        cols = row.find_all("td")
        cols = [
            float(ele.text)
            if is_number(ele.text)
            else ele.text.strip('"').strip()
            for ele in cols
        ]

        country_name = remove_parentheses_in(cols[1])

        row_fmt = {
            "country_name": country_name,
            "country_code": get_country_3_code(
                country_name, extra_country_codes=extra_codes_map
            ),
            "cost": cols[2],
        }
        data.append(row_fmt)

    return data


def get_rows_price_city(rows) -> list[dict[str, Any]]:
    data = []
    extra_codes_map = fetch_extra_codes()

    for row in rows:
        cols = row.find_all("td")
        cols = [
            float(ele.text)
            if is_number(ele.text)
            else ele.text.strip('"').strip()
            for ele in cols
        ]

        country_city = [
            remove_parentheses_in(col).strip() for col in cols[1].split(",")
        ]

        if len(country_city) == 2:
            country_name = country_city[1]
        else:
            country_name = country_city[2]

        row_fmt = {
            "city_name": country_city[0],
            "country_name": country_name,
            "country_code": get_country_3_code(
                country_name, extra_country_codes=extra_codes_map
            ),
            "cost": cols[2],
        }
        data.append(row_fmt)

    return data


def get_rows_price_id(rows) -> list[dict[str, Any]]:
    data = []
    for row in rows:
        text = row.text

        name_fmt = transform_to_db_name(text)

        hierarchy = text.split(":")
        category = hierarchy[0].strip()
        subcategory = hierarchy[1].strip()

        category_fmt = re.sub("[()]", "", category)
        category_fmt = re.sub(" ", "_", category_fmt.lower())

        subcat_matches = re.findall(r"([^\(,]+)|(\(.*?\))", subcategory)
        subcategories_fmt = [
            re.sub("[()]", "", subcat).strip()
            for subcat in chain(*subcat_matches)
            if subcat.strip()
        ]

        row_fmt = {
            "name": name_fmt,
            "id": row.attrs["value"],
            "category": category,
            "subcategories": subcategories_fmt,
        }
        data.append(row_fmt)

    return data


def get_table_content(rows) -> list[dict[str, Any]]:
    data = []
    extra_codes = fetch_extra_codes()

    for row in rows:
        cols = row.find_all("td")
        cols_fmt = list()
        for col in cols:
            if a := col.find("a"):
                cols_fmt.append(a.text.strip())
            else:
                cols_fmt.append(col.text.strip())

        if (
            not cols_fmt
            or "euro" in cols_fmt[0].lower()
            or "micronesia" in cols_fmt[0].lower()
        ):
            continue

        row_fmt = {
            "country_name": cols_fmt[0],
            "country_code": get_country_3_code(
                country_name=cols_fmt[0], extra_country_codes=extra_codes
            ),
            "last": cols_fmt[1],
            "previous": cols_fmt[2],
            "reference": cols_fmt[3],
            "unit": cols_fmt[4],
        }
        data.append(row_fmt)

    return data


def fetch_extra_codes() -> dict[str, str]:
    s3_client = S3(boto3.Session())

    extra_country_codes = s3_client.read_dics(
        cfg.DATA_BUCKET_NAME, "maps/extra_country_codes.csv"
    )

    extra_codes_map = dict()
    for extra_code in extra_country_codes:
        extra_codes_map[extra_code["country_name"]] = extra_code[
            "country_code"
        ]

    return extra_codes_map
