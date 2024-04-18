from internal_lib.parser.geo_codes import get_country_3_code


def format_country(results: list[dict[str, str]]) -> list[dict[str, str]]:
    results_fmt = list()
    keys_not_flatten = ["territories", "languages"]

    for result in results:
        flattened_result = flatten_dict(result, keys_not_flatten)

        flattened_result.pop("status")

        country_2_code = flattened_result["code"]
        flattened_result.pop("code")
        flattened_result["country_2_code"] = country_2_code
        flattened_result["country_3_code"] = get_country_3_code(
            flattened_result["name"], alpha_2_code=country_2_code
        )
        results_fmt.append(flattened_result)

    return results_fmt


def format_city(results: list[dict[str, str]]) -> list[dict[str, str]]:
    results_fmt = list()

    for result in results:
        flattened_result = flatten_dict(result)

        country_2_code = flattened_result["country_code"]
        flattened_result.pop("country_code")
        flattened_result["country_2_code"] = country_2_code
        flattened_result["country_3_code"] = get_country_3_code(
            alpha_2_code=country_2_code
        )
        results_fmt.append(flattened_result)

    return results_fmt


def flatten_dict(
    result: dict,
    keys_not_flatten: list = list(),
    parent_key: str = "",
    sep: str = "_",
) -> dict:
    items = {}
    for k, v in result.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k

        if isinstance(v, dict) and k not in keys_not_flatten:
            items.update(flatten_dict(v, keys_not_flatten, new_key, sep=sep))
        else:
            items[new_key] = v
    return items
