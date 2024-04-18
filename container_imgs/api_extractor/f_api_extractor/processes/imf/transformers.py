def dict_to_list_dict(results: dict[str, dict[str, str]]) -> list[dict]:
    return [
        dict(country_code=k, values_per_year=v) for k, v in results.items()
    ]
