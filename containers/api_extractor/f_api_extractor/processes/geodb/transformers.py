from typing import Union

from parser.geo_codes import get_country_3_code


def transform_country_code(results: list[dict[str, Union[str, list]]]):
    results_fmt = list()
    for result in results:
        result_fmt = {
            "country_2_code": result["code"],
            "country_3_code": get_country_3_code(
                alpha_2_code=result["code"],
            ),
            "country_name": result["name"],
            "currency_codes": result["currencyCodes"],
            "wiki_data_id": result["wikiDataId"],
        }
        results_fmt.append(result_fmt)

    puerto_rico_code = "PR"
    puerto_rico = {
        "country_2_code": puerto_rico_code,
        "country_3_code": get_country_3_code(
            alpha_2_code=puerto_rico_code,
        ),
        "country_name": "Puerto Rico",
        "currency_codes": "['USD']",
        "wiki_data_id": "Q1183",
    }
    results_fmt.append(puerto_rico)

    return results_fmt
