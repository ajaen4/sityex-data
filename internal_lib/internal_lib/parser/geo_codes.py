from pycountry import countries


def get_country_3_code(
    country_name: str = "",
    alpha_2_code: str = "",
    extra_country_codes: dict[str, str] = {},
) -> str:
    country_code = ""
    if country_name:
        country_code = get_country_code_from_name(country_name=country_name)

    if not country_code and alpha_2_code:
        country_code = get_country_code_from_code(alpha_2_code=alpha_2_code)

    if not country_code and country_name and extra_country_codes:
        country_code = get_country_code_from_extra_code(
            country_name=country_name,
            extra_country_codes=extra_country_codes,
        )

    if not country_code:
        raise Exception(
            f"Country not found, country_name: {country_name}, alpha_2_code: {alpha_2_code}"
        )

    return country_code


def get_country_code_from_name(
    country_name: str = "",
) -> str:
    country = countries.get(name=country_name.title())
    if country:
        return country.alpha_3 if country else ""

    try:
        return countries.lookup(country_name).alpha_3
    except LookupError:
        return ""


def get_country_code_from_code(
    alpha_2_code: str = "",
) -> str:
    country = countries.get(alpha_2=alpha_2_code.upper())

    return country.alpha_3 if country else ""


def get_country_code_from_extra_code(
    country_name: str, extra_country_codes: dict[str, str]
) -> str:
    return extra_country_codes[country_name]
