from typing import Union


def transform_hourly_json_to_table(results: dict[str, dict[str, Union[list, str]]]):
    table = list()
    for index, timestamp in enumerate(results["hourly"]["time"]):
        table.append(
            {
                "timestamp": timestamp,
                "timezone": results["timezone"],
                "cloud_cover_percent": results["hourly"]["cloudcover"][index],
                "relativehumidity_2m_percent": results["hourly"]["relativehumidity_2m"][
                    index
                ],
            }
        )

    return table


def transform_daily_json_to_table(results: dict[str, dict[str, Union[list, str]]]):
    table = list()
    for index, date in enumerate(results["daily"]["time"]):
        table.append(
            {
                "date": date,
                "timezone": results["timezone"],
                "sunrise": results["daily"]["sunrise"][index],
                "sunset": results["daily"]["sunset"][index],
                "mean_temperature_celsius": results["daily"]["temperature_2m_mean"][
                    index
                ],
                "precipitation_hours": results["daily"]["precipitation_hours"][index],
            }
        )

    return table


def transform_air_qual_json_to_table(results: dict[str, dict[str, Union[list, str]]]):
    table = list()
    for index, timestamp in enumerate(results["hourly"]["time"]):
        table.append(
            {
                "timestamp": timestamp,
                "timezone": results["timezone"],
                "european_aqi": results["hourly"]["european_aqi"][index],
            }
        )

    return table
