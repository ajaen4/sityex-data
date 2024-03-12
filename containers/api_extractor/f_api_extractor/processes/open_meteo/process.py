import boto3

from internal_lib.aws.s3 import S3
from internal_lib.files.file_paths import FilePaths

from internal_lib.api.api_extractor import ApiExtractor, ExtractorArgs
from f_api_extractor.clients import OpenMeteoClient
import f_api_extractor.config as cfg
from internal_lib.logger import logger
from internal_lib.files.content_writer import ContentWriter
from .transformers import (
    transform_daily_json_to_table,
    transform_hourly_json_to_table,
    transform_air_qual_json_to_table,
)


def run_open_meteo_workload():
    hist_open_meteo = OpenMeteoClient("customer-archive-api.open-meteo.com")
    hist_open_meteo_ext = ApiExtractor(hist_open_meteo)

    api_params = {
        "start_date": cfg.METRICS_TIME_WINDOW_START,
        "end_date": cfg.METRICS_TIME_WINDOW_END,
        "hourly": cfg.WEATHER_HOURLY_METRICS,
        "daily": cfg.WEATHER_DAILY_METRICS,
        "timezone": "auto",
    }

    hourly_args = ExtractorArgs(
        api_path="/v1/archive",
        params=api_params,
        process_output_func=transform_hourly_json_to_table,
        cache_content=True,
        use_cached_content=False,
    )

    daily_args = ExtractorArgs(
        api_path="/v1/archive",
        process_output_func=transform_daily_json_to_table,
        cache_content=False,
        use_cached_content=True,
    )

    logger.info("Starting open-meteo hourly workload...")
    run_hist_workload(hist_open_meteo_ext, hourly_args, daily_args)
    logger.info("Finished open-meteo hourly workload")

    air_qual_meteo = OpenMeteoClient("customer-air-quality-api.open-meteo.com")
    air_qual_meteo_ext = ApiExtractor(air_qual_meteo)

    api_params = {
        "start_date": cfg.AIR_QUAL_WINDOW_START,
        "end_date": cfg.AIR_QUAL_WINDOW_END,
        "hourly": cfg.AIR_QUAL_HOURLY_METRICS,
        "timezone": "auto",
    }

    air_qual_args = ExtractorArgs(
        api_path="/v1/air-quality",
        params=api_params,
        process_output_func=transform_air_qual_json_to_table,
    )

    logger.info("Starting open-meteo air quality workload...")
    run_air_workload(air_qual_meteo_ext, air_qual_args)
    logger.info("Finished open-meteo air quality workload")


def run_hist_workload(
    extractor: ApiExtractor,
    hourly_extractor_args: ExtractorArgs,
    daily_extractor_args: ExtractorArgs,
):
    s3_client = S3(boto3.Session())
    content_writer = ContentWriter(s3_client)

    all_cities_path = get_all_cities_path(s3_client)

    for city in s3_client.read_dics(cfg.DATA_BUCKET_NAME, all_cities_path):
        daily_file_paths = get_file_paths("daily_weather", city["geonameid"])
        hourly_file_paths = get_file_paths("hourly_weather", city["geonameid"])

        daily_has_results = path_has_results(s3_client, daily_file_paths)
        hourly_has_results = path_has_results(s3_client, hourly_file_paths)

        one_has_results = daily_has_results or hourly_has_results
        both_have_results = daily_has_results and hourly_has_results

        if both_have_results:
            logger.info(f"Skipping {city['name']}, already has results")
            continue

        if one_has_results and not both_have_results:
            raise Exception("Unfinished workload detected.")

        hourly_extractor_args.params["latitude"] = city["latitude"]
        hourly_extractor_args.params["longitude"] = city["longitude"]

        hourly_content = extractor.extract_content(hourly_extractor_args)
        content_writer.write_content(hourly_content, hourly_file_paths)

        daily_content = extractor.extract_content(daily_extractor_args)
        content_writer.write_content(daily_content, daily_file_paths)


def run_air_workload(
    extractor: ApiExtractor,
    air_quality_ext_args: ExtractorArgs,
):
    s3_client = S3(boto3.Session())
    content_writer = ContentWriter(s3_client)

    all_cities_path = get_all_cities_path(s3_client)

    for city in s3_client.read_dics(cfg.DATA_BUCKET_NAME, all_cities_path):
        air_qual_file_paths = get_file_paths("air_quality", city["geonameid"])

        air_qual_has_results = path_has_results(s3_client, air_qual_file_paths)

        if air_qual_has_results:
            logger.info(f"Skipping {city['name']}, already has results")
            continue

        air_quality_ext_args.params["latitude"] = city["latitude"]
        air_quality_ext_args.params["longitude"] = city["longitude"]

        air_qual_content = extractor.extract_content(air_quality_ext_args)
        content_writer.write_content(air_qual_content, air_qual_file_paths)


def get_file_paths(file_name: str, city_id: str):
    return FilePaths(
        bucket_name=cfg.DATA_BUCKET_NAME,
        local_prefix="./outputs/weather/",
        s3_prefix=f"bronze/cities/weather/{city_id}/{cfg.FORMATTED_DATE}/",
        file_name=f"{file_name}.csv",
    )


def get_all_cities_path(s3_client: S3):
    all_cities_paths = s3_client.list_files(
        cfg.DATA_BUCKET_NAME,
        f"silver/cities/geographical/all_cities/{cfg.FORMATTED_DATE}/",
    )
    return [
        all_cities_path
        for all_cities_path in all_cities_paths
        if all_cities_path.endswith(".csv")
    ][0]


def path_has_results(s3_client: S3, file_paths: FilePaths):
    path = f"{file_paths.s3_prefix}/{file_paths.file_name}"
    results_paths = s3_client.list_files(cfg.DATA_BUCKET_NAME, path)

    return any(results_paths)
