import boto3

from internal_lib.aws.s3 import S3
from internal_lib.files.content_writer import ContentWriter
from internal_lib.files.file_paths import FilePaths

from internal_lib.api.api_extractor import ApiExtractor, ExtractorArgs
from f_api_extractor.clients import CountCitClient
import f_api_extractor.config as cfg
from internal_lib.logger import logger

from .transformers import format_country, format_city


def run_count_cities_workload():
    count_cit_client = CountCitClient(
        host="countries-cities.p.rapidapi.com",
    )
    count_cit_ext = ApiExtractor(count_cit_client)
    s3_client = S3(boto3.Session())
    content_writer = ContentWriter(s3_client)

    countries_info = s3_client.read_dics(
        cfg.DATA_BUCKET_NAME,
        f"bronze/countries/geographical/countries_list/{cfg.FORMATTED_DATE}/countries_list.csv",
    )

    countries_file_paths = FilePaths(
        bucket_name=cfg.DATA_BUCKET_NAME,
        local_prefix="./outputs/countries_cities/countries_details/",
        s3_prefix=f"bronze/countries/geographical/countries_details/{cfg.FORMATTED_DATE}/",
        file_name="countries_details.csv",
    )

    api_paths = [
        f'/location/country/{country["country_2_code"]}' for country in countries_info
    ]

    extractor_args = ExtractorArgs(
        api_paths=api_paths,
        process_output_func=format_country,
    )

    countries_info = count_cit_ext.extract_contents(
        extractor_args,
    )
    content_writer.write_content(countries_info, countries_file_paths)

    params = {
        "per_page": "20",
        "population": cfg.MIN_POPULATION,
    }

    for country in countries_info:
        cities_list_path = FilePaths(
            bucket_name=cfg.DATA_BUCKET_NAME,
            local_prefix="./outputs/countries_cities/cities_details/",
            s3_prefix=f"bronze/cities/geographical/cities_details/{cfg.FORMATTED_DATE}/",
            file_name=f'{country["country_2_code"]}.csv',
        )

        cities_list_extractor_args = ExtractorArgs(
            api_path=f'/location/country/{country["country_2_code"]}/city/list',
            params=params,
            process_output_func=format_city,
            data_key="cities",
        )

        cit_per_count_content = count_cit_ext.extract_content(
            cities_list_extractor_args,
        )
        if not cit_per_count_content:
            logger.info(f"No cities found for {country['country_2_code']}")

        if cit_per_count_content:
            content_writer.write_content(cit_per_count_content, cities_list_path)
