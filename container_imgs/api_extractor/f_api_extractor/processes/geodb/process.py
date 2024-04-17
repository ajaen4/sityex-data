import boto3

from internal_lib.aws.s3 import S3
from internal_lib.files.file_paths import FilePaths

from internal_lib.api.api_extractor import ApiExtractor, ExtractorArgs
from f_api_extractor.clients import GeoDBClient
import f_api_extractor.config as cfg
from internal_lib.files.content_writer import ContentWriter

from .transformers import (
    transform_country_code,
)


def run_geodb_workload():
    geo_db_client = GeoDBClient(
        host="wft-geo-db.p.rapidapi.com",
    )
    geo_ext = ApiExtractor(geo_db_client)

    file_paths = FilePaths(
        bucket_name=cfg.DATA_BUCKET_NAME,
        local_prefix="./outputs/geo_db/",
        s3_prefix=f"bronze/countries/geographical/countries_list/{cfg.FORMATTED_DATE}/",
        file_name="countries_list.csv",
    )
    params = {"limit": cfg.GEO_DB_REQUEST_LIMIT}
    extractor_args = ExtractorArgs(
        api_path="/v1/geo/countries",
        params=params,
        process_output_func=transform_country_code,
    )

    content = geo_ext.extract_content(extractor_args)

    s3_client = S3(boto3.Session())
    content_writer = ContentWriter(s3_client)
    content_writer.write_content(content, file_paths)
