import boto3

from aws_lib.s3 import S3
from files.file_paths import FilePaths

from files.content_writer import ContentWriter
from f_api_extractor.api_extractor import ApiExtractor, ExtractorArgs
from f_api_extractor.clients import IMFClient
import f_api_extractor.config as cfg

from .transformers import dict_to_list_dict


def run_imf_workload():
    imf_client = IMFClient(host="www.imf.org")
    imf_ext = ApiExtractor(imf_client)
    s3_client = S3(boto3.Session())
    content_writer = ContentWriter(s3_client)

    inflation_file_paths = FilePaths(
        bucket_name=cfg.DATA_BUCKET_NAME,
        local_prefix="./outputs/countries/indicators/",
        s3_prefix=f"bronze/countries/indicators/{cfg.FORMATTED_DATE}/",
        file_name="inflation.csv",
    )

    inflation_ind = "PCPIPCH"
    inf_extractor_args = ExtractorArgs(
        api_path=f"/external/datamapper/api/v1/{inflation_ind}",
        process_output_func=dict_to_list_dict,
    )

    inf_content = imf_ext.extract_content(inf_extractor_args)
    content_writer.write_content(inf_content, inflation_file_paths)

    unemp_file_paths = FilePaths(
        bucket_name=cfg.DATA_BUCKET_NAME,
        local_prefix="./outputs/countries/indicators/",
        s3_prefix=f"bronze/countries/indicators/{cfg.FORMATTED_DATE}/",
        file_name="unemployment_rate.csv",
    )

    unemp_rate = "LUR"
    unemp_extractor_args = ExtractorArgs(
        api_path=f"/external/datamapper/api/v1/{unemp_rate}",
        process_output_func=dict_to_list_dict,
    )

    unemp_content = imf_ext.extract_content(unemp_extractor_args)
    content_writer.write_content(unemp_content, unemp_file_paths)

    PPP_file_paths = FilePaths(
        bucket_name=cfg.DATA_BUCKET_NAME,
        local_prefix="./outputs/countries/indicators/",
        s3_prefix=f"bronze/countries/indicators/{cfg.FORMATTED_DATE}/",
        file_name="purchasing_power_parity.csv",
    )

    PPP_int_dollars = "PPPPC"
    PPP_int_extractor_args = ExtractorArgs(
        api_path=f"/external/datamapper/api/v1/{PPP_int_dollars}",
        process_output_func=dict_to_list_dict,
    )

    PPP_int_content = imf_ext.extract_content(PPP_int_extractor_args)
    content_writer.write_content(PPP_int_content, PPP_file_paths)
