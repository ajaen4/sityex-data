from f_api_extractor.logger import logger

from f_api_extractor.clients import Client

from .extractor_args import ExtractorArgs


class ApiExtractor:
    def __init__(self, client: Client):
        self.client = client

    def extract_content(
        self,
        extractor_args: ExtractorArgs,
    ):
        logger.info(f"Extracting content from {extractor_args.api_path}...")

        if extractor_args.use_cached_content:
            content = self.cached_content
        else:
            content = self.client.query_endpoint(
                extractor_args.api_path,
                params=extractor_args.params,
                data_key=extractor_args.data_key,
            )

        if extractor_args.cache_content:
            self.cached_content = content

        if content and extractor_args.process_output_func:
            content = extractor_args.process_output_func(content)

        logger.info(f"Finished extracting content from {extractor_args.api_path}")

        return content

    def extract_contents(
        self,
        extractor_args: ExtractorArgs,
    ):
        contents = list()
        for api_path in extractor_args.api_paths:
            logger.info(f"Extracting content from {api_path}...")

            content = self.client.query_endpoint(
                api_path, params=extractor_args.params, data_key=extractor_args.data_key
            )

            if extractor_args.process_output_func:
                content = extractor_args.process_output_func(content)

            contents.extend(content)

        logger.info(f"Finished extracting content from {api_path}")

        return contents
