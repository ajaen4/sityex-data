from dataclasses import dataclass


@dataclass
class ExtractorArgs:
    api_path: str = None
    api_paths: list[str] = None
    params: dict[str, str] = None
    process_output_func: callable = None
    data_key: str = None
    cache_content: bool = False
    use_cached_content: bool = False
