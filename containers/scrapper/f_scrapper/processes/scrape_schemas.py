from internal_lib.files.file_paths import FilePaths
from dataclasses import dataclass


@dataclass
class HTMLElem:
    type: str
    property_key: str = None
    property_value: str = None
    is_find_all: bool = False
    elem_position: int = None


@dataclass
class ScrapeElems:
    row_search_elems: list[HTMLElem]
    row_extract_func: callable


@dataclass
class ScrapeConfig:
    url_path: str
    file_paths: FilePaths
    scrape_elems: ScrapeElems
