from typing import Optional, Callable

from internal_lib.files.file_paths import FilePaths
from dataclasses import dataclass


@dataclass
class HTMLElem:
    type: str
    property_key: str = ""
    property_value: str = ""
    is_find_all: Optional[bool] = False
    elem_position: Optional[int] = None


@dataclass
class ScrapeElems:
    row_search_elems: list[HTMLElem]
    row_extract_func: Callable


@dataclass
class ScrapeConfig:
    name: str
    url_path: str
    file_paths: FilePaths
    scrape_elems: ScrapeElems
