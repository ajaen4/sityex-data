from dataclasses import dataclass, field
from typing import Optional, Callable


@dataclass
class ExtractorArgs:
    api_path: str = ""
    api_paths: list[str] = field(default_factory=list)
    params: dict[str, str] = field(default_factory=dict)
    process_output_func: Optional[Callable] = None
    data_key: str = ""
    cache_content: bool = False
    use_cached_content: bool = False
