from typing import Sequence, List, Dict, Any, Union
from urllib.parse import urljoin

JsonDict = Dict[str, Any]


def compose_url(base_url: str, path: List[str]) -> str:
    if not isinstance(path, list):
        raise TypeError("URL path must be a list")
    parts = [base_url] + path
    url = "/".join(str(part).strip("/") for part in parts)
    return url
