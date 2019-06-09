from typing import List, Any, Dict
from urllib.parse import urljoin, urlencode

JsonDict = Dict[str, Any]


def compose_url(base_url: str, path: List, query: JsonDict = None) -> str:
    if not isinstance(path, list):
        raise TypeError("URL path must be a list")
    parts = [base_url] + path
    url = "/".join(str(part).strip("/") for part in parts)
    if query:
        url = "{}?{}".format(url, urlencode(query))
    return url
