from typing import List
import requests


def compose_url(base_url: str, path: List) -> str:
    if not isinstance(path, list):
        raise TypeError("URL path must be a list")
    parts = [base_url] + path
    url = "/".join(str(part).strip("/") for part in parts)
    return url


def details_from_http_error(response: requests.Response) -> str:
    try:
        response_json = response.json()
    # Requests raises a ValueError if the response is invalid JSON
    except ValueError:
        details = ""
    else:
        details = response_json.get("message")
    details = details.strip() if details else ""
    return details
