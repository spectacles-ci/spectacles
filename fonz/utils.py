from typing import Sequence, List, Dict, Any, Union

JsonDict = Dict[str, Any]


def compose_url(
    url_base: str,
    endpoint: str,
    endpointid: Union[str, int] = None,
    subendpoint: str = None,
    subendpointid: Union[str, int] = None,
) -> str:

    url = "{}{}".format(url_base, endpoint)

    if endpointid:
        url += "/" + str(endpointid)

        if subendpoint:
            url += "/" + subendpoint

            if subendpointid:
                url += "/" + str(subendpointid)

    return url
