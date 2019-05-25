from typing import Sequence, List, Dict, Any, Union

JsonDict = Dict[str, Any]


def compose_url(url_base: str,
                endpoint: str,
                endpointid: Union[str, int] = None,
                subendpoint: str = None,
                subendpointid: Union[str, int] = None) -> str:

    if not url_base or not endpoint:
        raise Exception('compose_url requires url_base and endpoint.')

    url = "{}{}".format(url_base, endpoint)

    if endpointid:
        url += '/' + str(endpointid)

        if subendpoint:
            url += '/' + subendpoint

            if subendpointid:
                url += '/' + str(subendpointid)

    return url
