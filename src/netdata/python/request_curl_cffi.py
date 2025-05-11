from curl_cffi import requests as crequests
from typing import NamedTuple, Optional

class Response(NamedTuple):
    content: str
    status_code: int
    url: str
    ok: bool
    reason: str

def requests_with_curl_cffi(session: crequests.Session, url: str, timeout: int, headers: dict[str, str], proxy: Optional[str]) -> Response:
    session.headers.update(headers)
    if proxy:
        session.proxies.update({"http": proxy, "https": proxy})
    response = session.get(url, timeout=timeout)
    response_tuple = Response(response.text, response.status_code, response.url, response.ok, response.reason)
    return response_tuple
