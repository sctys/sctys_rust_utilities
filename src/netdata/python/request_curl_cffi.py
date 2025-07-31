from curl_cffi import requests as crequests
from typing import NamedTuple, Optional

class Response(NamedTuple):
    content: str
    status_code: int
    url: str
    ok: bool
    reason: str
    cookies: dict[str, str]

def requests_with_curl_cffi(session: crequests.Session, url: str, timeout: tuple[int], headers: dict[str, str], proxy: Optional[str]) -> Response:
    session.headers.update(headers)
    if proxy:
        session.proxies.update({"http": proxy, "https": proxy})
    try:
        response = session.get(url, timeout=timeout)
        response_tuple = Response(response.text, response.status_code, response.url, response.ok, response.reason, response.cookies.get_dict())
        return response_tuple
    except crequests.exceptions.Timeout as e:
        if proxy:
            proxy_url = proxy.split("@")[-1]
            print(f"Timeout connecting through proxy {proxy_url}")
            raise
    except Exception as e:
        raise
    
    
