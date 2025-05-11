import time
from typing import NamedTuple, Optional
from playwright.sync_api import sync_playwright


USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"
VIEWPORT = {"width": 1920, "height": 1080}

class Response(NamedTuple):
    content: str
    status_code: int
    url: str
    ok: bool
    reason: str

def requests_with_playwright(url: str, timeout: int, proxy: Optional[dict[str, str]], headless: bool, browser_wait: int, page_evaluation: Optional[str]) -> dict[str, str|int]:
    sync_playwright().start()
    with sync_playwright() as playwright:
        if proxy:
            browser = playwright.firefox.launch(headless=headless, proxy=proxy)
        else:
            browser = playwright.firefox.launch(headless=headless)
        context = browser.new_context(
            viewport=VIEWPORT,
            user_agent=USER_AGENT,
        )
        page = context.new_page()
        response = page.goto(url, timeout=timeout * 1000)
        if page_evaluation:
            page.evaluate(page_evaluation)
        time.sleep(browser_wait)
        response_tuple = Response(page.content(), response.status, page.url, response.ok, response.status_text)
        browser.close()
        return response_tuple


def get_header_for_requests(url: str, timeout: int, proxy: Optional[dict[str, str]], headless: bool) -> dict[str, str]:
    with sync_playwright() as playwright:
        header_dict = {}
        if proxy:
            browser = playwright.chromium.launch(headless=headless, proxy=proxy)
        else:
            browser = playwright.chromium.launch(headless=headless)
        context = browser.new_context(
            viewport=VIEWPORT,
            user_agent=USER_AGENT,
        )

        def log_request(route, request):
            url = request.url
            header = request.headers
            header_dict[url] = header
            route.continue_()
        
        context.route("**/*", log_request)
        try:
            page = context.new_page()
            page.goto(url, timeout=timeout * 1000)

            browser.close()

        except Exception as e:
            print("Fail to load header from {}. {}".format(url, e))
        return header_dict
