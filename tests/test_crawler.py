import asyncio

import httpx
import pytest

from cannibalize.ingest.crawler import crawl_urls

HTML = """<!doctype html>
<html><head>
  <title>Running shoes</title>
  <link rel="canonical" href="https://x.com/canon">
  <style>body{}</style>
</head><body>
  <nav>nav links</nav>
  <main>
    <h1>Best running shoes</h1>
    <p>Long body about marathon training and running shoes.</p>
  </main>
  <footer>footer</footer>
</body></html>
"""

ROBOTS_DISALLOW = "User-agent: *\nDisallow: /blocked\n"


def _handler(request: httpx.Request) -> httpx.Response:
    path = request.url.path
    if path == "/robots.txt":
        return httpx.Response(200, text=ROBOTS_DISALLOW)
    if path.startswith("/blocked"):
        return httpx.Response(200, text=HTML)
    if path == "/404":
        return httpx.Response(404, text="nope")
    return httpx.Response(200, text=HTML)


@pytest.fixture
def mock_transport(monkeypatch):
    transport = httpx.MockTransport(_handler)
    real_client = httpx.AsyncClient

    def patched(*args, **kwargs):
        kwargs["transport"] = transport
        return real_client(*args, **kwargs)

    monkeypatch.setattr("cannibalize.ingest.crawler.httpx.AsyncClient", patched)


def test_crawler_extracts_and_respects_robots(store, mock_transport):
    urls = [
        "https://x.com/ok",
        "https://x.com/blocked/page",
        "https://x.com/404",
    ]
    ok, fail = asyncio.run(crawl_urls(urls, store, concurrency=3))
    assert ok == 1
    assert fail == 2

    page = store.get_page("https://x.com/ok")
    assert page is not None
    assert page["title"] == "Running shoes"
    assert page["h1"] == "Best running shoes"
    assert page["canonical"] == "https://x.com/canon"
    assert "marathon" in page["body_text"]
    assert "nav links" not in page["body_text"]
    assert "footer" not in page["body_text"]
