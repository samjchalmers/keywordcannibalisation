from __future__ import annotations

import asyncio
import logging
import random
from datetime import UTC, datetime, timedelta
from urllib.parse import urlsplit, urlunsplit
from urllib.robotparser import RobotFileParser

import httpx
from selectolax.parser import HTMLParser

from cannibalize.db.store import Store

log = logging.getLogger(__name__)

TRANSIENT_STATUSES = {408, 429, 500, 502, 503, 504}
MAX_ATTEMPTS = 3
REQUEST_TIMEOUT = 10.0


class RobotsCache:
    def __init__(self, user_agent: str) -> None:
        self.user_agent = user_agent
        self._parsers: dict[str, RobotFileParser | None] = {}

    async def allowed(self, client: httpx.AsyncClient, url: str) -> bool:
        parts = urlsplit(url)
        host_key = f"{parts.scheme}://{parts.netloc}"
        if host_key not in self._parsers:
            robots_url = urlunsplit((parts.scheme, parts.netloc, "/robots.txt", "", ""))
            try:
                r = await client.get(robots_url, timeout=REQUEST_TIMEOUT)
                if 200 <= r.status_code < 300 and r.text:
                    rp = RobotFileParser()
                    rp.parse(r.text.splitlines())
                    self._parsers[host_key] = rp
                else:
                    self._parsers[host_key] = None
            except Exception:
                self._parsers[host_key] = None

        rp = self._parsers[host_key]
        if rp is None:
            return True
        return rp.can_fetch(self.user_agent, url)


async def _backoff_sleep(attempt: int) -> None:
    delay = (2**attempt) + random.uniform(0, 0.5)
    await asyncio.sleep(delay)


async def _fetch_one(
    client: httpx.AsyncClient,
    url: str,
    semaphore: asyncio.Semaphore,
    robots: RobotsCache,
) -> tuple[str, str | None, str]:
    async with semaphore:
        try:
            if not await robots.allowed(client, url):
                return url, None, "blocked by robots.txt"
        except Exception as e:
            return url, None, f"robots check failed: {type(e).__name__}: {e}"

        last_err = "unknown"
        for attempt in range(MAX_ATTEMPTS):
            try:
                r = await client.get(url, timeout=REQUEST_TIMEOUT, follow_redirects=True)
                if r.status_code in TRANSIENT_STATUSES:
                    last_err = f"HTTP {r.status_code}"
                    if attempt < MAX_ATTEMPTS - 1:
                        await _backoff_sleep(attempt)
                        continue
                    return url, None, last_err
                if r.status_code >= 400:
                    return url, None, f"HTTP {r.status_code}"
                return url, r.text, "ok"
            except (httpx.TimeoutException, httpx.TransportError) as e:
                last_err = f"{type(e).__name__}: {e}"
                if attempt < MAX_ATTEMPTS - 1:
                    await _backoff_sleep(attempt)
                    continue
                return url, None, last_err
            except Exception as e:
                return url, None, f"{type(e).__name__}: {e}"
        return url, None, last_err


def _extract(html: str) -> tuple[str | None, str | None, str | None, str | None]:
    tree = HTMLParser(html)
    title_node = tree.css_first("title")
    title = title_node.text(strip=True) if title_node else None

    h1_node = tree.css_first("h1")
    h1 = h1_node.text(strip=True) if h1_node else None

    canonical = None
    canon_node = tree.css_first('link[rel="canonical"]')
    if canon_node:
        canonical = canon_node.attributes.get("href")

    for tag in ("script", "style", "nav", "footer", "header", "aside"):
        for node in tree.css(tag):
            node.decompose()

    main_node = tree.css_first("main") or tree.css_first("article") or tree.body
    body_text = main_node.text(separator=" ", strip=True) if main_node else None

    return title, h1, canonical, body_text


async def crawl_urls(
    urls: list[str],
    store: Store,
    *,
    concurrency: int = 10,
    user_agent: str = "cannibalize-bot/0.1",
) -> tuple[int, int]:
    semaphore = asyncio.Semaphore(concurrency)
    robots = RobotsCache(user_agent)
    ok_count = 0
    fail_count = 0

    headers = {"User-Agent": user_agent}
    async with httpx.AsyncClient(headers=headers) as client:
        tasks = [asyncio.create_task(_fetch_one(client, url, semaphore, robots)) for url in urls]
        try:
            for coro in asyncio.as_completed(tasks):
                url, html, status = await coro
                if html is None:
                    log.warning("crawl FAIL %s: %s", url, status)
                    fail_count += 1
                    continue
                try:
                    title, h1, canonical, body_text = _extract(html)
                except Exception as e:
                    log.warning("crawl PARSE FAIL %s: %s", url, e)
                    fail_count += 1
                    continue
                store.upsert_page(url, title, h1, canonical, body_text)
                ok_count += 1
        except (KeyboardInterrupt, asyncio.CancelledError):
            for t in tasks:
                t.cancel()
            raise

    return ok_count, fail_count


def crawl_all(
    store: Store,
    *,
    concurrency: int = 10,
    force: bool = False,
    max_age_days: int = 7,
) -> tuple[int, int]:
    all_urls = store.get_all_urls()
    if not force:
        cutoff = (datetime.now(UTC) - timedelta(days=max_age_days)).isoformat()
        recent = store.get_recently_crawled_urls(cutoff)
        all_urls = [u for u in all_urls if u not in recent]

    if not all_urls:
        return 0, 0

    return asyncio.run(crawl_urls(all_urls, store, concurrency=concurrency))
