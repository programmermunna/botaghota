"""Shared pytest fixtures: a local HTTP site and crawler helpers."""

import os
import sys
import threading
from collections import Counter
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import ClassVar

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import botaghota


def make_args(*argv):
    return botaghota.build_parser().parse_args(list(argv))


class SiteHandler(BaseHTTPRequestHandler):
    """Serves a dict of pages; counts every request path."""

    pages: ClassVar[dict] = {}
    robots: str = "User-agent: *\nAllow: /\n"
    hits: ClassVar[Counter] = Counter()
    sitemap: str = ""

    def do_GET(self):
        type(self).hits[self.path] += 1
        if self.path == "/reset":
            # Simulate a dead/broken host: close with no response.
            try:
                self.connection.shutdown(2)
            except OSError:
                pass
            self.connection.close()
            return
        if self.path == "/robots.txt":
            body = type(self).robots.encode()
            self._respond(200, "text/plain", body)
            return
        if self.path == "/sitemap.xml" and type(self).sitemap:
            self._respond(200, "application/xml",
                          type(self).sitemap.encode())
            return
        entry = type(self).pages.get(self.path)
        if entry is None:
            self._respond(404, "text/plain", b"not found")
            return
        status, ctype, body = entry
        self._respond(status, ctype,
                      body.encode() if isinstance(body, str) else body)

    def _respond(self, status, ctype, body):
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def log_message(self, *args):  # silence per-request logging
        pass


def page(links=(), title="t", body=""):
    anchors = "".join(f'<a href="{l}">x</a>' for l in links)
    return (200, "text/html",
            f"<html><head><title>{title}</title></head>"
            + f"<body>{anchors}{body}</body></html>")


@pytest.fixture
def site():
    """A dict-driven website on an ephemeral localhost port.

    Mutate `site.pages`/`site.robots`/`site.sitemap` before (or during) a
    crawl; `site.hits` records every request path.
    """
    SiteHandler.pages = {}
    SiteHandler.robots = "User-agent: *\nAllow: /\n"
    SiteHandler.sitemap = ""
    SiteHandler.hits = Counter()
    server = ThreadingHTTPServer(("127.0.0.1", 0), SiteHandler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    SiteHandler.base_url = f"http://127.0.0.1:{server.server_address[1]}"
    yield SiteHandler
    server.shutdown()
    server.server_close()


def run_crawler(tmp_path, seeds, seconds=2.0, extra_args=()):
    """Run a crawler in-process for `seconds`, then stop and checkpoint.

    Returns the Crawler so tests can inspect errors/state.
    """
    args = make_args("--data-dir", str(tmp_path), "--delay", "0.01",
                     *extra_args)
    crawler = botaghota.Crawler(args)
    crawler.polite.default_delay = 0.01
    thread = threading.Thread(
        target=crawler.run, args=(list(seeds),), daemon=True)
    thread.start()
    thread.join(timeout=seconds)
    crawler.request_stop()
    thread.join(timeout=10)
    crawler.checkpoint()
    return crawler


def all_crawled_urls(tmp_path):
    """Every line of every crawled_links/*.txt file."""
    urls = []
    links_dir = tmp_path / "crawled_links"
    if links_dir.exists():
        for f in sorted(links_dir.glob("*.txt")):
            urls.extend(f.read_text().split())
    return urls
