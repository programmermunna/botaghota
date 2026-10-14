#!/usr/bin/env python3
"""botaghota — a polite, crash-safe, dependency-free web crawler.

Usage:
    python botaghota.py <seed-url> [--data-dir DIR] [--delay SECONDS]
                                   [--max-depth N] [--limit N]

Design rules (non-negotiable):
    * Standard library only. No paid APIs, no cloud services.
    * Never lose data on crash/power loss: SQLite transactions, atomic
      os.replace for state files, fsync on every durable write.
    * Bounded memory regardless of runtime: the frontier and the seen set
      live in SQLite, not in Python containers.
    * Polite: honors robots.txt and crawl-delay, per-host rate limits,
      exponential backoff on 429/5xx, honest User-Agent. Never bypasses
      logins, paywalls, CAPTCHAs, or robots rules.
    * Never crashes on bad input: every network/parse error is caught,
      logged, counted, and skipped.
"""

from __future__ import annotations

import argparse
import asyncio
import gzip
import hashlib
import json
import logging
import mmap
import os
import signal
import sqlite3
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import urllib.robotparser
import xml.etree.ElementTree
from collections import defaultdict
from html.parser import HTMLParser
from typing import Optional

__version__ = "0.1.0"

USER_AGENT = f"botaghota/{__version__} (+https://github.com/programmermunna/botaghota)"
DEFAULT_DELAY = 1.0            # seconds between requests to the same host
DEFAULT_TIMEOUT = 15.0         # per-request timeout
MAX_BODY_BYTES = 5 * 1024 * 1024   # never read more than 5 MiB of a page
MAX_URL_LENGTH = 2048
ROBOTS_TTL = 3600.0            # re-fetch robots.txt after this many seconds
ROBOTS_DENY_TTL = 1800.0       # how long a 5xx robots.txt blocks a host
BACKOFF_BASE = 30.0            # first backoff after 429/5xx
BACKOFF_MAX = 3600.0           # cap for exponential backoff
STATE_FLUSH_INTERVAL = 30.0    # seconds between state.json checkpoints

LOG = logging.getLogger("botaghota")


# --------------------------------------------------------------------------
# URL utilities
# --------------------------------------------------------------------------

def url_hash(url: str) -> int:
    """8-byte hash of a URL, stored as a signed 64-bit integer."""
    raw = hashlib.blake2b(url.encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(raw, "big", signed=True)


def clean_url(url: str) -> Optional[str]:
    """Normalize a URL for dedup; return None if it is not fetchable http(s)."""
    url = url.strip()
    if not url or len(url) > MAX_URL_LENGTH:
        return None
    try:
        parts = urllib.parse.urlsplit(url)
    except ValueError:
        return None
    scheme = parts.scheme.lower()
    if scheme not in ("http", "https"):
        return None
    host = (parts.hostname or "").lower()
    if not host:
        return None
    port = parts.port
    netloc = host
    if port and not (scheme == "http" and port == 80) \
            and not (scheme == "https" and port == 443):
        netloc = f"{host}:{port}"
    path = parts.path or "/"
    # /dir/index.html == /dir/ == /dir for dedup purposes.
    leaf = path.rsplit("/", 1)[-1]
    if leaf in ("index.html", "index.htm"):
        path = path[: len(path) - len(leaf)] or "/"
    if len(path) > 1 and path.endswith("/"):
        path = path[:-1]
    # Stable query ordering so ?a=1&b=2 dedups with ?b=2&a=1.
    query = parts.query
    if query:
        try:
            pairs = urllib.parse.parse_qsl(query, keep_blank_values=True)
            query = urllib.parse.urlencode(sorted(pairs))
        except ValueError:
            pass
    return urllib.parse.urlunsplit((scheme, netloc, path, query, ""))


def host_of(url: str) -> str:
    return urllib.parse.urlsplit(url).netloc


def domain_of(url: str) -> str:
    """Registrable-domain approximation: last two labels of the hostname.

    Good enough for frontier scoring and per-domain caps without a public
    suffix list dependency.
    """
    host = host_of(url).split(":", 1)[0]
    labels = host.split(".")
    return ".".join(labels[-2:]) if len(labels) >= 2 else host


def looks_like_trap(url: str, max_depth: int, depth: int) -> bool:
    """Heuristic crawler-trap guard: endless calendars, session IDs, loops."""
    if depth > max_depth:
        return True
    if len(url) > MAX_URL_LENGTH:
        return True
    parts = urllib.parse.urlsplit(url)
    path_segments = [s for s in parts.path.split("/") if s]
    # Repeating the same segment over and over (/a/a/a/a/...) is a trap.
    run = 1
    for prev, cur in zip(path_segments, path_segments[1:]):
        run = run + 1 if cur == prev else 1
        if run >= 4:
            return True
    if len(parts.query.split("&")) > 10 if parts.query else False:
        return True
    return False


# --------------------------------------------------------------------------
# Storage: the seen set, the frontier queue, and page records in SQLite
# --------------------------------------------------------------------------

class BloomSeen:
    """File-backed Bloom filter for dedup at 100M+ URL scale.

    The bit array lives in an mmap'd file: constant RSS regardless of how
    many URLs are seen, and it survives kill -9 because mmap dirty pages
    flush on checkpoint. Trades a small false-positive rate (an unseen URL
    may be skipped) for this bounded footprint — that is the documented
    deal of --bloom mode.
    """

    def __init__(self, path: str, mbits: int = 512, k: int = 4):
        self.m = mbits * 1024 * 1024          # bits
        self.k = k
        self.path = path
        self.count = 0
        nbytes = self.m // 8
        exists = os.path.exists(path) and os.path.getsize(path) == nbytes
        self._file = open(path, "r+b" if exists else "w+b")
        if not exists:
            self._file.truncate(nbytes)
            self._file.flush()
            os.fsync(self._file.fileno())
        self.mm = mmap.mmap(self._file.fileno(), 0)
        self._lock = threading.Lock()
        if exists:
            # Recover an approximate count from the bits on disk.
            meta = path + ".count"
            try:
                with open(meta, encoding="ascii") as f:
                    self.count = int(f.read().strip())
            except (OSError, ValueError):
                pass

    def _positions(self, url: str):
        digest = hashlib.blake2b(url.encode("utf-8"),
                                 digest_size=16).digest()
        h1 = int.from_bytes(digest[:8], "big")
        h2 = int.from_bytes(digest[8:], "big") | 1   # must stay non-zero
        for i in range(self.k):
            yield (h1 + i * h2) % self.m

    def is_seen(self, url: str) -> bool:
        mm = self.mm
        return all(
            mm[p >> 3] & (1 << (p & 7)) for p in self._positions(url))

    def _set(self, url: str) -> bool:
        """Set all bits; returns False if they were already set."""
        new = False
        for p in self._positions(url):
            byte = self.mm[p >> 3]
            bit = 1 << (p & 7)
            if not byte & bit:
                self.mm[p >> 3] = byte | bit
                new = True
        return new

    def claim(self, url: str) -> bool:
        with self._lock:
            if self.is_seen(url):
                return False
            self._set(url)
            self.count += 1
            return True

    def mark_seen(self, url: str) -> None:
        with self._lock:
            if self._set(url):
                self.count += 1

    def flush(self) -> None:
        self.mm.flush()
        tmp = self.path + ".count.tmp"
        try:
            with open(tmp, "w", encoding="ascii") as f:
                f.write(str(self.count))
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self.path + ".count")
        except OSError:
            pass

    def close(self) -> None:
        try:
            self.flush()
            self.mm.close()
        finally:
            self._file.close()


SCHEMA = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS seen (
    hash INTEGER PRIMARY KEY
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS queue (
    url_hash  INTEGER PRIMARY KEY,
    url       TEXT NOT NULL,
    depth     INTEGER NOT NULL,
    priority  REAL NOT NULL DEFAULT 0,
    next_fetch REAL NOT NULL DEFAULT 0
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS idx_queue_pop ON queue(next_fetch, priority);
CREATE TABLE IF NOT EXISTS inflight (
    url_hash  INTEGER PRIMARY KEY,
    url       TEXT NOT NULL,
    depth     INTEGER NOT NULL,
    claimed_at REAL NOT NULL
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS unclaimed (
    hash INTEGER PRIMARY KEY
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS domains (
    domain      TEXT PRIMARY KEY,
    first_seen  REAL NOT NULL,
    sitemap_done INTEGER NOT NULL DEFAULT 0
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS pages (
    url_hash   INTEGER PRIMARY KEY,
    url        TEXT NOT NULL,
    fetched_at REAL NOT NULL,
    status     INTEGER,
    depth      INTEGER NOT NULL,
    domain     TEXT NOT NULL
) WITHOUT ROWID;
"""


class Storage:
    """All persistent crawler state in one SQLite database.

    Every mutating operation runs inside a transaction, so a kill -9 or
    power loss can at worst lose the last committed page — never leave the
    database half-written.
    """

    def __init__(self, path: str, bloom_mbits: int = 0):
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        # check_same_thread=False: fetch calls may run in asyncio worker
        # threads; all access below is serialized by _lock anyway.
        self._conn = sqlite3.connect(path, timeout=30.0,
                                     isolation_level=None,
                                     check_same_thread=False)
        self._lock = threading.RLock()
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.executescript(SCHEMA)
        self.bloom: Optional[BloomSeen] = None
        if bloom_mbits:
            bits_path = os.path.join(
                os.path.dirname(os.path.abspath(path)), "bloom.bits")
            self.bloom = BloomSeen(bits_path, mbits=bloom_mbits)

    def close(self) -> None:
        try:
            self._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        except sqlite3.Error:
            pass
        self._conn.close()
        if self.bloom:
            self.bloom.close()

    def flush(self) -> None:
        """Persist volatile state (bloom bits) without closing."""
        if self.bloom:
            self.bloom.flush()

    # -- seen set ----------------------------------------------------------

    def is_seen(self, url: str) -> bool:
        if self.bloom:
            return self.bloom.is_seen(url)
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM seen WHERE hash = ?", (url_hash(url),)
            ).fetchone()
        return row is not None

    def mark_seen(self, url: str) -> None:
        if self.bloom:
            self.bloom.mark_seen(url)
            return
        with self._lock:
            self._conn.execute(
                "INSERT OR IGNORE INTO seen(hash) VALUES (?)",
                (url_hash(url),),
            )

    def claim(self, url: str) -> bool:
        """Atomically claim a URL for crawling.

        Returns True exactly once per URL (per run or across restarts),
        so two workers can never fetch the same page concurrently.
        """
        h = url_hash(url)
        with self._lock:
            if self.bloom:
                # A requeued URL lands in `unclaimed` (bloom bits can't be
                # unset); consume that ticket, else test-and-set the bits.
                cur = self._conn.execute(
                    "DELETE FROM unclaimed WHERE hash = ?", (h,))
                if cur.rowcount:
                    return True
                return self.bloom.claim(url)
            cur = self._conn.execute(
                "INSERT OR IGNORE INTO seen(hash) VALUES (?)", (h,))
        return cur.rowcount > 0

    def requeue(self, url: str, depth: int, next_fetch: float) -> None:
        """Un-claim a URL and put it back on the frontier, atomically."""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                h = url_hash(url)
                if self.bloom:
                    self._conn.execute(
                        "INSERT OR REPLACE INTO unclaimed(hash)"
                        " VALUES (?)", (h,))
                else:
                    self._conn.execute(
                        "DELETE FROM seen WHERE hash = ?", (h,))
                self._conn.execute(
                    "DELETE FROM inflight WHERE url_hash = ?", (h,))
                self._conn.execute(
                    "INSERT OR IGNORE INTO queue(url_hash, url, depth,"
                    " priority, next_fetch) VALUES (?, ?, ?, 0, ?)",
                    (h, url, depth, next_fetch),
                )
                self._conn.execute("COMMIT")
            except sqlite3.Error:
                self._conn.execute("ROLLBACK")
                raise

    def ack(self, url: str) -> None:
        """Mark a dequeued URL as fully handled (removes inflight row)."""
        with self._lock:
            self._conn.execute(
                "DELETE FROM inflight WHERE url_hash = ?", (url_hash(url),))

    def recover_inflight(self) -> int:
        """Move URLs claimed by a killed run back to the queue.

        Called once at startup: anything left in inflight was being
        fetched when the process died, so it was neither recorded nor
        re-queued. Returns the number recovered.
        """
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                rows = self._conn.execute(
                    "SELECT url_hash, url, depth FROM inflight").fetchall()
                for h, url, depth in rows:
                    if self.bloom:
                        self._conn.execute(
                            "INSERT OR REPLACE INTO unclaimed(hash)"
                            " VALUES (?)", (h,))
                    else:
                        self._conn.execute(
                            "DELETE FROM seen WHERE hash = ?", (h,))
                    self._conn.execute(
                        "INSERT OR IGNORE INTO queue(url_hash, url, depth,"
                        " priority, next_fetch) VALUES (?, ?, ?, 0, 0)",
                        (h, url, depth),
                    )
                self._conn.execute("DELETE FROM inflight")
                self._conn.execute("COMMIT")
            except sqlite3.Error:
                self._conn.execute("ROLLBACK")
                raise
        return len(rows)

    # -- frontier ----------------------------------------------------------

    def enqueue(self, url: str, depth: int, priority: float = 0.0,
                next_fetch: float = 0.0) -> bool:
        with self._lock:
            cur = self._conn.execute(
                "INSERT OR IGNORE INTO queue(url_hash, url, depth,"
                " priority, next_fetch) VALUES (?, ?, ?, ?, ?)",
                (url_hash(url), url, depth, priority, next_fetch),
            )
        return cur.rowcount > 0

    def dequeue(self, now: Optional[float] = None) -> Optional[tuple]:
        """Pop the highest-priority due URL, atomically."""
        now = time.time() if now is None else now
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._conn.execute(
                    "SELECT url_hash, url, depth FROM queue"
                    " WHERE next_fetch <= ?"
                    " ORDER BY priority DESC, next_fetch, url_hash"
                    " LIMIT 1",
                    (now,),
                ).fetchone()
                if row:
                    self._conn.execute(
                        "DELETE FROM queue WHERE url_hash = ?", (row[0],)
                    )
                    self._conn.execute(
                        "INSERT OR REPLACE INTO inflight(url_hash, url,"
                        " depth, claimed_at) VALUES (?, ?, ?, ?)",
                        (row[0], row[1], row[2], now),
                    )
                self._conn.execute("COMMIT")
            except sqlite3.Error:
                self._conn.execute("ROLLBACK")
                raise
        return (row[1], row[2]) if row else None

    def queue_size(self) -> int:
        with self._lock:
            return self._conn.execute(
                "SELECT COUNT(*) FROM queue").fetchone()[0]

    def seen_size(self) -> int:
        if self.bloom:
            return self.bloom.count
        with self._lock:
            return self._conn.execute(
                "SELECT COUNT(*) FROM seen").fetchone()[0]

    # -- page records ------------------------------------------------------

    def record_page(self, url: str, status: Optional[int], depth: int) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO pages(url_hash, url, fetched_at,"
                " status, depth, domain) VALUES (?, ?, ?, ?, ?, ?)",
                (url_hash(url), url, time.time(), status, depth,
                 domain_of(url)),
            )

    def top_domains(self, limit: int = 10) -> list:
        with self._lock:
            return self._conn.execute(
                "SELECT domain, COUNT(*) AS n FROM pages"
                " GROUP BY domain ORDER BY n DESC LIMIT ?",
                (limit,),
            ).fetchall()

    def domain_count(self, domain: str) -> int:
        with self._lock:
            return self._conn.execute(
                "SELECT COUNT(*) FROM pages WHERE domain = ?",
                (domain,),
            ).fetchone()[0]

    # -- domains (frontier scoring + sitemap tracking) ---------------------

    def note_domain(self, domain: str) -> bool:
        """Record a domain; returns True the first time it is seen."""
        with self._lock:
            cur = self._conn.execute(
                "INSERT OR IGNORE INTO domains(domain, first_seen)"
                " VALUES (?, ?)",
                (domain, time.time()),
            )
        return cur.rowcount > 0

    def claim_sitemap(self, domain: str) -> bool:
        """Atomically flip sitemap_done for a domain; True once per domain."""
        with self._lock:
            self.note_domain(domain)
            cur = self._conn.execute(
                "UPDATE domains SET sitemap_done = 1"
                " WHERE domain = ? AND sitemap_done = 0",
                (domain,),
            )
        return cur.rowcount > 0

    def last_fetched(self, url: str) -> Optional[float]:
        with self._lock:
            row = self._conn.execute(
                "SELECT fetched_at FROM pages WHERE url_hash = ?",
                (url_hash(url),),
            ).fetchone()
        return row[0] if row else None

    def page_known(self, url: str) -> bool:
        with self._lock:
            return self._conn.execute(
                "SELECT 1 FROM pages WHERE url_hash = ?",
                (url_hash(url),),
            ).fetchone() is not None


# --------------------------------------------------------------------------
# robots.txt
# --------------------------------------------------------------------------

class RobotsCache:
    """Per-host robots.txt parser cache.

    Policy: 4xx on robots.txt means "no robots file" (allow everything);
    5xx or a fetch error means the host is having trouble — treat as fully
    disallowed for ROBOTS_DENY_TTL seconds rather than hammering it.
    """

    def __init__(self, fetcher: "Fetcher"):
        self._fetcher = fetcher
        self._cache: dict[str, tuple] = {}   # host -> (parser_or_None, expiry)
        self._lock = threading.Lock()

    def _load(self, host: str, scheme: str) -> None:
        url = f"{scheme}://{host}/robots.txt"
        rp = urllib.robotparser.RobotFileParser()
        status, body = self._fetcher.fetch_raw(url, cap=512 * 1024)
        now = time.time()
        if status == 200 and body is not None:
            rp.parse(body.decode("utf-8", "replace").splitlines())
            self._cache[host] = (rp, now + ROBOTS_TTL)
        elif status is not None and 400 <= status < 500:
            self._cache[host] = (None, now + ROBOTS_TTL)  # no rules: allow
        else:
            rp.parse([])   # unreachable: robots fetch failed
            self._cache[host] = ("DENY", now + ROBOTS_DENY_TTL)

    def _entry(self, url: str):
        parts = urllib.parse.urlsplit(url)
        host = parts.netloc
        with self._lock:
            entry = self._cache.get(host)
            if entry is None or entry[1] < time.time():
                self._load(host, parts.scheme)
                entry = self._cache[host]
        return entry[0]

    def can_fetch(self, url: str) -> bool:
        rp = self._entry(url)
        if rp is None:
            return True
        if rp == "DENY":
            return False
        return rp.can_fetch(USER_AGENT, url)

    def crawl_delay(self, url: str) -> Optional[float]:
        rp = self._entry(url)
        if rp is None or rp == "DENY":
            return None
        delay = rp.crawl_delay(USER_AGENT)
        return float(delay) if delay else None

    def sitemaps(self, url: str) -> list:
        """Sitemap URL(s) declared in robots.txt, if any."""
        rp = self._entry(url)
        if rp is None or rp == "DENY":
            return []
        return list(rp.site_maps() or [])


# --------------------------------------------------------------------------
# Fetching
# --------------------------------------------------------------------------

class Fetcher:
    """urllib-based fetcher with a size cap and gzip decoding."""

    def __init__(self, timeout: float = DEFAULT_TIMEOUT):
        self.timeout = timeout
        # One opener per thread: OpenerDirector is not guaranteed
        # thread-safe and asyncio workers fetch via asyncio.to_thread.
        self._local = threading.local()

    def _opener(self):
        opener = getattr(self._local, "opener", None)
        if opener is None:
            opener = urllib.request.build_opener(
                urllib.request.HTTPRedirectHandler())
            self._local.opener = opener
        return opener

    def fetch_raw(self, url: str, cap: int = MAX_BODY_BYTES) -> tuple:
        """Fetch a URL. Returns (status, body_bytes).

        status is the HTTP status code, or None on a network-level error.
        Never raises.
        """
        req = urllib.request.Request(
            url, headers={
                "User-Agent": USER_AGENT,
                "Accept-Encoding": "gzip",
                "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
            }
        )
        try:
            with self._opener().open(req, timeout=self.timeout) as resp:
                data = resp.read(cap + 1)[:cap]
                if resp.headers.get("Content-Encoding") == "gzip":
                    try:
                        data = gzip.decompress(data)
                    except (OSError, EOFError):
                        pass
                return resp.status, data
        except urllib.error.HTTPError as e:
            return e.code, None
        except (urllib.error.URLError, OSError, ValueError, TimeoutError):
            return None, None

    def fetch(self, url: str) -> tuple:
        """Fetch and decode an HTML page. Returns (status, text_or_None)."""
        status, body = self.fetch_raw(url)
        if status != 200 or body is None:
            return status, None
        return status, body.decode("utf-8", "replace")


# --------------------------------------------------------------------------
# HTML link extraction (stdlib HTMLParser)
# --------------------------------------------------------------------------

class LinkExtractor(HTMLParser):
    """Collects <a href> links and <link rel=canonical> from a document."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.links: list[str] = []
        self.canonical: Optional[str] = None

    def handle_starttag(self, tag: str, attrs) -> None:
        attrs = dict(attrs)
        if tag == "a":
            href = attrs.get("href")
            if href:
                self.links.append(href)
        elif tag == "link":
            rel = (attrs.get("rel") or "").lower()
            if "canonical" in rel and attrs.get("href"):
                self.canonical = attrs["href"]


def extract_links(html: str, base_url: str) -> tuple[list[str],
                                                     Optional[str]]:
    """Returns (cleaned absolute outlinks, canonical URL or None)."""
    parser = LinkExtractor()
    try:
        parser.feed(html)
        parser.close()
    except Exception:  # HTMLParser is forgiving; still never propagate
        pass
    out = []
    for href in parser.links:
        abs_url = urllib.parse.urljoin(base_url, href)
        cleaned = clean_url(abs_url)
        if cleaned:
            out.append(cleaned)
    canonical = None
    if parser.canonical:
        canonical = clean_url(urllib.parse.urljoin(base_url,
                                                   parser.canonical))
    return out, canonical


# --------------------------------------------------------------------------
# sitemap.xml
# --------------------------------------------------------------------------

MAX_SITEMAP_URLS = 50_000


def parse_sitemap(data: bytes) -> tuple[list[str], list[str]]:
    """Parse sitemap XML. Returns (page_urls, child_sitemap_urls)."""
    try:
        root = xml.etree.ElementTree.fromstring(data)
    except xml.etree.ElementTree.ParseError:
        return [], []
    tag = root.tag.rsplit("}", 1)[-1]
    pages: list[str] = []
    children: list[str] = []
    if tag == "sitemapindex":
        for sm in root:
            loc = sm.findtext("{*}loc")
            if loc:
                children.append(loc.strip())
    elif tag == "urlset":
        for u in root:
            loc = u.findtext("{*}loc")
            if loc:
                pages.append(loc.strip())
    return pages, children


async def discover_sitemap_urls(crawler: "Crawler", page_url: str,
                                domain: str) -> None:
    """Fetch sitemap(s) for a host once and enqueue the URLs found."""
    parts = urllib.parse.urlsplit(page_url)
    maps = crawler.robots.sitemaps(page_url)[:5]
    if not maps:
        maps = [f"{parts.scheme}://{parts.netloc}/sitemap.xml"]
    total = 0
    for sm_url in maps:
        status, body = await asyncio.to_thread(
            crawler.fetcher.fetch_raw, sm_url)
        if status != 200 or body is None:
            continue
        pages, children = parse_sitemap(body)
        # One level of sitemap indexes is enough.
        for child in children[:10]:
            cstatus, cbody = await asyncio.to_thread(
                crawler.fetcher.fetch_raw, child)
            if cstatus == 200 and cbody is not None:
                c_pages, _ = parse_sitemap(cbody)
                pages.extend(c_pages)
        for raw in pages:
            if total >= MAX_SITEMAP_URLS:
                return
            cleaned = clean_url(raw)
            if cleaned and crawler.enqueue_link(cleaned, depth=0,
                                                priority=1.5):
                total += 1
        if total:
            LOG.info("sitemap %s: enqueued %d URL(s)", sm_url, total)


# --------------------------------------------------------------------------
# Per-host politeness: rate limiting + exponential backoff
# --------------------------------------------------------------------------

class Politeness:
    """Tracks per-host earliest-next-fetch times and 429/5xx backoff."""

    def __init__(self, default_delay: float = DEFAULT_DELAY):
        self.default_delay = default_delay
        self._next_at: dict[str, float] = {}
        self._fail_count: dict[str, int] = defaultdict(int)
        self._lock = threading.Lock()

    def note_fetched(self, host: str, crawl_delay: Optional[float]) -> None:
        delay = max(self.default_delay, crawl_delay or 0.0)
        with self._lock:
            self._next_at[host] = max(
                self._next_at.get(host, time.time()),
                time.time(),
            ) + delay
            self._fail_count[host] = 0

    def note_failure(self, host: str) -> None:
        """Exponential backoff for 429/5xx responses."""
        with self._lock:
            self._fail_count[host] += 1
            backoff = min(
                BACKOFF_MAX,
                BACKOFF_BASE * (2 ** (self._fail_count[host] - 1)),
            )
            self._next_at[host] = time.time() + backoff

    def ready_in(self, host: str) -> float:
        """Seconds until this host may be fetched again."""
        with self._lock:
            return max(0.0, self._next_at.get(host, 0.0) - time.time())


# --------------------------------------------------------------------------
# Atomic state file + daily link files
# --------------------------------------------------------------------------

class DailyLinkWriter:
    """Appends crawled URLs to crawled_links/YYYY-MM-DD.txt, fsynced."""

    def __init__(self, directory: str):
        self._dir = directory
        os.makedirs(directory, exist_ok=True)
        self._file = None
        self._day = None

    def _current(self):
        day = time.strftime("%Y-%m-%d")
        if day != self._day:
            if self._file:
                self._file.flush()
                os.fsync(self._file.fileno())
                self._file.close()
            self._day = day
            path = os.path.join(self._dir, f"{day}.txt")
            self._file = open(path, "a", encoding="utf-8", buffering=1)
        return self._file

    def write(self, url: str) -> None:
        f = self._current()
        f.write(url + "\n")
        f.flush()
        os.fsync(f.fileno())   # survive kill -9 / power loss

    def close(self) -> None:
        if self._file:
            self._file.flush()
            os.fsync(self._file.fileno())
            self._file.close()
            self._file = None


def write_state(path: str, state: dict) -> None:
    """Atomically write JSON state (tmp file + os.replace + dir fsync)."""
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(os.path.abspath(path)),
                             suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2, sort_keys=True)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
        dir_fd = os.open(os.path.dirname(os.path.abspath(path)), os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    except OSError:
        try:
            os.unlink(tmp)
        except OSError:
            pass


# --------------------------------------------------------------------------
# The crawler
# --------------------------------------------------------------------------

class Crawler:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.data_dir = args.data_dir
        os.makedirs(self.data_dir, exist_ok=True)
        self.storage = Storage(
            os.path.join(self.data_dir, "botaghota.db"),
            bloom_mbits=args.bloom_mbits if args.bloom else 0)
        self.fetcher = Fetcher()
        self.robots = RobotsCache(self.fetcher)
        self.polite = Politeness(default_delay=args.delay)
        self.writer = DailyLinkWriter(
            os.path.join(self.data_dir, "crawled_links"))
        self.state_path = os.path.join(self.data_dir, "state.json")
        self.pages_done = 0
        self.errors: dict[str, int] = defaultdict(int)
        self.started_at = time.time()
        self._stop = threading.Event()
        self._last_state_flush = 0.0
        self._closed = False
        self._seed_urls: list[str] = []

    # -- lifecycle --------------------------------------------------------

    def request_stop(self, *_args) -> None:
        LOG.info("Shutdown requested; finishing current page...")
        self._stop.set()

    def run(self, seeds: list[str]) -> None:
        """Entry point: run the async crawl loop with --workers coroutines."""
        try:
            asyncio.run(self._run_async(seeds))
        finally:
            self.checkpoint()

    async def _run_async(self, seeds: list[str]) -> None:
        recovered = self.storage.recover_inflight()
        if recovered:
            LOG.warning("Recovered %d URL(s) lost mid-flight by a previous"
                        " unclean shutdown.", recovered)
        for seed in seeds:
            cleaned = clean_url(seed)
            if cleaned is None:
                LOG.error("Seed %r is not a valid http(s) URL; skipping",
                          seed)
                continue
            self._seed_urls.append(cleaned)
            self.storage.note_domain(domain_of(cleaned))
            self.storage.enqueue(cleaned, depth=0, priority=3.0)
        workers = [
            asyncio.create_task(self._worker(i), name=f"w{i}")
            for i in range(max(1, self.args.workers))
        ]
        flusher = asyncio.create_task(self._flusher())
        await asyncio.gather(*workers)
        flusher.cancel()
        try:
            await flusher
        except asyncio.CancelledError:
            pass

    async def _flusher(self) -> None:
        while not self._stop.is_set():
            self.maybe_flush_state()
            await asyncio.sleep(STATE_FLUSH_INTERVAL)

    def _reseed(self, seeds: list[str]) -> int:
        n = 0
        for seed in seeds:
            if self.maybe_recrawl(seed, depth=0):
                n += 1
            elif self.storage.enqueue(seed, depth=0, priority=3.0):
                n += 1
        return n

    def maybe_recrawl(self, url: str, depth: int) -> bool:
        """If a seen URL is stale enough, schedule a recrawl."""
        interval = self.args.recrawl_hours * 3600
        if interval <= 0:
            return False
        last = self.storage.last_fetched(url)
        if last is None:
            return False
        if time.time() - last < interval:
            return False
        self.storage.requeue(url, depth, next_fetch=time.time())
        return True

    async def _worker(self, worker_id: int) -> None:
        while not self._stop.is_set():
            if self.args.limit and self.pages_done >= self.args.limit:
                LOG.info("Reached --limit %d; stopping.", self.args.limit)
                self._stop.set()
                break
            item = self.storage.dequeue()
            if item is None:
                if self.storage.queue_size() == 0:
                    n = self._reseed(self._seed_urls)
                    LOG.info("Queue empty; re-seeded %d URL(s).", n)
                await asyncio.sleep(0.5)
                continue
            url, depth = item
            host = host_of(url)
            wait = self.polite.ready_in(host)
            if wait > 0:
                self.storage.enqueue(url, depth=depth,
                                     next_fetch=time.time() + wait)
                await asyncio.sleep(min(wait, 1.0))
                continue
            await self.crawl_page(url, depth)

    # -- per-page work -----------------------------------------------------

    async def crawl_page(self, url: str, depth: int) -> None:
        try:
            if not self.storage.claim(url):
                return   # already seen or claimed by another worker
            await self._crawl_page_claimed(url, depth)
        finally:
            self.storage.ack(url)

    async def _crawl_page_claimed(self, url: str, depth: int) -> None:
        if looks_like_trap(url, self.args.max_depth, depth):
            self.errors["trap"] += 1
            return
        domain = domain_of(url)
        cap = self.args.domain_cap
        if cap and self.storage.domain_count(domain) >= cap:
            self.errors["domain_cap"] += 1
            return
        allowed = await asyncio.to_thread(self.robots.can_fetch, url)
        if not allowed:
            self.errors["robots_denied"] += 1
            LOG.debug("robots.txt denies %s", url)
            return
        if self.args.sitemap and self.storage.claim_sitemap(domain):
            try:
                await discover_sitemap_urls(self, url, domain)
            except Exception as e:   # sitemap trouble never kills a crawl
                self.errors["sitemap"] += 1
                LOG.debug("sitemap discovery failed for %s: %s", domain, e)
        delay = await asyncio.to_thread(self.robots.crawl_delay, url)
        host = host_of(url)
        status, html = await asyncio.to_thread(self.fetcher.fetch, url)
        if status is None:
            self.errors["network"] += 1
            # A dropped connection is URL-level trouble, not a signal the
            # host wants slower crawling — count it, move on.
            return
        if status == 429 or status >= 500:
            key = f"http_{status}"
            self.errors[key] += 1
            self.polite.note_failure(host)
            # Put it back so the page is not lost, after backoff. The
            # claim is released atomically with the re-enqueue.
            self.storage.requeue(url, depth,
                                 next_fetch=time.time() + BACKOFF_BASE)
            return
        self.polite.note_fetched(host, delay)
        if status != 200 or html is None:
            self.errors[f"http_{status}"] += 1
            self.storage.record_page(url, status, depth)
            return

        first_crawl = not self.storage.page_known(url)
        self.storage.record_page(url, status, depth)
        self.pages_done += 1
        LOG.info("[%d] %s (%s)", self.pages_done, url,
                 asyncio.current_task().get_name())

        links, canonical = extract_links(html, url)
        out_url = canonical or url
        if canonical and canonical != url:
            # The page says this content lives at the canonical URL; mark
            # it seen so we never fetch the same content twice.
            self.storage.mark_seen(canonical)
        if first_crawl:
            self.writer.write(out_url)   # daily files stay duplicate-free

        for link in links:
            self.enqueue_link(link, depth + 1)

    def enqueue_link(self, link: str, depth: int,
                     priority: float = 0.0) -> bool:
        """Dedup + trap-check + priority-scored enqueue of an outlink."""
        if self.storage.is_seen(link):
            self.maybe_recrawl(link, depth)
            return False
        if looks_like_trap(link, self.args.max_depth, depth):
            return False
        is_new_domain = self.storage.note_domain(domain_of(link))
        if priority <= 0 and is_new_domain:
            priority = 2.0   # never-before-seen domain: crawl first
        return self.storage.enqueue(link, depth=depth, priority=priority)

    # -- state -------------------------------------------------------------

    def snapshot_state(self) -> dict:
        elapsed = max(0.001, time.time() - self.started_at)
        return {
            "pages": self.pages_done,
            "seen": self.storage.seen_size(),
            "queue": self.storage.queue_size(),
            "pages_per_hour": round(self.pages_done / elapsed * 3600, 1),
            "errors": dict(self.errors),
            "started_at": self.started_at,
            "updated_at": time.time(),
        }

    def maybe_flush_state(self, force: bool = False) -> None:
        now = time.time()
        if force or now - self._last_state_flush >= STATE_FLUSH_INTERVAL:
            self._last_state_flush = now
            self.storage.flush()
            write_state(self.state_path, self.snapshot_state())

    def checkpoint(self) -> None:
        if self._closed:
            return
        self._closed = True
        self.maybe_flush_state(force=True)
        self.writer.close()
        self.storage.close()

    # -- stats -------------------------------------------------------------

    def print_stats(self) -> None:
        state = self.snapshot_state()
        print(f"pages crawled : {state['pages']}")
        print(f"unique seen   : {state['seen']}")
        print(f"queue size    : {state['queue']}")
        print(f"pages/hour    : {state['pages_per_hour']}")
        if self.errors:
            print("errors:")
            for kind, n in sorted(self.errors.items(), key=lambda kv: -kv[1]):
                print(f"  {kind:<14} {n}")
        top = self.storage.top_domains(10)
        if top:
            print("top domains:")
            for domain, n in top:
                print(f"  {n:>6}  {domain}")


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="botaghota",
        description="Polite, crash-safe, dependency-free web crawler.",
    )
    p.add_argument("seed", nargs="*", help="Seed URL(s) to start crawling from")
    p.add_argument("--data-dir", default="data",
                   help="Directory for the database, state, and output "
                        "(default: ./data)")
    p.add_argument("--delay", type=float, default=DEFAULT_DELAY,
                   help="Minimum seconds between requests per host "
                        f"(default: {DEFAULT_DELAY})")
    p.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT,
                   help="Per-request timeout in seconds "
                        f"(default: {DEFAULT_TIMEOUT})")
    p.add_argument("--max-depth", type=int, default=10,
                   help="Maximum link depth from a seed (default: 10)")
    p.add_argument("--domain-cap", type=int, default=0,
                   help="Max pages per domain (0 = unlimited)")
    p.add_argument("--limit", type=int, default=0,
                   help="Stop after N pages (0 = run forever)")
    p.add_argument("--workers", type=int, default=1,
                   help="Concurrent fetch workers (default: 1)")
    p.add_argument("--recrawl-hours", type=float, default=24.0,
                   help="Recrawl a page after this many hours "
                        "(0 = never recrawl; default: 24)")
    p.add_argument("--no-sitemap", dest="sitemap", action="store_false",
                   help="Do not fetch sitemap.xml for new domains")
    p.set_defaults(sitemap=True)
    p.add_argument("--bloom", action="store_true",
                   help="Use a file-backed Bloom filter for dedup "
                        "(constant memory; small false-positive rate)")
    p.add_argument("--bloom-mbits", type=int, default=512,
                   help="Bloom filter size in mebibits, used with --bloom "
                        "(default: 512 = 64 MiB file, ~100M URLs)")
    p.add_argument("--stats", action="store_true",
                   help="Print crawl statistics and exit")
    p.add_argument("-v", "--verbose", action="store_true",
                   help="Debug logging")
    return p


def main(argv: Optional[list] = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    crawler = Crawler(args)
    if args.stats:
        crawler.print_stats()
        crawler.checkpoint()
        return 0
    if not args.seed:
        build_parser().error("at least one seed URL is required")
    crawler.fetcher.timeout = args.timeout

    def _sig(signum, _frame):
        LOG.info("Received signal %d", signum)
        crawler.request_stop()

    signal.signal(signal.SIGINT, _sig)
    signal.signal(signal.SIGTERM, _sig)
    try:
        crawler.run(args.seed)
    finally:
        crawler.checkpoint()
    return 0


if __name__ == "__main__":
    sys.exit(main())
