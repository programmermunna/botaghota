"""End-to-end crawler tests against the local test site fixture."""

import os
import signal
import subprocess
import sys
import threading
import time
from collections import Counter
from http.server import ThreadingHTTPServer
from typing import ClassVar

from conftest import SiteHandler, all_crawled_urls, page, run_crawler

import botaghota

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def build_site(site, n=6):
    """index -> /p0../p{n-1}; each page links back to index."""
    site.pages["/"] = page(links=[f"/p{i}.html" for i in range(n)])
    for i in range(n):
        site.pages[f"/p{i}.html"] = page(links=["/"], title=f"p{i}")


def test_crawls_whole_site(site, tmp_path):
    build_site(site)
    run_crawler(tmp_path, [site.base_url + "/"], seconds=3.0)
    urls = set(all_crawled_urls(tmp_path))
    assert site.base_url + "/" in urls
    assert site.base_url + "/p3.html" in urls


def test_dedup_across_runs(site, tmp_path):
    """Restarting with the same data dir must not duplicate links."""
    build_site(site)
    run_crawler(tmp_path, [site.base_url + "/"], seconds=3.0)
    run_crawler(tmp_path, [site.base_url + "/"], seconds=2.0)
    urls = all_crawled_urls(tmp_path)
    assert len(urls) == len(set(urls))


def test_robots_disallow(site, tmp_path):
    site.pages["/"] = page(links=["/private/secret.html", "/ok.html"])
    site.pages["/private/secret.html"] = page()
    site.pages["/ok.html"] = page()
    site.robots = "User-agent: *\nDisallow: /private/\n"
    run_crawler(tmp_path, [site.base_url + "/"], seconds=3.0)
    assert site.hits.get("/private/secret.html", 0) == 0
    urls = set(all_crawled_urls(tmp_path))
    assert site.base_url + "/ok.html" in urls
    assert site.base_url + "/private/secret.html" not in urls


def test_crawl_delay_is_honored(site, tmp_path):
    site.pages["/"] = page(links=["/p0.html", "/p1.html", "/p2.html"])
    for i in range(3):
        site.pages[f"/p{i}.html"] = page()
    site.robots = "User-agent: *\nAllow: /\nCrawl-delay: 0.2\n"
    start = time.time()
    crawler = run_crawler(tmp_path, [site.base_url + "/"], seconds=1.5,
                          extra_args=("--delay", "0.05"))
    elapsed = time.time() - start
    n = len(all_crawled_urls(tmp_path))
    # 4 pages at >= 0.2s spacing cannot finish far inside the window.
    assert elapsed >= 0.2 * max(0, n - 1) * 0.5
    assert crawler.errors.get("robots_denied", 0) == 0


def test_trap_guard_bounds_infinite_site(site, tmp_path):
    """A 'calendar' that always links to the next page is bounded by depth."""
    for i in range(50):
        site.pages[f"/cal/{i}"] = page(links=[f"/cal/{i + 1}"])
    site.pages["/"] = page(links=["/cal/0"])
    run_crawler(tmp_path, [site.base_url + "/"], seconds=3.0,
                extra_args=("--max-depth", "5"))
    cal_hits = sum(v for k, v in site.hits.items() if k.startswith("/cal/"))
    assert 0 < cal_hits <= 8


def test_network_errors_never_crash(site, tmp_path):
    """Broken connections + dead hosts: crawler survives and counts errors."""
    site.pages["/"] = page(links=["/reset", "http://127.0.0.1:1/dead",
                                  "/ok.html"])
    site.pages["/ok.html"] = page()
    crawler = run_crawler(tmp_path, [site.base_url + "/"], seconds=4.0)
    # /reset drops the connection mid-request -> "network"; the dead host
    # fails robots.txt fetch -> "robots_denied". Both counted, no crash.
    assert crawler.errors.get("network", 0) >= 1
    assert site.base_url + "/ok.html" in set(all_crawled_urls(tmp_path))


def test_state_file_written(site, tmp_path):
    import json
    build_site(site, n=3)
    run_crawler(tmp_path, [site.base_url + "/"], seconds=2.0)
    state = json.loads((tmp_path / "state.json").read_text())
    assert state["pages"] >= 1
    assert state["seen"] >= state["pages"]


def test_crash_recovery_no_duplicates(site, tmp_path):
    """kill -9 mid-run, restart: no lost and no duplicated links."""
    build_site(site, n=12)
    env = dict(os.environ)
    cmd = [sys.executable, os.path.join(REPO, "botaghota.py"),
           site.base_url + "/", "--data-dir", str(tmp_path),
           "--delay", "0.02"]
    proc = subprocess.Popen(cmd, cwd=REPO, env=env,
                            stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL)
    # Wait until a few pages are durably written, then kill -9.
    deadline = time.time() + 15
    while time.time() < deadline:
        if len(all_crawled_urls(tmp_path)) >= 3:
            break
        time.sleep(0.05)
    proc.kill()
    proc.wait(timeout=10)
    assert len(all_crawled_urls(tmp_path)) >= 1

    proc2 = subprocess.Popen(cmd, cwd=REPO, env=env,
                             stdout=subprocess.DEVNULL,
                             stderr=subprocess.DEVNULL)
    time.sleep(5)
    proc2.send_signal(signal.SIGTERM)
    proc2.wait(timeout=15)

    urls = all_crawled_urls(tmp_path)
    assert len(urls) == len(set(urls)), "duplicate links after crash"
    expected = {site.base_url + "/"} | {
        site.base_url + f"/p{i}.html" for i in range(12)}
    assert expected.issubset(set(urls)), "lost links after crash"


def test_sitemap_discovery(site, tmp_path):
    """URLs only reachable via sitemap.xml are crawled once per domain."""
    site.pages["/"] = page()
    site.pages["/sm1.html"] = page()
    site.pages["/sm2.html"] = page()
    site.sitemap = (
        '<?xml version="1.0"?>'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
        f"<url><loc>{site.base_url}/sm1.html</loc></url>"
        f"<url><loc>{site.base_url}/sm2.html</loc></url>"
        "</urlset>")
    run_crawler(tmp_path, [site.base_url + "/"], seconds=3.0)
    urls = set(all_crawled_urls(tmp_path))
    assert site.base_url + "/sm1.html" in urls
    assert site.base_url + "/sm2.html" in urls


def test_recrawl_after_interval(site, tmp_path):
    """A stale page is refetched once its recrawl interval passes."""
    import sqlite3
    build_site(site, n=2)
    run_crawler(tmp_path, [site.base_url + "/"], seconds=2.0)
    hits_first = site.hits.get("/p0.html", 0)
    assert hits_first >= 1
    # Age every page beyond the recrawl interval.
    conn = sqlite3.connect(str(tmp_path / "botaghota.db"))
    conn.execute("UPDATE pages SET fetched_at = fetched_at - 100000")
    conn.commit()
    conn.close()
    run_crawler(tmp_path, [site.base_url + "/"], seconds=2.5,
                extra_args=("--recrawl-hours", "0.001"))
    assert site.hits.get("/p0.html", 0) > hits_first
    # Recrawls never duplicate lines in the daily files.
    urls = all_crawled_urls(tmp_path)
    assert len(urls) == len(set(urls))


def test_canonical_dedup(site, tmp_path):
    """A page's rel=canonical is recorded, not refetched."""
    site.pages["/"] = page(links=["/a?utm_source=x"])
    site.pages["/a?utm_source=x"] = (
        200, "text/html",
        '<html><head><link rel="canonical" href="/a">'
        + "<title>a</title></head><body></body></html>")
    site.pages["/a"] = page()
    run_crawler(tmp_path, [site.base_url + "/"], seconds=3.0)
    urls = set(all_crawled_urls(tmp_path))
    # The canonical URL is written; the parameterized one is not.
    assert site.base_url + "/a" in urls
    assert site.base_url + "/a?utm_source=x" not in urls


def test_bloom_mode_crawls_and_persists(site, tmp_path):
    """--bloom dedups across restarts with constant memory."""
    build_site(site, n=4)
    args = ("--bloom", "--bloom-mbits", "1")
    run_crawler(tmp_path, [site.base_url + "/"], seconds=3.0,
                extra_args=args)
    run_crawler(tmp_path, [site.base_url + "/"], seconds=2.0,
                extra_args=args)
    urls = all_crawled_urls(tmp_path)
    assert len(urls) == len(set(urls))
    assert site.base_url + "/p1.html" in set(urls)
    assert (tmp_path / "bloom.bits").stat().st_size == (1024 * 1024) // 8


def test_extract_writes_jsonl(site, tmp_path):
    import json as _json
    site.pages["/"] = (200, "text/html",
                       '<html lang="en"><head><title>Home</title>'
                       + '<meta name="description" content="the home page">'
                       + "</head><body></body></html>")
    run_crawler(tmp_path, [site.base_url + "/"], seconds=2.0,
                extra_args=("--extract",))
    out = list((tmp_path / "extracted").glob("*.jsonl"))
    assert out, "no .jsonl output"
    rec = _json.loads(out[0].read_text().splitlines()[0])
    assert rec["url"] == site.base_url + "/"
    assert rec["title"] == "Home"
    assert rec["description"] == "the home page"
    assert rec["lang"] == "en"
    assert rec["status"] == 200
    assert "content_hash" in rec


def test_extract_custom_plugin(site, tmp_path):
    plugin = tmp_path / "myplugin.py"
    plugin.write_text(
        "def extract(url, html):\n"
        "    return {'n_links': html.count('<a ')}\n")
    site.pages["/"] = page(links=["/p0.html"])
    site.pages["/p0.html"] = page()
    run_crawler(tmp_path, [site.base_url + "/"], seconds=2.0,
                extra_args=("--extract", str(plugin)))
    import json as _json
    rec = _json.loads(
        next((tmp_path / "extracted").glob("*.jsonl"))
        .read_text().splitlines()[0])
    assert rec["extractor"] == str(plugin)
    assert rec["n_links"] == 1


def test_stats_runs(site, tmp_path, capsys):
    build_site(site, n=3)
    run_crawler(tmp_path, [site.base_url + "/"], seconds=2.0)
    args = botaghota.build_parser().parse_args(
        ["--data-dir", str(tmp_path), "--stats"])
    c = botaghota.Crawler(args)
    c.print_stats()
    c.checkpoint()
    out = capsys.readouterr().out
    assert "queue size" in out
    assert "pages" in out


def test_allow_deny_domains(site, tmp_path):
    site.pages["/"] = page(links=["/ok.html", "http://other.test/x",
                                  "http://127.0.0.1:1/blocked"])
    site.pages["/ok.html"] = page()
    run_crawler(tmp_path, [site.base_url + "/"], seconds=3.0,
                extra_args=("--allow-domain", "127.0.0.1",
                            "--deny-domain", "nope.test"))
    urls = set(all_crawled_urls(tmp_path))
    assert site.base_url + "/ok.html" in urls
    assert not any("other.test" in u for u in urls)


def test_config_toml(tmp_path, site):
    cfg = tmp_path / "my.toml"
    cfg.write_text('delay = 0.01\nworkers = 2\nmax_depth = 4\n'
                   'deny_domains = ["evil.test"]\nverbose = true\n')
    args = botaghota.parse_args(
        ["--config", str(cfg), site.base_url + "/", "--workers", "3"])
    assert args.workers == 3          # CLI beats config
    assert args.max_depth == 4        # config beats built-in default
    assert args.deny_domains == ["evil.test"]
    assert args.verbose is True


def test_mini_toml_parser():
    cfg = botaghota.parse_config_text(
        '# comment\nworkers = 4\ndelay = 0.5\nbloom = true\n'
        'name = "x" # inline\nextract = ["a", "b"]\n[section]\nignored=1\n')
    assert cfg == {"workers": 4, "delay": 0.5, "bloom": True,
                   "name": "x", "extract": ["a", "b"]}


def test_ollama_plugin_is_safe_offline(tmp_path):
    # No Ollama server running: the plugin must answer fast and never raise.
    spec = os.path.join(os.path.dirname(botaghota.__file__),
                        "plugins", "ollama_topics.py")
    extractors = botaghota.load_extractors([spec])
    _name, fn = extractors[0]
    out = fn("http://x/", "<html>hello</html>")
    assert out == {"topic": "unknown"}
    for _ in range(3):
        out = fn("http://x/", "<html>hello</html>")
    assert out == {}   # gave up politely


def _second_site():
    """A second host (different port) to test cross-site scope."""

    class Handler2(SiteHandler):
        pages: ClassVar = {}
        robots = "User-agent: *\nAllow: /\n"
        sitemap = ""
        hits: ClassVar = Counter()

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler2)
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, daemon=True).start()
    Handler2.base_url = \
        f"http://127.0.0.1:{server.server_address[1]}"
    return Handler2, server


def test_scope_local_stays_on_seed_host(site, tmp_path):
    other, server = _second_site()
    try:
        other.pages["/x.html"] = page()
        site.pages["/"] = page(links=["/in.html", other.base_url + "/x.html"])
        site.pages["/in.html"] = page()
        run_crawler(tmp_path, [site.base_url + "/"], seconds=3.0,
                    extra_args=("--scope", "local"))
        urls = set(all_crawled_urls(tmp_path))
        assert site.base_url + "/in.html" in urls
        assert not any(u.startswith(other.base_url) for u in urls)
        assert "/x.html" not in other.hits   # never even fetched
    finally:
        server.shutdown()


def test_scope_global_follows_outlinks(site, tmp_path):
    other, server = _second_site()
    try:
        other.pages["/x.html"] = page()
        site.pages["/"] = page(links=["/in.html", other.base_url + "/x.html"])
        site.pages["/in.html"] = page()
        run_crawler(tmp_path, [site.base_url + "/"], seconds=3.0,
                    extra_args=("--scope", "global"))
        urls = set(all_crawled_urls(tmp_path))
        assert site.base_url + "/in.html" in urls
        assert other.base_url + "/x.html" in urls
    finally:
        server.shutdown()


def test_help_command(capsys):
    for word in ("help", "/help", "manual"):
        capsys.readouterr()
        assert botaghota.main([word]) == 0
        out = capsys.readouterr().out
        assert "CRAWL SCOPES" in out
        assert "--scope local" in out or "local" in out


def test_workers_flag_and_counts_file(site, tmp_path):
    args = botaghota.parse_args(["-w", "3", site.base_url + "/",
                                 "--data-dir", str(tmp_path)])
    assert args.workers == 3
    site.pages["/"] = page(links=["/a.html"])
    site.pages["/a.html"] = page()
    run_crawler(tmp_path, [site.base_url + "/"], seconds=2.5,
                extra_args=("-w", "2"))
    counts = (tmp_path / "counts.txt").read_text()
    assert "unique links     : 2" in counts
    assert "pages crawled    : 2" in counts
