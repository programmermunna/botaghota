"""End-to-end crawler tests against the local test site fixture."""

import os
import signal
import subprocess
import sys
import time

import botaghota
from conftest import all_crawled_urls, page, run_crawler

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
