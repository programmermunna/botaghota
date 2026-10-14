"""URL cleaning and trap-guard unit tests."""

import botaghota
from botaghota import clean_url, looks_like_trap, url_hash


def test_clean_url_basic():
    assert clean_url("HTTP://EXAMPLE.COM/Path") == "http://example.com/Path"
    assert clean_url("https://example.com") == "https://example.com/"


def test_clean_url_strips_fragment():
    assert clean_url("http://example.com/a#sec") == "http://example.com/a"


def test_clean_url_strips_default_port():
    assert clean_url("http://example.com:80/a") == "http://example.com/a"
    assert clean_url("https://example.com:443/a") == "https://example.com/a"
    assert clean_url("http://example.com:8080/a") == \
        "http://example.com:8080/a"


def test_clean_url_rejects_non_http():
    assert clean_url("ftp://example.com/x") is None
    assert clean_url("javascript:void(0)") is None
    assert clean_url("mailto:a@b.c") is None
    assert clean_url("") is None
    assert clean_url("   ") is None


def test_clean_url_rejects_overlong():
    assert clean_url("http://example.com/" + "x" * 3000) is None


def test_url_hash_stable_and_64bit():
    h = url_hash("http://example.com/")
    assert isinstance(h, int)
    assert -(2 ** 63) <= h < 2 ** 63
    assert h == url_hash("http://example.com/")


def test_trap_guard_repeated_segments():
    assert looks_like_trap("http://x/a/a/a/a/", 10, 0)
    assert not looks_like_trap("http://x/a/b/a/b/", 10, 0)


def test_trap_guard_depth():
    assert looks_like_trap("http://x/a", max_depth=3, depth=4)
    assert not looks_like_trap("http://x/a", max_depth=3, depth=3)


def test_trap_guard_query_blowup():
    url = "http://x/?" + "&".join(f"p{i}=1" for i in range(12))
    assert looks_like_trap(url, 10, 0)


def test_extract_links_resolves_relative():
    html = '<a href="/b">x</a><a href="c.html">y</a><a href="#f">z</a>'
    links = botaghota.extract_links(html, "http://example.com/a/")
    assert "http://example.com/b" in links
    assert "http://example.com/a/c.html" in links


def test_parse_sitemap_urlset():
    xml = (
        '<?xml version="1.0"?>'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
        "<url><loc>http://x/a</loc><lastmod>2026-01-01</lastmod></url>"
        "<url><loc>http://x/b</loc></url>"
        "</urlset>").encode()
    pages, children = botaghota.parse_sitemap(xml)
    assert pages == ["http://x/a", "http://x/b"]
    assert children == []


def test_parse_sitemap_index():
    xml = (
        '<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
        "<sitemap><loc>http://x/sm1.xml</loc></sitemap>"
        "</sitemapindex>").encode()
    pages, children = botaghota.parse_sitemap(xml)
    assert pages == []
    assert children == ["http://x/sm1.xml"]


def test_parse_sitemap_garbage():
    assert botaghota.parse_sitemap(b"not xml") == ([], [])
