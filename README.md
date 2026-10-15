# botaghota

A polite, crash-safe, dependency-free web crawler and data-discovery engine.

`botaghota` crawls the web forever without losing a link: it honors
`robots.txt` and crawl-delay, backs off on 429/5xx, keeps every byte of
state in SQLite (atomic, `kill -9`-proof), and stays under ~150 MB of RAM
no matter how long it runs.

![demo](docs/demo.gif)

## Quickstart (< 60 seconds)

```bash
git clone https://github.com/programmermunna/botaghota
cd botaghota
python botaghota.py https://example.com/
```

That's it — no dependencies, Python 3.10+ only. Links appear in
`data/crawled_links/YYYY-MM-DD.txt`; state lives in `data/`.

Or install it:

```bash
pip install .
botaghota https://example.com/
```

## What it does

- **Two crawl scopes** — `--scope local` (default) maps one website fully:
  every unique link on the seed's own host is followed, nothing leaves the
  site. `--scope global` is the classic mode: outlinks to any website are
  followed, bounded by politeness, allow/deny lists, depth and domain caps.
- **Polite by design** — robots.txt caching, per-host rate limiting,
  exponential backoff on 429/5xx, honest `botaghota/x.y` User-Agent.
  Never bypasses logins, paywalls, CAPTCHAs, or robots rules.
- **Crash-safe** — every page claim is a SQLite transaction; an `inflight`
  table is recovered at startup, so `kill -9` or power loss costs zero
  links and produces zero duplicates.
- **Bounded memory** — the frontier, dedup set, and all caches live on
  disk. Optional `--bloom` mode handles 100M+ URLs in a fixed-size mmap.
- **Concurrent** — `--workers N` asyncio workers with per-host token
  buckets; a fast host never starves a slow one.
- **Smart frontier** — new domains get priority, depth limits and
  per-domain caps bound the crawl, stale pages recrawl on a freshness
  interval, and `sitemap.xml` is discovered automatically.
- **Structured output (opt-in)** — `--extract` plugins emit title,
  meta description, language, status and content hash to daily `.jsonl`.

## Usage

```
python botaghota.py <seed> [<seed>...] [options]
python botaghota.py help          # full user manual (also: /help)

  --scope local|global local = seed's own site only, every unique link
  -l, --local          (default); global = follow outlinks to any site
  -g, --global         aliases for --scope local / --scope global
  --data-dir DIR       state + output directory (default: data)
  --workers N          concurrent fetch workers (default: 1)
  --delay SEC          per-host delay between requests (default: 1.0)
  --max-depth N        frontier depth limit (default: 6)
  --domain-cap N       max pages per registrable domain
  --limit N            stop after N pages this run
  --recrawl-hours H    recrawl pages older than H hours (default: 24)
  --no-sitemap         skip sitemap.xml discovery
  --bloom              mmap Bloom-filter dedup for very large crawls
  --extract [PLUGIN]   page metadata -> data/extracted/*.jsonl
  --stats              print stats for the data dir and exit
  --dashboard [PORT]   serve a live dashboard on 127.0.0.1 (default 8080)
  --config FILE        read defaults from a TOML file (see config.example.toml)
  --allow-domain D     only crawl these domains (repeatable)
  --deny-domain D      never crawl these domains (repeatable)
  -v, --verbose        debug logging
```

Stop it anytime with Ctrl-C — it finishes the in-flight page and
checkpoints. A second Ctrl-C exits immediately; nothing is lost either way.

## Extract plugins

`--extract` with no argument writes the built-in record
(url, title, meta description, language, status, content hash).
Write your own:

```python
# plugins/topics.py
def extract(url: str, html: str) -> dict:
    return {"topic": "cats" if "cat" in html.lower() else "other"}
```

```bash
python botaghota.py https://example.com --extract topics --extract my_mod
```

Results land in `data/extracted/YYYY-MM-DD.jsonl`. An offline
Ollama-based topic classifier ships as an opt-in example in
`plugins/ollama_topics.py`.

## Running it forever

- **Docker**: `docker build -t botaghota . && docker run -v botaghota-data:/data botaghota https://example.com/`
- **systemd**: see `deploy/botaghota.service`
- **Windows**: see `docs/windows-startup.md`
- **Config**: copy `config.example.toml` to `config.toml`, run with `--config config.toml`

## Development

```bash
pip install .[dev]
ruff check botaghota.py tests/
python -m pytest tests/ -x -q
```

See [CONTRIBUTING.md](CONTRIBUTING.md). MIT licensed — see [LICENSE](LICENSE).
