# Contributing to botaghota

Thanks for helping make the politest crawler on the internet a little better.

## Ground rules (non-negotiable)

1. **Core stays dependency-free.** `python botaghota.py <seed>` must work on a
   bare Python 3.10+ install. Optional features go behind extras
   (`pip install .[extra]`) or `--extract` plugins.
2. **Never lose data.** All state changes are atomic: SQLite transactions,
   `os.replace`, fsync. If a change can drop or duplicate a URL on `kill -9`,
   it will not merge.
3. **Bounded memory.** No unbounded in-memory sets/lists. Frontier, dedup and
   caches are disk-backed (SQLite / bloom mmap).
4. **Stay polite.** Honor robots.txt and crawl-delay, per-host rate limits,
   exponential backoff on 429/5xx, and an honest User-Agent. Never bypass
   logins, paywalls, CAPTCHAs, or robots rules.
5. **Never crash on input.** Every network/parse error is caught, logged,
   counted, and skipped.

## Workflow

- Fork, branch from `main`, keep commits small and messages descriptive.
- Add or update tests in `tests/` — the suite spins up a local `http.server`,
  no network needed.
- Before opening a PR, run:

  ```bash
  ruff check botaghota.py tests/
  python -m pytest tests/ -x -q
  ```

- CI runs the same checks on Python 3.10–3.13.
- If you add a dependency, justify it in the PR description and put it behind
  an optional extra — never in core.

## Writing an `--extract` plugin

A plugin is any module exposing `def extract(url: str, html: str) -> dict`.
Drop it in `plugins/<name>.py` or pass a file path:

```python
# plugins/word_count.py
def extract(url, html):
    return {"words": len(html.split())}
```

```bash
python botaghota.py https://example.com --extract word_count
```

Output lands in `data/extracted/YYYY-MM-DD.jsonl`.
