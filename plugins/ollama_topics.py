"""Opt-in --extract plugin: tag each page with a topic from a local Ollama.

100% offline and free: talks to an Ollama server on localhost only.
Enable it with:

    ollama pull llama3.2            # once, any model works
    python botaghota.py <seed> --extract ollama_topics

Environment knobs:
    BOTAGHOTA_OLLAMA_URL     default http://127.0.0.1:11434
    BOTAGHOTA_OLLAMA_MODEL   default llama3.2
    BOTAGHOTA_OLLAMA_TIMEOUT seconds, default 10

If Ollama is unreachable the plugin gives up after a few failures and
returns {} so it never slows or crashes a crawl.
"""

import json
import os
import urllib.request

_URL = os.environ.get("BOTAGHOTA_OLLAMA_URL", "http://127.0.0.1:11434")
_MODEL = os.environ.get("BOTAGHOTA_OLLAMA_MODEL", "llama3.2")
_TIMEOUT = float(os.environ.get("BOTAGHOTA_OLLAMA_TIMEOUT", "10"))

_TOPICS = ("news", "docs", "forum", "shop", "blog", "code", "media", "other")
_PROMPT = (
    "Classify the main topic of this web page into exactly one of: "
    + ", ".join(_TOPICS)
    + ". Reply with the single word only.\n\n"
)

_failures = 0
_GIVE_UP_AFTER = 3


def _ask(prompt: str) -> str:
    body = json.dumps({
        "model": _MODEL,
        "prompt": prompt,
        "stream": False,
        "options": {"num_predict": 8, "temperature": 0},
    }).encode()
    req = urllib.request.Request(
        f"{_URL}/api/generate", data=body,
        headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=_TIMEOUT) as resp:
        return json.loads(resp.read()).get("response", "")


def extract(url: str, html: str) -> dict:
    global _failures
    if _failures >= _GIVE_UP_AFTER:
        return {}
    try:
        # A small text sample is enough; never send megabytes to the model.
        answer = _ask(_PROMPT + html[:2000]).strip().lower().split()
        topic = next((w for w in answer if w in _TOPICS), "other")
        _failures = 0
        return {"topic": topic}
    except Exception:  # noqa: BLE001 - offline LLM trouble must not break a crawl
        _failures += 1
        return {"topic": "unknown"} if _failures < _GIVE_UP_AFTER else {}
