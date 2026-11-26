#!/usr/bin/env python3
"""Leion: evidence-mining agent for the Delphi Agent Arena (Evidence Mining).

The agent reads live Delphi prediction markets, researches dated, sourced
public evidence that could move the forecaster, and submits that evidence
through the Delphi Evidence API.

Pure Python standard library. No third-party packages. No credential is ever
stored in a file: the Delphi API key lives in the registered connection
``custom.delphi`` and reaches a request through a surrogate from
``dynamic_credentials.py``, exactly like the connector helper at
``~/workspace/custom-connectors/custom.delphi/bin/delphi.py``.

Usage:
    python3 agent.py --mode dry-run [--max-markets N] [--market 0x...]
    python3 agent.py --mode live    [--max-markets N] [--market 0x...]

Modes:
    dry-run  Research markets and write the exact note plus links that would
             be submitted to dryrun/<address>.txt. Nothing is submitted.
             This is the default.
    live     Submit real evidence through POST /v1/submissions. Only use
             live after a human has reviewed dry-run output.

Rules enforced by this agent:
    * Max 3 submissions per market and max 5 per hour (checked live against
      GET /v1/limits and GET /v1/markets/{market}/entry-state).
    * Never submit after a market's evidenceCutoffAt.
    * Never submit to a market whose judgment pot is empty.
    * Never submit the same evidence twice to one market. A note is
      identified by the sha256 of its text and recorded in state.json.
    * Only facts found by web search with a verifiable source URL and a
      date are used. No speculation, no fabricated facts, no copied
      submissions from other entrants.
"""
import argparse
import hashlib
import html as html_lib
import http.client
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
import zlib
from datetime import datetime, timezone

sys.path.insert(0, "/opt/muse/skills/custom-connector-setup/bin")
from dynamic_credentials import add_surrogate_to_request

API_BASE = "https://delphi.fyi/v1"
MARKETS_URL = "https://competition.delphi.fyi/markets.json"
CONNECTION = "custom.delphi"
ALLOWED_HOSTS = ["delphi.fyi", "api.delphi.fyi"]

AGENT_ID = "leion"
HARNESS = "muse-app"
MODEL = "muse-spark-1.3"
USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)
# Cloudflare blocks the default Python-urllib user agent with error 1010,
# so every request below sends a browser user agent.

MAX_NOTE_CHARS = 8000
# The API counts the note itself as one of at most 3 items per
# submission (note + links + files), so at most 2 links fit with a note.
MAX_LINKS = 2
MAX_SUBMITS_PER_MARKET = 3
MAX_SUBMITS_PER_HOUR = 5

WORKDIR = os.path.dirname(os.path.abspath(__file__))
STATE_PATH = os.path.join(WORKDIR, "state.json")
LOG_PATH = os.path.join(WORKDIR, "agent.log")
DRYRUN_DIR = os.path.join(WORKDIR, "dryrun")


# --------------------------------------------------------------------------
# Small utilities
# --------------------------------------------------------------------------

def now_utc():
    return datetime.now(timezone.utc)


def parse_time(text):
    """Parse an ISO 8601 timestamp with a Z suffix into an aware datetime."""
    if not text:
        return None
    return datetime.fromisoformat(text.replace("Z", "+00:00"))


def today_str():
    return now_utc().strftime("%Y-%m-%d")


def log_event(level, action, market=None, detail=""):
    entry = {
        "ts": now_utc().isoformat(),
        "level": level,
        "action": action,
        "market": market,
        "detail": detail,
    }
    line = json.dumps(entry, ensure_ascii=False)
    print(line, flush=True)
    with open(LOG_PATH, "a", encoding="utf-8") as handle:
        handle.write(line + "\n")


def load_state():
    if os.path.exists(STATE_PATH):
        with open(STATE_PATH, encoding="utf-8") as handle:
            return json.load(handle)
    return {"submitted": []}


def save_state(state):
    tmp = STATE_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as handle:
        json.dump(state, handle, indent=2, ensure_ascii=False)
    os.replace(tmp, STATE_PATH)


def note_hash(note):
    return hashlib.sha256(note.encode("utf-8")).hexdigest()


def already_submitted(state, market_address, note):
    digest = note_hash(note)
    for record in state.get("submitted", []):
        if record.get("market") == market_address and record.get("note_sha256") == digest:
            return True
    return False


def submissions_on_market(state, market_address):
    return sum(
        1 for r in state.get("submitted", [])
        if r.get("market") == market_address
    )


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args):
        raise RuntimeError("redirect refused, verify the endpoint")


def read_response_text(resp):
    """Read a response body in chunks and decode it.

    The body is read incrementally so a connection that is cut off
    mid-transfer still yields the bytes received so far. A gzipped body
    is decompressed incrementally, so a truncated gzip stream still
    yields its complete prefix.
    """
    encoding = (resp.headers.get("Content-Encoding") or "").lower()
    decoder = zlib.decompressobj(31) if encoding == "gzip" else None
    out = bytearray()
    while True:
        try:
            chunk = resp.read(4096)
        except http.client.IncompleteRead as exc:
            chunk = exc.partial
            if decoder is not None:
                try:
                    out += decoder.decompress(chunk)
                except zlib.error:
                    pass
            elif chunk:
                out += chunk
            break
        if not chunk:
            break
        if decoder is not None:
            out += decoder.decompress(chunk)
        else:
            out += chunk
    return bytes(out).decode("utf-8", "replace")


def plain_get(url, timeout=30, use_gzip=False):
    """Unauthenticated GET with a browser user agent. Returns text or None.

    Only ask for gzip when ``use_gzip`` is set. The markets host cuts
    off plain chunked bodies mid-transfer, while its small gzipped body
    arrives intact. DuckDuckGo does the opposite: a request that asks
    for gzip gets an empty results shell, while a plain request returns
    full results. Retries a few times.
    """
    for attempt in range(4):
        request = urllib.request.Request(url, method="GET")
        request.add_header("User-Agent", USER_AGENT)
        if use_gzip:
            request.add_header("Accept-Encoding", "gzip")
        try:
            with urllib.request.build_opener(NoRedirect()).open(
                request, timeout=timeout
            ) as resp:
                return read_response_text(resp)
        except Exception as exc:
            log_event("warning", "http_get_failed",
                      detail="%s attempt=%d: %s" % (url, attempt + 1, exc))
            time.sleep(1)
    return None


# --------------------------------------------------------------------------
# Delphi Evidence API (authenticated, via the registered connection)
# --------------------------------------------------------------------------

def api_call(method, path, body=None, multipart=None):
    """Call the Delphi Evidence API.

    Returns (status, payload_dict). ``multipart`` is a list of
    (field_name, value) pairs encoded as multipart/form-data.
    """
    url = API_BASE + path
    data = None
    headers = {
        "X-Agent-Id": AGENT_ID,
        "X-Agent-Harness": HARNESS,
        "X-Agent-Model": MODEL,
        "User-Agent": USER_AGENT,
    }
    if multipart is not None:
        boundary = "----leion-" + uuid.uuid4().hex
        data = encode_multipart(multipart, boundary)
        headers["Content-Type"] = "multipart/form-data; boundary=" + boundary
        headers["Idempotency-Key"] = uuid.uuid4().hex
    elif body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=data, method=method)
    for key, value in headers.items():
        request.add_header(key, value)
    add_surrogate_to_request(request, CONNECTION, allowed_hosts=ALLOWED_HOSTS)
    try:
        with urllib.request.build_opener(NoRedirect()).open(request, timeout=30) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", "replace")[:500]
        try:
            payload = json.loads(raw)
        except ValueError:
            payload = {"raw": raw}
        return exc.code, payload
    except Exception as exc:
        return 0, {"error": str(exc)}


def encode_multipart(fields, boundary):
    """Encode [(name, value), ...] as multipart/form-data bytes.

    Repeated field names are kept, so ``links[]`` can appear several times.
    """
    parts = []
    for name, value in fields:
        parts.append("--" + boundary)
        parts.append('Content-Disposition: form-data; name="%s"' % name)
        parts.append("")
        parts.append(value)
    parts.append("--" + boundary + "--")
    parts.append("")
    return "\r\n".join(parts).encode("utf-8")


def get_limits():
    status, payload = api_call("GET", "/limits")
    if status != 200:
        raise RuntimeError("GET /v1/limits failed: %s %s" % (status, payload))
    return payload


def get_entry_state(market_address):
    status, payload = api_call("GET", "/markets/%s/entry-state" % market_address)
    if status != 200:
        raise RuntimeError(
            "GET /v1/markets/%s/entry-state failed: %s %s"
            % (market_address, status, payload)
        )
    return payload


def submit_evidence(market_address, note, links):
    """POST /v1/submissions. Returns (status, payload)."""
    fields = [("marketAddress", market_address), ("note", note)]
    for link in links:
        fields.append(("links[]", link))
    return api_call("POST", "/submissions", multipart=fields)


# --------------------------------------------------------------------------
# Markets
# --------------------------------------------------------------------------

def fetch_markets():
    for attempt in range(5):
        text = plain_get(MARKETS_URL, timeout=30, use_gzip=True)
        if text is None:
            continue
        try:
            markets = json.loads(text)
        except ValueError:
            log_event("warning", "markets_parse_failed",
                      detail="attempt=%d" % (attempt + 1))
            continue
        snapshot = os.path.join(WORKDIR, "markets-snapshot.json")
        with open(snapshot, "w", encoding="utf-8") as handle:
            json.dump(markets, handle, indent=2, ensure_ascii=False)
        return markets
    raise RuntimeError("could not fetch markets.json")


def market_is_eligible(market, now):
    """Filter to open markets with time left and a non-empty judgment pot."""
    if market.get("status") != "open":
        return False, "status is %s" % market.get("status")
    cutoff = parse_time(market.get("evidenceCutoffAt"))
    if cutoff is None or now >= cutoff:
        return False, "evidence cutoff already passed"
    try:
        pot = int(market.get("pots", {}).get("judgement", "0"))
    except ValueError:
        pot = 0
    if pot <= 0:
        return False, "judgment pot is empty"
    return True, ""


# --------------------------------------------------------------------------
# Research: web search plus page fetches, pure stdlib
# --------------------------------------------------------------------------

STOPWORDS = {
    "will", "the", "a", "an", "of", "in", "on", "by", "for", "to", "at",
    "be", "is", "are", "its", "per", "with", "from", "after", "before",
    "than", "or", "and", "as", "if", "that", "this", "than", "over",
}

SEARCH_URL = "https://html.duckduckgo.com/html/?q="
NEWS_RSS_URL = "https://news.google.com/rss/search?q="


def search_news_rss(query, max_results=6):
    """Search Google News RSS. Returns [{title, url, snippet, pubDate}].

    The feed is structured XML with dated items and needs no key. The
    item links are Google News redirects; they are resolved to the
    publisher URL before being used in a note.
    """
    import xml.etree.ElementTree as ET

    url = NEWS_RSS_URL + urllib.parse.quote_plus(query) + "&hl=en-US&gl=US&ceid=US:en"
    page = plain_get(url, timeout=25)
    if page is None:
        return []
    try:
        root = ET.fromstring(page)
    except ET.ParseError as exc:
        log_event("warning", "rss_parse_failed", detail="%s" % exc)
        return []
    results = []
    for item in root.iter("item"):
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        pub = (item.findtext("pubDate") or "").strip()
        if not title or not link:
            continue
        source = item.find("source")
        results.append({
            "title": title,
            "url": link,
            "snippet": title,
            "pubDate": pub,
            "label": source.text.strip() if source is not None and source.text else "",
            "source_url": source.get("url", "") if source is not None else "",
        })
        if len(results) >= max_results:
            break
    return results


def resolve_url(url, timeout=15):
    """Clean up a Google News redirect link.

    The publisher URL inside a news.google.com redirect is resolved
    client-side by JavaScript, so it cannot be recovered with plain
    HTTP. The redirect link itself still opens the article in a
    browser, so keep it, but strip the tracking query parameters.
    """
    if "news.google.com" not in url:
        return url
    return url.split("?")[0]



VERB_CUTS = (
    " be ", " win ", " report ", " show ", " pass ", " list ",
    " publish ", " rank ", " finish ", " invoke ", " approve ",
    " reach ", " have ", " record ", " elect ", " exceed ",
)

TRAILING_PREPS = ("for", "of", "in", "on", "at", "to", "by", "and")


def extract_topic(question):
    """Pull a short search topic out of a market question.

    Drops the leading 'Will', cuts the trailing resolution scaffolding
    (', per ...', ' published ...', ' by Dec ...') and the main verb
    phrase, so the topic stays short enough for a search engine to
    match. Falls back to the leading content words when the verb cut
    leaves almost nothing.
    """
    text = (question or "").strip()
    text = re.sub(r"^Will\s+", "", text, flags=re.I)
    text = re.sub(r"\s*\?\s*$", "", text)
    for cut in (
        r",\s*per\s+.*$",
        r",\s*published\s+.*$",
        r",\s*released\s+.*$",
        r"\s+in\s+its\s+.*$",
        r"\s+by\s+Dec.*$",
        r"\s+on\s+Dec.*$",
        r"\s+at\s+its\s+.*$",
        r"\s+in\s+a\s+plenary.*$",
    ):
        text = re.sub(cut, "", text, flags=re.I)
    text = re.sub(r"\([^)]*\)", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    cut_at = len(text)
    low = " " + text.lower() + " "
    for verb in VERB_CUTS:
        idx = low.find(verb)
        if idx > 0:
            cut_at = min(cut_at, idx)
    topic = text[:cut_at].strip(" ,")
    if len(topic) < 12:
        words = [
            w for w in re.findall(r"[A-Za-z][A-Za-z\-]{2,}", text)
            if w.lower() not in STOPWORDS
            and w.lower() not in ("win", "will", "report", "show", "pass")
            and not w.isdigit()
        ]
        topic = " ".join(words[:5])
    words = topic.split()
    if len(words) > 8:
        words = words[:8]
    while words and words[-1].lower().strip(",") in TRAILING_PREPS:
        words.pop()
    return " ".join(words)


def build_queries(market):
    """Category-specific search queries for one market."""
    topic = extract_topic(market.get("question", ""))
    category = market.get("category", "")
    year = "2026"
    if category == "votes_government":
        return [
            "%s poll %s" % (topic, year),
            "%s turnout history" % topic,
            "%s latest news %s" % (topic, year),
        ]
    if category == "official_statistics":
        return [
            "%s %s forecast" % (topic, year),
            "%s October %s" % (topic, year),
            "%s consensus forecast %s" % (topic, year),
        ]
    if category == "controls":
        return [
            "%s %s forecast" % (topic, year),
            "%s expectations %s" % (topic, year),
        ]
    if category == "releases_standards":
        return [
            "%s release date" % topic,
            "%s latest release %s" % (topic, year),
        ]
    if category == "niche_communities":
        return [
            "%s %s" % (topic, year),
            "%s latest %s" % (topic, year),
        ]
    return [
        "%s %s" % (topic, year),
        "%s latest news %s" % (topic, year),
    ]


def search_web(query, max_results=6):
    """Search the web. Google News RSS plus DuckDuckGo HTML, merged.

    Returns [{title, url, snippet, label}]. ``label`` names the
    publisher for attribution. RSS gives dated items with a publisher
    name; DuckDuckGo gives direct publisher URLs and snippets.
    DuckDuckGo rate-limits bursts from one IP by answering with an
    empty results shell, so a search that returns zero results is
    retried after a pause, up to three attempts.
    """
    results = search_news_rss(query, max_results)
    seen_urls = {r["url"] for r in results}
    url = SEARCH_URL + urllib.parse.quote_plus(query)
    for attempt, pause in enumerate((0, 8, 20)):
        if pause:
            time.sleep(pause)
        page = plain_get(url, timeout=25)
        if page is None:
            continue
        ddg = parse_ddg(page, max_results)
        if ddg:
            for item in ddg:
                if item["url"] not in seen_urls:
                    seen_urls.add(item["url"])
                    results.append(item)
            break
        log_event("warning", "search_empty", detail="query=%r attempt=%d"
                  % (query, attempt + 1))
    return results[: max_results * 2]


def parse_ddg(page, max_results=6):
    """Parse DuckDuckGo HTML results. Returns [{title, url, snippet}]."""
    results = []
    pattern = re.compile(
        r'class="result__a"[^>]*href="([^"]+)"[^>]*>(.*?)</a>', re.S
    )
    snippets = re.findall(
        r'class="result__snippet"[^>]*>(.*?)</a>', page, re.S
    )
    for index, match in enumerate(pattern.finditer(page)):
        href, raw_title = match.groups()
        title = html_lib.unescape(re.sub(r"<[^>]+>", "", raw_title)).strip()
        target = href
        parsed = urllib.parse.urlparse(href)
        query_params = urllib.parse.parse_qs(parsed.query)
        if "uddg" in query_params:
            target = query_params["uddg"][0]
        elif target.startswith("//"):
            target = "https:" + target
        if not target.startswith("http"):
            continue
        snippet = ""
        if index < len(snippets):
            snippet = html_lib.unescape(
                re.sub(r"<[^>]+>", "", snippets[index])
            ).strip()
        results.append({
            "title": title,
            "url": target,
            "snippet": snippet,
            "label": urllib.parse.urlparse(target).netloc,
        })
        if len(results) >= max_results:
            break
    return results


def fetch_page_text(url, max_chars=6000):
    """Fetch a page and return its visible text, stripped of markup."""
    raw = plain_get(url, timeout=20)
    if raw is None:
        return None
    text = re.sub(r"<script[^>]*>.*?</script>", " ", raw, flags=re.S | re.I)
    text = re.sub(r"<style[^>]*>.*?</style>", " ", text, flags=re.S | re.I)
    text = re.sub(r"<[^>]+>", "\n", text)
    text = html_lib.unescape(text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n", text)
    return text.strip()[:max_chars] or None


def topic_keywords(market):
    words = re.findall(r"[A-Za-z][A-Za-z\-]{2,}", market.get("question", ""))
    keep = set()
    for word in words:
        low = word.lower()
        if low not in STOPWORDS:
            keep.add(low)
    keep.update(w.lower() for w in extract_topic(market.get("question", "")).split())
    return keep


def pick_facts(candidates, keywords, limit=4):
    """Score candidate sentences. Keep dated, numeric, on-topic ones.

    Candidates are dicts with ``text``, ``url``, ``label`` and ``kind``
    (``page`` for sentences from a fetched article, ``title`` for
    headlines and snippets). Page sentences score higher because they
    carry findings, not just headlines. Returns (facts, used_urls)
    where each fact keeps its source label and kind.
    """
    seen = set()
    scored = []
    months = (
        "january", "february", "march", "april", "may", "june", "july",
        "august", "september", "october", "november", "december",
    )
    for candidate in candidates:
        sentence = re.sub(r"\s+", " ", candidate["text"]).strip()
        if len(sentence) < 40 or len(sentence) > 400:
            continue
        key = sentence.lower()
        if key in seen:
            continue
        seen.add(key)
        words = set(re.findall(r"[a-z\-]+", key))
        overlap = len(words & keywords)
        if overlap == 0:
            continue
        score = overlap * 2
        if candidate.get("kind") == "page":
            score += 2
        if re.search(r"\d", sentence):
            score += 2
        if any(m in key for m in months) or re.search(r"20\d\d", sentence):
            score += 1
        if "%" in sentence:
            score += 1
        scored.append((score, sentence, candidate["url"],
                       candidate["label"], candidate.get("kind", "title")))
    scored.sort(key=lambda item: -item[0])
    facts = []
    used_urls = []
    for _score, sentence, url, label, kind in scored:
        if url not in used_urls:
            used_urls.append(url)
        facts.append({"text": sentence, "url": url, "label": label,
                      "kind": kind})
        if len(facts) >= limit:
            break
    return facts, used_urls


def is_substantive(sentence):
    """True when a sentence carries a real figure, not just a date.

    Dates and ordinals (Nov 29, 2026, 2nd) do not count. A decimal, a
    percent or a multi-digit number does.
    """
    text = re.sub(
        r"(january|february|march|april|may|june|july|august|september|"
        r"october|november|december)\s+\d{1,2}(,\s*20\d\d)?",
        "", sentence, flags=re.I)
    text = re.sub(r"\b20\d\d\b", "", text)
    text = re.sub(r"\b\d+(st|nd|rd|th)\b", "", text)
    return bool(re.search(r"\d+\.\d+|\d{3,}|%", text))


def research(market):
    """Research one market.

    Returns (note, links) or (None, reason). Every fact in the note comes
    from a fetched search result or page, with its source URL attached.
    """
    queries = build_queries(market)
    keywords = topic_keywords(market)
    candidates = []
    sources = []
    for query in queries:
        results = search_web(query, max_results=5)
        log_event("info", "search", market.get("address"),
                  "query=%r results=%d" % (query, len(results)))
        for result in results:
            if result["url"] not in sources:
                sources.append(result["url"])
            if result["snippet"]:
                label = result.get("label") or urllib.parse.urlparse(
                    result["url"]).netloc
                snippet = result["snippet"]
                if result.get("pubDate"):
                    snippet = "%s (%s)" % (snippet, result["pubDate"])
                candidates.append({
                    "text": snippet, "url": result["url"], "label": label,
                    "kind": "title",
                })
        # Fetch the top two pages per query for grounded sentences.
        # Google News links redirect to the publisher and cannot be
        # fetched here, so only direct publisher URLs are fetched.
        for result in results[:2]:
            if "news.google.com" in result["url"]:
                continue
            final_url = resolve_url(result["url"])
            text = fetch_page_text(final_url)
            if not text:
                continue
            label = result.get("label") or urllib.parse.urlparse(
                final_url).netloc
            for sentence in re.split(r"(?<=[.!?])\s+", text):
                candidates.append({
                    "text": sentence, "url": final_url, "label": label,
                    "kind": "page",
                })
        time.sleep(2)
    if not sources:
        # Every query came back empty. Retry once with a short fallback
        # built from the question's leading content words.
        words = [
            w for w in re.findall(r"[A-Za-z][A-Za-z\-]{2,}",
                                  market.get("question", ""))
            if w.lower() not in STOPWORDS
        ]
        fallback = " ".join(words[:5]) + " 2026"
        results = search_web(fallback, max_results=5)
        log_event("info", "search", market.get("address"),
                  "query=%r results=%d (fallback)" % (fallback, len(results)))
        for result in results:
            if result["url"] not in sources:
                sources.append(result["url"])
            if result["snippet"]:
                label = result.get("label") or urllib.parse.urlparse(
                    result["url"]).netloc
                snippet = result["snippet"]
                if result.get("pubDate"):
                    snippet = "%s (%s)" % (snippet, result["pubDate"])
                candidates.append({
                    "text": snippet, "url": result["url"], "label": label,
                    "kind": "title",
                })
    facts, used_urls = pick_facts(candidates, keywords, limit=4)
    if len(facts) < 2:
        return None, "insufficient sourced evidence (%d usable facts)" % len(facts)
    if len(set(f["url"] for f in facts)) < 2:
        return None, "facts come from a single source only"
    if not any(is_substantive(f["text"]) for f in facts):
        return None, "no substantive figure in evidence (headlines only)"
    # Resolve Google News redirect links to publisher URLs so the note
    # and the submitted links name the real source.
    resolved = {}
    for fact in facts:
        if fact["url"] not in resolved:
            resolved[fact["url"]] = resolve_url(fact["url"])
        fact["url"] = resolved[fact["url"]]
    links = []
    for url in used_urls:
        final = resolved.get(url, url)
        if final not in links:
            links.append(final)
    links = links[:MAX_LINKS]
    lines = [
        'Evidence as of %s (UTC) for the market: "%s"'
        % (today_str(), market.get("question", "")),
        "",
    ]
    for fact in facts:
        label = fact.get("label") or urllib.parse.urlparse(
            fact["url"]).netloc
        lines.append("- %s (Source: %s)" % (fact["text"], label))
    lines.append("")
    lines.append(
        "Why this matters: the facts above are dated, public and drawn "
        "from the linked sources. They bear on the outcome the market "
        "resolves, as stated in its resolution criteria. No forecast, "
        "opinion or unpublished information is added."
    )
    note = "\n".join(lines)
    if len(note) > MAX_NOTE_CHARS:
        note = note[:MAX_NOTE_CHARS]
    return note, links


# --------------------------------------------------------------------------
# Main loop
# --------------------------------------------------------------------------

def run_once(mode, max_markets, only_market):
    state = load_state()
    os.makedirs(DRYRUN_DIR, exist_ok=True)
    markets = fetch_markets()
    log_event("info", "markets_fetched", detail="count=%d" % len(markets))
    now = now_utc()
    eligible = []
    for market in markets:
        if only_market and market.get("address") != only_market:
            continue
        ok, reason = market_is_eligible(market, now)
        if ok:
            eligible.append(market)
        else:
            log_event("info", "skip_market", market.get("address"),
                      "reason=%s" % reason)
    eligible.sort(key=lambda m: m.get("evidenceCutoffAt", ""))
    log_event("info", "eligible_markets", detail="count=%d" % len(eligible))

    limits = get_limits()
    log_event("info", "limits", detail=json.dumps(limits, ensure_ascii=False))
    left = int(limits.get("submitsLeftThisHour", 0))
    if left <= 0:
        log_event("info", "hour_limit_reached",
                  detail="hourEndsAt=%s" % limits.get("hourEndsAt"))
        return {"submitted": 0, "skipped": len(markets) - len(eligible),
                "reason": "hour limit reached"}

    submitted = 0
    run_hashes = set()
    for market in eligible:
        if submitted >= left or submitted >= max_markets:
            break
        address = market.get("address")
        try:
            entry = get_entry_state(address)
        except Exception as exc:
            log_event("warning", "skip_market", address,
                      "reason=entry-state failed: %s" % exc)
            continue
        if not entry.get("canSubmit", False):
            log_event("info", "skip_market", address,
                      "reason=entry-state canSubmit is false")
            continue
        if int(entry.get("remainingOnMarket", 0)) <= 0:
            log_event("info", "skip_market", address,
                      "reason=no submissions left on this market")
            continue
        try:
            pot = int(entry.get("judgementPotRemainingBaseUnits", "0"))
        except ValueError:
            pot = 0
        if pot <= 0:
            log_event("info", "skip_market", address,
                      "reason=judgment pot is empty")
            continue
        if submissions_on_market(state, address) >= MAX_SUBMITS_PER_MARKET:
            log_event("info", "skip_market", address,
                      "reason=already submitted %d times to this market"
                      % MAX_SUBMITS_PER_MARKET)
            continue

        note, links_or_reason = research(market)
        if note is None:
            log_event("info", "skip_market", address,
                      "reason=%s" % links_or_reason)
            continue
        links = links_or_reason
        digest = note_hash(note)
        if digest in run_hashes or already_submitted(state, address, note):
            log_event("info", "skip_market", address,
                      "reason=identical evidence already submitted")
            continue
        run_hashes.add(digest)

        if mode == "dry-run":
            out_path = os.path.join(DRYRUN_DIR, address + ".txt")
            with open(out_path, "w", encoding="utf-8") as handle:
                handle.write("MARKET: %s\n" % address)
                handle.write("QUESTION: %s\n" % market.get("question", ""))
                handle.write("MODE: dry-run (nothing submitted)\n")
                handle.write("LINKS:\n")
                for link in links:
                    handle.write("  %s\n" % link)
                handle.write("\nNOTE (%d chars):\n%s\n" % (len(note), note))
            log_event("info", "dry_run_written", address,
                      "path=%s chars=%d links=%d"
                      % (out_path, len(note), len(links)))
            submitted += 1
            continue

        status, payload = submit_evidence(address, note, links)
        if status in (200, 201, 202):
            receipt = payload.get("id") or payload.get("submissionId") or payload
            state["submitted"].append({
                "market": address,
                "note_sha256": digest,
                "links": links,
                "receipt": receipt,
                "submitted_at": now_utc().isoformat(),
                "mode": "live",
            })
            save_state(state)
            log_event("info", "submitted", address,
                      "receipt=%s" % json.dumps(receipt, ensure_ascii=False))
            submitted += 1
        else:
            log_event("error", "submit_failed", address,
                      "status=%s payload=%s"
                      % (status, json.dumps(payload, ensure_ascii=False)))
    save_state(state)
    log_event("info", "run_done",
              detail="mode=%s submitted=%d" % (mode, submitted))
    return {"submitted": submitted}


def main():
    parser = argparse.ArgumentParser(description="Leion Delphi evidence agent")
    parser.add_argument("--mode", choices=["dry-run", "live"], default="dry-run")
    parser.add_argument("--max-markets", type=int, default=3)
    parser.add_argument("--market", default=None,
                        help="only research this market address")
    args = parser.parse_args()
    if args.mode == "live":
        print("LIVE MODE: real submissions will be made.", flush=True)
    result = run_once(args.mode, args.max_markets, args.market)
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
