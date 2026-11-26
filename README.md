# Leion — Delphi Evidence-Mining Agent

Agent for the Delphi Agent Arena: Evidence Mining competition (DoraHacks).
The agent reads live Delphi prediction markets, researches dated, sourced
public evidence that could move the forecaster, and submits that evidence
through the Delphi Evidence API.

Pure Python standard library. No third-party packages.

## Files

- `agent.py` — the agent. One pass: fetch markets, check limits and
  per-market entry state, research, then submit (live) or write the exact
  note plus links that would be submitted (dry-run).
- `state.json` — record of live submissions (market, note sha256, links,
  receipt). Used to never submit the same evidence twice to one market.
- `agent.log` — JSON-lines log of every decision: markets considered,
  skipped with reason, submitted with receipt id.
- `dryrun/` — dry-run output, one file per market address.
- `markets-snapshot.json` — the markets.json snapshot from the latest run.

## How to run

Dry-run first. Nothing is submitted:

```text
python3 agent.py --mode dry-run --max-markets 3
```

Dry-run one market only:

```text
python3 agent.py --mode dry-run --market 0x94dbba7c9b44b788a701602ab6d919ff7b7fc38a
```

Live mode. Only run this after a human has reviewed dry-run output:

```text
python3 agent.py --mode live --max-markets 3
```

## How research works

The agent searches Google News RSS first for each market, using queries
built from the market question and category (votes, official statistics,
controls, releases, niche communities). DuckDuckGo HTML is the fallback
when the feed returns fewer than two items. DuckDuckGo rate-limits
bursts from one IP with an empty results shell, so a failed search is
retried after a pause, and a request that asks for gzip gets an empty
shell from DuckDuckGo while the markets host needs gzip, so gzip is
only asked for when fetching markets.json. It fetches the top source
pages, keeps only sentences that are dated or numeric and on topic, and
builds the note from those sentences, each with its source domain
attached. Google News redirect links are kept as-is (they open the
article in a browser); their tracking query parameters are stripped.

If fewer than two usable facts from at least two distinct sources are
found, the market is skipped and the reason is logged. No speculation
and no fabricated facts are ever added.

## Limits enforced

- Max 3 submissions per market (checked against entry-state and state.json).
- Max 5 submissions per hour (checked against GET /v1/limits; the run
  stops when `submitsLeftThisHour` is 0).
- Daily judgment bounty cap 1000 USDC.e (reported by GET /v1/limits).
- Never submit after a market's `evidenceCutoffAt`.
- Never submit to a market whose judgment pot is empty.
- At most 3 items per submission in total (note + links + files), so
  the agent attaches at most 2 links alongside a note.

## Credentials

The Delphi API key lives in the registered connection `custom.delphi`
and is never written to any file. The agent imports
`dynamic_credentials.py` and attaches the surrogate exactly like the
connector helper at
`~/workspace/custom-connectors/custom.delphi/bin/delphi.py`. Every
request sends `X-Agent-Id: leion`, `X-Agent-Harness: muse-app`,
`X-Agent-Model: muse-spark-1.3` and a browser user agent, because
Cloudflare blocks the default Python-urllib user agent with error 1010.

## Competition facts

- Evidence API base: `https://delphi.fyi/v1`.
- Markets list: `https://competition.delphi.fyi/markets.json` (public).
- Prize qualification: at least 10 judged submissions across at least
  3 markets.
- Deadline: 11 Dec 2026 17:00 UTC.
