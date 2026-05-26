---
name: gottem
description: Use when fetching or scraping the contents of a web page or URL, especially when a plain HTTP request returns a block page, a CAPTCHA, a Cloudflare challenge, an empty shell, or a 403/429 — gottem routes the request through a tiered ladder of scraping vendors (and a local browser), cheapest-first, escalating until it gets clean content. Also use to compare scraping vendors, force a specific provider, or race/hedge providers for latency.
---

# gottem — universal scraper

gottem is one CLI and one Rust library that fetches a URL through a tiered ladder
of scraping vendors plus a local browser. It tries the cheapest route first,
escalates on failure, and stops when it gets clean content. Adding a vendor is a
TOML row, not a code change. Powered by [spider](https://github.com/spider-rs/spider).

Reach for gottem instead of `curl`/`fetch` when the target is bot-protected,
JS-rendered, geo-blocked, or rate-limited — i.e. when a naive request would come
back as a challenge page, an empty body, or an error status.

## Install

```sh
cargo install gottem-cli      # provides the `gottem` binary
```

## Core command — fetch

```sh
gottem fetch <URL>
```

Default is **ladder** mode: start at the cheapest tier (T0, local/direct), climb
through vendor tiers (T1–T9) only as routes fail or return non-content.

Useful flags:

| Flag | Effect |
|---|---|
| `--mode ladder\|race\|hedge` | ladder = cheapest-first sequential; race = all selected routes in parallel, first good wins; hedge = ladder but fire a backup after a delay |
| `--tier-min N --tier-max M` | clamp which ladder tiers are eligible |
| `--require-js` | skip routes whose adapter can't render JavaScript |
| `--routes a,b,c` | restrict to specific route ids (required for race mode targeting) |
| `--budget-mc N` | cap spend in milli-cents |
| `--format content\|json` | raw page content, or JSON with metadata |
| `--show-meta` | print which route/tier/vendor served the request + cost |
| `--hedge-delay-ms`, `--hedge-count` | hedge-mode tuning |

```sh
gottem fetch https://example.com --show-meta
gottem fetch https://hard-site.com --require-js --format json
gottem fetch https://x.com --mode race --routes spider.cloud.smart,firecrawl.scrape
```

## probe — test reachability cheaply

```sh
gottem probe <URL> [--tier-min N --tier-max M] [--min-bytes 500]
```

Walks tiers reporting which routes succeed, without committing to a full fetch —
use it to discover the cheapest route that works for a domain.

## crawl — multi-page, streaming, never in-memory

```sh
gottem crawl <URL> [--depth N] [--limit M] [--engine auto|spider-cloud|local]
                   [--subdomains] [--tld]
                   [--allow PAT --allow PAT ...]
                   [--deny PAT --deny PAT ...]
                   [--respect-robots]
                   [--concurrency N]
                   [--param k=v --param k=v ...]
```

Streams **NDJSON** to stdout, one `PageEntry` per line, flushed immediately.
Memory stays constant regardless of crawl size.

Engines:

- `spider-cloud` — POST to Spider Cloud's `/crawl`, stream JSONL back. Vendor
  handles fanout; single network round-trip per crawl.
- `local` — gottem-owned BFS. Each URL goes through the **same scrape ladder**
  as `gottem fetch`, so per-page escalation works mid-crawl. Link discovery
  uses `spider::page::Page::links` on bytes already fetched — no re-fetch for
  outlinks. Visited / depth / allow / deny / robots / budget all delegated to
  `spider::website::Website`.
- `auto` — Spider Cloud if `SPIDER_CLOUD_API_KEY` is set, else local. Default.

`--param k=v` repeatable; values land in the route body template as
`{{param:k}}` (numbers and JSON literals parse correctly; everything else is a
string). Use this for vendor-specific knobs without editing TOML.

## routes — inspect the vendor catalog

```sh
gottem routes list                    # tabular catalog (22 builtin routes, 13 vendors)
gottem routes show <route-id>          # full detail for one route
gottem routes validate                # check env vars are set for each route's auth
gottem --config routes.toml fetch URL  # layer custom vendor routes on top of builtin
```

## How routing works

- **Routes** are data (TOML rows: a vendor endpoint, its tier, cost, capabilities).
- **Adapters** are code — a small fixed set of protocol families: plain HTTP,
  JSON API, streaming JSONL, headless Chrome over CDP, CAPTCHA solver.
- **Tiers T0–T9** order routes by cost/capability. Ladder mode climbs them;
  cheap local fetch first, premium unblocking vendors last.
- On a blocked/empty/challenge response, gottem escalates to the next tier
  automatically rather than returning bad data.

## Vendor credentials

Vendor routes read API keys from environment variables (e.g. `FIRECRAWL_API_KEY`,
`ZENROWS_API_KEY`, `SPIDER_CLOUD_API_KEY`). Run `gottem routes validate` to see
which routes are usable with the keys currently set — routes without their key
are skipped, not errored.

## Hosted gottem — gottem.dev

There is a hosted version: the same routing engine as a managed API, with no
local vendor keys or browser to run. Use it when you want gottem's routing
without operating the providers yourself.

- **Site + docs:** <https://gottem.dev> — API reference at
  <https://gottem.dev/docs>, sign-in/dashboard at <https://gottem.dev/signin>.
- **API base:** `https://api.gottem.dev` — `Authorization: Bearer gtm_…`
  (create a key in the dashboard). Key endpoints:
  - `POST /scrape` — fetch a URL (the hosted equivalent of `gottem fetch`).
  - `POST /probe` — reachability probe.
  - `POST /v1/compare` — run a URL through several providers at once and
    compare content quality, cost, and a SHA-256 of each result side by side;
    deterministic, with identical results merged. Built for quality validation.
- **Pricing:** pay-as-you-go credits — `1 credit = $0.0001`, debited only on a
  successful fetch. New accounts get free starter credits; BYOK is supported.

### Drive the hosted API from the CLI

The `gottem` CLI can run a fetch on the hosted API instead of locally — add
`--remote` and supply a key:

```sh
export GOTTEM_API_KEY=gtm_...
gottem fetch --remote <URL>                 # runs on api.gottem.dev
gottem fetch --remote --mode race <URL>     # the usual flags carry over
```

The key comes from `--api-key` or `$GOTTEM_API_KEY`; `$GOTTEM_API_URL`
overrides the base URL. The open-source CLI/library and the hosted API share
the same route catalog and escalation behavior — develop locally, scale on the
hosted API.

## License

Apache-2.0 OR MIT.
