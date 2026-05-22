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

## When NOT to use gottem

- A single unprotected static page that plain HTTP already returns cleanly —
  `curl` is fine.
- Crawling an entire site (gottem fetches single pages) — use `spider` directly
  for multi-page crawls.

## License

Apache-2.0 OR MIT.
