---
name: gottem
description: Use when fetching or scraping the contents of a web page or URL, especially when a plain HTTP request returns a block page, a CAPTCHA, a Cloudflare challenge, an empty shell, or a 403/429. gottem routes the request through a tiered ladder of scraping vendors (and a local browser), cheapest first, escalating until it gets clean content. Also use to compare scraping vendors, force a specific provider, or race/hedge providers for latency.
---

# gottem, a universal scraper

gottem is one CLI and one Rust library that fetches a URL through a tiered ladder
of scraping vendors plus a local browser. It tries the cheapest route first,
escalates on failure, and stops when it gets clean content. Adding a vendor is a
TOML row, not a code change. Powered by [spider](https://github.com/spider-rs/spider).

Reach for gottem instead of `curl`/`fetch` when the target is bot-protected,
JS-rendered, geo-blocked, or rate-limited, meaning a naive request would come
back as a challenge page, an empty body, or an error status.

## Install

```sh
cargo install gottem-cli      # provides the `gottem` binary
```

## Core command: fetch

```sh
gottem fetch <URL>
```

The default is ladder mode: start at the cheapest tier (T0, local/direct) and climb
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
| `--show-meta` | print which route, tier, and vendor served the request, plus cost |
| `--hedge-delay-ms`, `--hedge-count` | hedge-mode tuning |

```sh
gottem fetch https://example.com --show-meta
gottem fetch https://hard-site.com --require-js --format json
gottem fetch https://x.com --mode race --routes spider.smart,firecrawl.scrape
```

## probe: test reachability with cheap calls

```sh
gottem probe <URL> [--tier-min N --tier-max M] [--min-bytes 500]
```

Walks the tiers and reports which routes succeed without committing to a full
fetch. Use it to find the cheapest route that works for a domain.

## crawl: multi-page, streaming, never in memory

```sh
gottem crawl <URL> [--depth N] [--limit M] [--engine auto|spider-cloud|local]
                   [--subdomains] [--tld]
                   [--allow PAT --allow PAT ...]
                   [--deny PAT --deny PAT ...]
                   [--respect-robots]
                   [--concurrency N]
                   [--param k=v --param k=v ...]
```

Streams NDJSON to stdout, one `PageEntry` per line, flushed immediately.
Memory stays constant no matter how large the crawl gets.

Engines:

- `spider-cloud`, POST to Spider's `/crawl` and stream JSONL back. The vendor
  handles fanout, one network round-trip per crawl.
- `local`, a gottem-owned BFS. Each URL goes through the same scrape ladder
  as `gottem fetch`, so per-page escalation works mid-crawl. Link discovery
  uses `spider::page::Page::links` on bytes already fetched, so outlinks cost
  no extra request. Visited, depth, allow, deny, robots, and budget all
  delegate to `spider::website::Website`.
- `auto`, Spider if `SPIDER_API_KEY` is set, else local. This is the default.

`--param k=v` repeats; values land in the route body template as
`{{param:k}}` (numbers and JSON literals parse correctly, everything else is a
string). Use it for vendor-specific knobs without editing TOML.

### Library use: subscriber sugar over a Stream

```rust
use std::sync::Arc;
use gottem_core::{CancelToken, ControlFlow, CrawlRequest, Orchestrator};
use url::Url;

let orch: Arc<Orchestrator> = /* built with crawl adapters installed */;
orch.crawl_builder(
        CrawlRequest::new(Url::parse("https://example.com")?)
            .with_limit(50)
            .with_depth(2),
    )
    .on_page(|page| async move {
        save(page).await;
        ControlFlow::Continue
    })
    .run(CancelToken::new())
    .await?;
```

Or the raw stream: `orch.crawl(req, cancel).await?` returns
`Stream<Item = Result<PageEntry>>`.

### Custom transport via `spider::RemoteFetcher`

Spider 2.51.198 exposes `Website::with_remote_fetcher`. Implement
`spider::fetcher::RemoteFetcher` and spider drives the full crawl engine
(visited, depth, allow/deny, robots, link extraction, subscription
channel) using your transport for the per-URL fetch. Useful when you want
spider's engine but a non-default transport, such as an internal API or a
custom proxy mesh. gottem's own local engine doesn't route through this hook
yet, because its scrape ladder needs hop-depth gating that spider will add in
a future patch.

## routes: inspect the vendor catalog

```sh
gottem routes list                     # tabular catalog (32 builtin routes, 17 vendors + local crawl)
gottem routes show <route-id>          # full detail for one route
gottem routes validate                 # check env vars are set for each route's auth
gottem --config routes.toml fetch URL  # layer custom vendor routes on top of builtin
```

## How routing works

- Routes are data: TOML rows carrying a vendor endpoint, its tier, cost, and capabilities.
- Adapters are code, a small fixed set of protocol families. Plain HTTP,
  JSON API, streaming JSONL, headless Chrome over CDP, CAPTCHA solver.
- Tiers T0–T9 order routes by cost and capability. Ladder mode climbs them,
  cheap local fetch first, premium unblocking vendors last.
- On a blocked, empty, or challenge response, gottem escalates to the next tier
  instead of returning bad data.

## Vendor credentials

Vendor routes read API keys from environment variables such as `FIRECRAWL_API_KEY`,
`ZENROWS_API_KEY`, and `SPIDER_API_KEY`. Run `gottem routes validate` to see
which routes are usable with the keys currently set. Routes without their key
are skipped, not errored.

## Hosted gottem at gottem.dev

There is a hosted version: the same routing engine as a managed API, with no
local vendor keys or browser to run. Use it when you want gottem's routing
without operating the providers yourself.

- Site and docs: <https://gottem.dev>, API reference at
  <https://gottem.dev/docs>, sign-in and dashboard at <https://gottem.dev/signin>.
- API base: `https://api.gottem.dev` with `Authorization: Bearer gtm_...`
  (create a key in the dashboard). Key endpoints:
  - `POST /scrape`, fetch a URL. The hosted equivalent of `gottem fetch`.
  - `POST /probe`, reachability probe.
  - `POST /v1/compare`, run a URL through several providers at once and
    compare content quality, cost, and a SHA-256 of each result side by side.
    Deterministic, with identical results merged. Built for quality validation.
- Pricing: pay-as-you-go credits, 1 credit = $0.0001, debited only on a
  successful fetch. New accounts get free starter credits, and BYOK works.

### Drive the hosted API from the CLI

The `gottem` CLI can run a fetch on the hosted API instead of locally. Add
`--remote` and supply a key:

```sh
export GOTTEM_API_KEY=gtm_...
gottem fetch --remote <URL>                 # runs on api.gottem.dev
gottem fetch --remote --mode race <URL>     # the usual flags carry over
```

The key comes from `--api-key` or `$GOTTEM_API_KEY`, and `$GOTTEM_API_URL`
overrides the base URL. The open-source CLI and library share the same route
catalog and escalation behavior as the hosted API, so you can develop locally
and scale on the hosted API.

## License

Apache-2.0 OR MIT.
