<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="assets/gottem-logo-github-dark.svg">
    <img alt="gottem, a universal scraper" src="assets/gottem-logo-github-light.svg" width="540">
  </picture>
</p>

<p align="center">
  <em>One URL in, clean content out. Cheapest route first.</em>
</p>

---

## What is gottem

gottem is one CLI and one Rust library that talks to every major scraping vendor, plus your local browser, through a single tiered ladder. You give it a URL. It tries the cheapest way first, escalates when blocked, races vendors when speed matters, and stops when it gets clean content.

Each way to fetch is a *route*. Routes are described in TOML and matched to one of a handful of *adapters*: plain HTTP, JSON API, streaming JSONL, headless Chrome over CDP, CAPTCHA solver. Adding a vendor is a TOML row, not a code change and not a release.

Powered by [`spider`](https://github.com/spider-rs/spider).

---

## Install

```bash
cargo install gottem-cli
```

That puts the `gottem` binary on your PATH from
[crates.io](https://crates.io/crates/gottem-cli), no checkout needed.
Building from source works too, say for unreleased changes:

```bash
git clone https://github.com/spider-rs/gottem.git
cd gottem
cargo install --path crates/gottem-cli
```

---

## Try it in 30 seconds

```bash
# Inspect what's available. No API keys needed yet.
gottem routes list
gottem routes show spider.smart

# Tell gottem which vendor keys you have.
export FIRECRAWL_API_KEY=fc-...
export SPIDER_API_KEY=sk-...

# Fetch a URL. gottem starts at the cheapest tier and escalates if the basic routes fail.
gottem fetch https://example.com --show-meta

# Race three routes in parallel. Fastest valid response wins.
gottem fetch https://example.com --mode race --routes firecrawl.scrape,spider.http,zenrows.basic

# Hedge: start at the lowest tier, fire a backup at the next tier after a delay.
gottem fetch https://example.com --mode hedge --hedge-delay-ms 2000

# Probe every tier on a target URL to pick a baseline.
gottem probe https://hard-to-scrape.test
```

---

## The tier ladder

Lower tier means cheaper and faster. Higher tier handles tougher anti-bot defenses. gottem walks the ladder cheapest-first by default and stops at the first route that returns valid content.

| Tier | Typical cost | What's at this level                                                       |
|------|-------------:|----------------------------------------------------------------------------|
| T0   | free         | direct local HTTP (you bring the URL, we send a GET)                       |
| T1–T3| varies       | local HTTP through a proxy, or local headless Chrome                        |
| T4   | $0.001       | basic cloud HTTP (Firecrawl, Spider HTTP, ScrapingBee, ScraperAPI, ZenRows, Crawlbase) |
| T5   | $0.005       | cloud HTTP with JS render                                                  |
| T6   | $0.0075      | cloud HTTP plus residential proxy                                          |
| T7   | $0.008–0.010 | smart unblockers that fall back internally (Spider Smart, Zyte, Brightdata Unblocker) |
| T8   | $0.010–0.015 | browser-as-a-service over CDP (Brightdata Scraping Browser, Browserless, Spider Browser Cloud, Kernel) |
| T9   | $0.02+       | last resort: multi-step actors, premium scraping APIs, CAPTCHA solvers, AI browser agents |

Pin the tier band with `--tier-min` / `--tier-max`, or hard-cap cost per fetch with `--budget-mc`.

---

## Built-in vendors

32 routes across 17 vendors, plus a local crawl route. All you need is the env var.

| Vendor                      | Routes | Env var                                     |
|-----------------------------|-------:|---------------------------------------------|
| Spider                      | 5      | `SPIDER_API_KEY`                            |
| ScraperAPI                  | 4      | `SCRAPERAPI_KEY`                            |
| ScrapingBee                 | 3      | `SCRAPINGBEE_API_KEY`                       |
| ZenRows                     | 3      | `ZENROWS_API_KEY`                           |
| Firecrawl                   | 2      | `FIRECRAWL_API_KEY`                         |
| Brightdata Web Unlocker     | 1      | `BRIGHTDATA_TOKEN`                          |
| Brightdata Scraping Browser | 1      | `BRIGHTDATA_BROWSER`                        |
| Zyte API                    | 1      | `ZYTE_API_KEY`                              |
| Browserless                 | 1      | `BROWSERLESS_TOKEN`                         |
| Browserbase                 | 1      | `BROWSERBASE_API_KEY` + `BROWSERBASE_PROJECT_ID` |
| Spider Browser Cloud        | 1      | `SPIDER_API_KEY` (shared)                   |
| Kernel                      | 1      | `KERNEL_API_KEY`                            |
| Crawlbase                   | 1      | `CRAWLBASE_TOKEN`                           |
| Diffbot                     | 1      | `DIFFBOT_TOKEN`                             |
| Apify                       | 1      | `APIFY_API_TOKEN`                           |
| Oxylabs Web Scraper         | 1      | `OXYLABS_USER` + `OXYLABS_PASS`             |
| DataForSEO                  | 1      | `DATAFORSEO_LOGIN` + `DATAFORSEO_PASSWORD`  |
| Browser Use Cloud           | 1      | `BROWSER_USE_API_KEY`                       |
| 2Captcha solver             | 1      | `2CAPTCHA_API_KEY` (¹)                      |

Don't see your vendor? Drop a TOML file in `crates/gottem-routes-builtin/routes/` and you're done. See "Routes are config, not code" below.

> ¹ `2CAPTCHA_API_KEY` starts with a digit, so POSIX shells (bash, zsh) refuse `export 2CAPTCHA_API_KEY=...`. Use a `.env` loader, prefix the binary with `env 2CAPTCHA_API_KEY=...`, or inject it through your CI's secret store. Rust reads it via `std::env::var` no matter how it got set.

---

## The three modes

### `--mode ladder` (default)

Try the cheapest route first. If the response fails validation (too short, WAF challenge, 5xx), escalate one tier and try again. Stop at the first valid response, the budget ceiling, or `--max-retries`.

Best for most batch jobs. Cost-optimal.

### `--mode race`

Fire all selected routes in parallel. First valid response wins, the rest are cancelled mid-flight.

Best for latency-critical fetches when the budget can absorb duplicate cost.

### `--mode hedge`

Fire route 0 at t=0. If it doesn't return quickly, fire route 1 at t = `--hedge-delay-ms`. Then route 2 at twice that delay, and so on. First valid wins. The delay shrinks on its own when latency variance is bad, so slow tails get hedged harder.

Best for high-throughput pipelines where most fetches are cheap but the long tail kills you.

---

## Crawling

gottem also crawls, streaming, with no in-memory accumulation. One CLI:

```bash
# Local BFS using your scrape ladder for each page.
gottem crawl https://example.com --depth 2 --limit 50

# Force the Spider /crawl endpoint (native JSONL streaming).
gottem crawl https://example.com --engine spider-cloud --depth 3 --limit 100

# Subdomains, allow/deny patterns, robots.txt.
gottem crawl https://example.com \
    --subdomains \
    --allow /blog --allow /docs \
    --deny /admin --deny '\.pdf$' \
    --respect-robots \
    --concurrency 8

# Dynamic params forwarded to the route body template. Vendor-specific knobs
# without editing the TOML.
gottem crawl https://example.com --param waitFor=2000 --param mode=chrome
```

Output is NDJSON on stdout, one `PageEntry` per line, flushed immediately.
Pipe it to `jq`, `tee`, Postgres COPY, whatever. Memory stays constant no
matter how large the crawl gets.

Two engines, picked by `--engine`:

| Engine | What it does |
|---|---|
| `spider-cloud` | POSTs to Spider's `/crawl` and streams the JSONL response back. One round-trip per crawl, the vendor handles fanout. Needs `SPIDER_API_KEY`. |
| `local` | BFS owned by gottem. Every URL goes through the same scrape ladder you'd use for a one-off `fetch`, so per-page escalation (T0 to T7) still works mid-crawl. Link discovery uses [`spider::page::Page::links`](https://docs.rs/spider) on bytes already returned, so outlink extraction costs no extra fetch. Visited, depth, allow, deny, robots, and budget all delegate to `spider::website::Website`. |
| `auto` | Spider if the key is set, else local. This is the default. |

Library use, with subscriber sugar over the raw `Stream`:

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
        save_to_db(page).await;
        ControlFlow::Continue
    })
    .run(CancelToken::new())
    .await?;
```

Or the raw stream:

```rust
let mut stream = orch.crawl(req, CancelToken::new()).await?;
while let Some(page) = futures_util::StreamExt::next(&mut stream).await {
    /* ... */
}
```

The crawl never grows memory beyond what the consumer holds. Pages flow
through, get yielded, and are dropped. The local engine runs N concurrent
workers (`--concurrency`, default 4) on the multi-threaded runtime.

---

## CAPTCHA chains

gottem ships a 2Captcha adapter at T9 that you compose into your pipeline when a vendor returns a challenge page:

1. Run the primary fetch through the ladder.
2. Detect a CAPTCHA in the response (your code or a validator).
3. Call the `captcha.2captcha` route, passing `siteKey` and `captchaType` in `req.extra`.
4. Receive a solved token as `content`.
5. Replay the original URL with the token embedded, as a cookie, form field, or header, depending on the captcha.

The solver handles 2Captcha's two-step submit-then-poll protocol internally, so you call it once. It supports reCAPTCHA v2, hCaptcha, and Cloudflare Turnstile.

---

## Routes are config, not code

Every vendor in gottem is one TOML row. Here's the entire Firecrawl route:

```toml
[[route]]
id          = "firecrawl.scrape"
adapter     = "http_json"
endpoint    = "https://api.firecrawl.dev/v1/scrape"
method      = "POST"
tier        = 4
cost        = 10
timeout_ms  = 30000

[route.auth]
kind = "bearer"
env  = "FIRECRAWL_API_KEY"

[route.body]
kind     = "json"
template = '''{"url":"{{url}}","formats":["markdown"]}'''

[route.parse]
kind = "json_path"
path = "$.data.markdown"

[[route.validate]]
kind = "min_bytes"
n    = 500
```

ZenRows-style query-string auth is the same pattern with `{{env:NAME}}` in the endpoint URL. A handful of adapters cover most scraping APIs in the wild:

- `direct_http`, plain GET/POST
- `http_json`, POST JSON and parse JSON (Firecrawl, Zyte, Brightdata, Diffbot, Apify, Oxylabs, DataForSEO)
- `http_jsonl_stream`, POST JSON and parse streaming JSONL (Spider)
- `chrome_cdp`, WebSocket CDP (Brightdata Scraping Browser, Browserless, Browserbase, Spider Browser Cloud)
- `captcha_2captcha` and `browser_use`, submit-then-poll protocols

You can also point gottem at your own `--config routes.toml` to layer custom routes on top of the built-ins.

---

## Modes recap

```bash
gottem fetch URL                                  # ladder, default
gottem fetch URL --mode race --routes a,b,c       # race A B C in parallel
gottem fetch URL --mode hedge --hedge-count 2     # primary plus 2 staggered backups
gottem fetch URL --budget-mc 100                  # cap at $0.01 per fetch
gottem fetch URL --tier-min 4 --tier-max 7        # skip local, cap below T8
gottem fetch URL --require-js                     # only routes that render JS
gottem fetch URL --format json                    # structured output with metadata
```

---

## Using it as a library

```rust
use std::sync::Arc;
use gottem_core::{Budget, CancelToken, LadderStrategy, Orchestrator,
                  RouteCatalogBuilder, ScrapeRequest, Tier, AdapterRegistry, Capabilities};
use url::Url;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let catalog = Arc::new(
        gottem_routes_builtin::register_all(RouteCatalogBuilder::new())?.build()
    );

    let mut registry = AdapterRegistry::new();
    gottem_adapters_http::register_all(&mut registry, None);
    registry.register(gottem_adapters_spider::SpiderAdapter::arc());

    let orch = Arc::new(Orchestrator::new(
        catalog.clone(),
        Arc::new(registry),
        Arc::new(Budget::new(1_000)),  // $0.10 ceiling
    ));

    let strategy = Arc::new(LadderStrategy::new(
        catalog.clone(), Tier::T0, Tier::T9, Capabilities::default(), 5,
    ));

    let resp = orch.fetch(
        ScrapeRequest::get(Url::parse("https://example.com")?),
        strategy,
        CancelToken::new(),
    ).await?;

    println!("{}", resp.content.unwrap_or_default());
    Ok(())
}
```

---

## Inspecting the catalog

```bash
gottem routes list           # tabular view of every loaded route
gottem routes show <id>      # full detail for one route
gottem routes validate       # check that every route's env var is set
```

`routes validate` exits 0 when every env var is present and exits 2 with a list otherwise, which is handy in CI.

---

## Hosted gottem at gottem.dev

Don't want to manage vendor keys or run a browser? The same engine runs as a
managed API at [gottem.dev](https://gottem.dev). Sign up, create a `gtm_`
key, and call it. No keys to wrangle, pay-as-you-go credits.

### From the CLI, with `--remote`

The `gottem` CLI can run any fetch against the hosted API instead of the local
ladder. Set your key once and add `--remote`:

```bash
export GOTTEM_API_KEY=gtm_your_key_here

# Runs on api.gottem.dev. No local vendor keys, no browser.
gottem fetch --remote https://example.com

# All the usual flags carry over to the hosted run.
gottem fetch --remote --mode race --show-meta https://example.com
gottem fetch --remote --format json https://example.com

# Or pass the key explicitly instead of the env var.
gottem fetch --remote --api-key gtm_your_key_here https://example.com
```

`$GOTTEM_API_URL` overrides the base URL, which defaults to `https://api.gottem.dev`.

### From any HTTP client

```bash
# Hosted equivalent of `gottem fetch <url>`.
curl -X POST https://api.gottem.dev/scrape \
  -H "Authorization: Bearer gtm_your_key_here" \
  -H "content-type: application/json" \
  -d '{"url": "https://example.com"}'

# /v1/compare runs every provider and puts quality, cost, and content
# side by side. Use it to pick a route.
curl -X POST https://api.gottem.dev/v1/compare \
  -H "Authorization: Bearer gtm_your_key_here" \
  -H "content-type: application/json" \
  -d '{"url": "https://example.com"}'
```

The CLI and the hosted API share one route catalog and one escalation path, so
you can prototype locally with this crate and run production traffic on the
hosted API. Full reference at [gottem.dev/docs](https://gottem.dev/docs).

---

## What's inside

```
gottem/
├── assets/                          logo, dark-mode logo, icon
└── crates/
    ├── gottem-core                  traits, types, orchestrator, retry strategies
    ├── gottem-adapters-http         direct_http · http_json · http_jsonl_stream
    ├── gottem-adapters-spider       T0–T3 local fetching via spider::Website
    ├── gottem-adapters-chrome       T8 CDP via `chromey` (chromiumoxide fork)
    ├── gottem-adapters-captcha      T9 2Captcha solver chain
    ├── gottem-adapters-browseruse   T9 Browser Use Cloud AI agent
    ├── gottem-routes-builtin        embedded vendor TOML, feature-gated per vendor
    └── gottem-cli                   `gottem` binary: fetch · probe · crawl · routes
```

Every adapter and every vendor sits behind a Cargo feature, so you can build a CLI with only the routes you need.

---

## License

Apache-2.0 OR MIT, your choice.
