<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="assets/gottem-logo-github-dark.svg">
    <img alt="gottem — universal scraper" src="assets/gottem-logo-github-light.svg" width="540">
  </picture>
</p>

<p align="center">
  <em>Universal scraper that reliably gets the data.</em>
</p>

---

## What is gottem

gottem is one CLI and one Rust library that talks to every major scraping vendor — and your local browser — through a single tiered ladder. You give it a URL; it tries the cheapest way first, escalates when blocked, races vendors when speed matters, and stops when it gets clean content.

Each "way to fetch" is called a **route**. Routes are described in TOML and matched to one of a small set of **adapters** (plain HTTP, JSON API, streaming JSONL, headless Chrome over CDP, CAPTCHA solver). Adding a new vendor is a TOML row — no code change, no release.

Powered by [`spider`](https://github.com/spider-rs/spider).

---

## Install

```bash
cd crates/gottem-cli
cargo install --path .
```

This installs the `gottem` binary on your PATH.

---

## Try it in 30 seconds

```bash
# Inspect what's available — no API keys needed yet.
gottem routes list
gottem routes show spider.cloud.smart

# Tell gottem which vendor keys you have.
export FIRECRAWL_API_KEY=fc-...
export SPIDER_CLOUD_API_KEY=sk-...

# Fetch a URL. gottem starts cheap, escalates if the cheap routes fail.
gottem fetch https://example.com --show-meta

# Race three routes in parallel — fastest valid response wins.
gottem fetch https://example.com --mode race --routes firecrawl.scrape,spider.cloud.http,zenrows.basic

# Hedge: start cheap, fire a backup at the next tier after a delay.
gottem fetch https://example.com --mode hedge --hedge-delay-ms 2000

# Probe every tier on a target URL — useful for picking a baseline.
gottem probe https://hard-to-scrape.test
```

---

## The tier ladder

Lower tier = cheaper and faster. Higher tier = handles tougher anti-bot defenses. gottem walks the ladder cheapest-first by default and stops at the first route that returns valid content.

| Tier | Typical cost | What's at this level                                                       |
|------|-------------:|----------------------------------------------------------------------------|
| T0   | free         | direct local HTTP (you bring the URL, we send a GET)                       |
| T1–T3| varies       | local HTTP through a proxy, or local headless Chrome                        |
| T4   | $0.001       | basic cloud HTTP (Firecrawl, Spider Cloud HTTP, ScrapingBee, ZenRows)      |
| T5   | $0.005       | cloud HTTP **with JS render**                                              |
| T6   | $0.0075      | cloud HTTP + **residential proxy**                                         |
| T7   | $0.008–0.010 | smart unblockers — auto-fallback inside the vendor (Spider Smart, Zyte, Brightdata Unblocker) |
| T8   | $0.010–0.015 | **browser-as-a-service** over CDP (Brightdata Scraping Browser, Browserless, Spider Browser Cloud) |
| T9   | $0.02+       | last-resort: multi-step actors, premium scraping APIs, CAPTCHA solvers     |

You can pin the tier band you want with `--tier-min` / `--tier-max`, or hard-cap cost per fetch with `--budget-mc`.

---

## Built-in vendors

20 routes across 11 services. All you need is the env var.

| Vendor                    | Routes (count) | Env var                          |
|---------------------------|---------------:|----------------------------------|
| Spider Cloud              | 4              | `SPIDER_CLOUD_API_KEY`           |
| Firecrawl                 | 2              | `FIRECRAWL_API_KEY`              |
| ZenRows                   | 3              | `ZENROWS_API_KEY`                |
| ScrapingBee               | 3              | `SCRAPINGBEE_API_KEY`            |
| Brightdata Web Unlocker   | 1              | `BRIGHTDATA_TOKEN`               |
| Zyte API                  | 1              | `ZYTE_API_KEY`                   |
| Brightdata Scraping Browser | 1            | `BRIGHTDATA_BROWSER`             |
| Browserless               | 1              | `BROWSERLESS_TOKEN`              |
| Spider Browser Cloud      | 1              | `SPIDER_CLOUD_API_KEY` (shared)  |
| Apify                     | 1              | `APIFY_API_TOKEN`                |
| Oxylabs Web Scraper       | 1              | `OXYLABS_USER` + `OXYLABS_PASS`  |
| 2Captcha solver           | 1              | `2CAPTCHA_API_KEY` (¹)           |

Don't see your vendor? Drop a TOML file in `crates/gottem-routes-builtin/routes/` and you're done. See **Adding a vendor** below.

> ¹ `2CAPTCHA_API_KEY` starts with a digit, so POSIX shells (bash, zsh) refuse `export 2CAPTCHA_API_KEY=...`. Use a `.env` loader, prefix the binary with `env 2CAPTCHA_API_KEY=...`, or inject through your CI's secret store. Rust reads it via `std::env::var` regardless of how it got set.

---

## The three modes

### `--mode ladder` (default)

Try cheapest first. If the response fails validation (too short, WAF challenge, 5xx), escalate one tier and try again. Stop at the first valid response, the budget ceiling, or `--max-retries`.

Best for: most batch jobs. Cost-optimal.

### `--mode race`

Fire all selected routes in parallel. First valid response wins; the rest are cancelled mid-flight.

Best for: latency-critical fetches when budget allows duplicate cost.

### `--mode hedge`

Fire route 0 at t=0. If it doesn't return quickly, fire route 1 at t = `--hedge-delay-ms`. Then route 2 at 2× that delay, and so on. First valid wins. The delay shrinks adaptively when latency variance is bad — slow tails get hedged more aggressively automatically.

Best for: high-throughput pipelines where most fetches are cheap but the long tail kills you.

---

## CAPTCHA chains

gottem ships a 2Captcha adapter at T9 that you compose into your pipeline when a vendor returns a challenge page:

1. Run the primary fetch through the ladder.
2. Detect a CAPTCHA in the response (your code or a validator).
3. Call the `captcha.2captcha` route, passing `siteKey` + `captchaType` in `req.extra`.
4. Receive a solved token as `content`.
5. Replay the original URL with the token embedded (cookie / form field / header — depends on the captcha).

The solver handles 2Captcha's two-step submit-then-poll protocol internally — you just call it once. Supports reCAPTCHA v2, hCaptcha, and Cloudflare Turnstile.

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

Adding ZenRows-style query-string auth is the same pattern with `{{env:NAME}}` in the endpoint URL. There are five adapters that cover essentially every scraping API in the wild:

- **`direct_http`** — plain GET/POST
- **`http_json`** — POST JSON, parse JSON (Firecrawl, Zyte, Brightdata, Apify, Oxylabs)
- **`http_jsonl_stream`** — POST JSON, parse streaming JSONL (Spider Cloud)
- **`chrome_cdp`** — WebSocket CDP (Brightdata Scraping Browser, Browserless)
- **`captcha_2captcha`** — submit + poll (2Captcha)

You can also point gottem at your own `--config routes.toml` to layer custom routes on top of the built-ins.

---

## Modes recap

```bash
gottem fetch URL                                  # ladder, default
gottem fetch URL --mode race --routes a,b,c       # race A B C in parallel
gottem fetch URL --mode hedge --hedge-count 2     # primary + 2 staggered backups
gottem fetch URL --budget-mc 100                  # cap at $0.01 per fetch
gottem fetch URL --tier-min 4 --tier-max 7        # skip local; cap below T8
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

    let resp = orch.fetch_cheap(
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

`routes validate` exits 0 when every env var is present, exits 2 with a list otherwise — handy in CI.

---

## Hosted gottem — gottem.dev

Don't want to manage vendor keys or run a browser? The same engine runs as a
managed API at **[gottem.dev](https://gottem.dev)**. Sign up, create a `gtm_`
key, and call it — no keys to wrangle, pay-as-you-go credits.

### From the CLI — `--remote`

The `gottem` CLI can run any fetch against the hosted API instead of the local
ladder. Set your key once and add `--remote`:

```bash
export GOTTEM_API_KEY=gtm_your_key_here

# Runs on api.gottem.dev — no local vendor keys, no browser.
gottem fetch --remote https://example.com

# All the usual flags carry over to the hosted run.
gottem fetch --remote --mode race --show-meta https://example.com
gottem fetch --remote --format json https://example.com

# Or pass the key explicitly instead of the env var.
gottem fetch --remote --api-key gtm_your_key_here https://example.com
```

`$GOTTEM_API_URL` overrides the base URL (defaults to `https://api.gottem.dev`).

### From any HTTP client

```bash
# Hosted equivalent of `gottem fetch <url>`.
curl -X POST https://api.gottem.dev/scrape \
  -H "Authorization: Bearer gtm_your_key_here" \
  -H "content-type: application/json" \
  -d '{"url": "https://example.com"}'

# /v1/compare — runs every provider and compares quality, cost, and content
# side by side. Great for picking a route.
curl -X POST https://api.gottem.dev/v1/compare \
  -H "Authorization: Bearer gtm_your_key_here" \
  -H "content-type: application/json" \
  -d '{"url": "https://example.com"}'
```

The CLI and the hosted API share the same route catalog and escalation logic —
prototype locally with this crate, run production traffic on the hosted API.
Full reference: **[gottem.dev/docs](https://gottem.dev/docs)**.

---

## What's inside

```
gottem/
├── assets/                          logo, dark-mode logo, icon
└── crates/
    ├── gottem-core                  traits, types, orchestrator, retry strategies
    ├── gottem-adapters-http         direct_http · http_json · http_jsonl_stream
    ├── gottem-adapters-spider       T0–T3 local fetching via spider::Website
    ├── gottem-adapters-chrome       T8 CDP via spider::chromiumoxide
    ├── gottem-adapters-captcha      T9 2Captcha solver chain primitive
    ├── gottem-routes-builtin        embedded vendor TOML, feature-gated per vendor
    └── gottem-cli                   `gottem` binary — fetch · probe · routes
```

Every adapter and every vendor is behind a Cargo feature, so you can build a CLI with only the routes you actually need.

---

## License

Apache-2.0 OR MIT, your choice.
