//! Built-in vendor route catalogs for gottem.
//!
//! Each vendor is a Cargo feature flag. Routes ship as embedded TOML (`include_str!`)
//! and load into a [`RouteCatalogBuilder`] at runtime — no file I/O, no network during
//! catalog construction.
//!
//! # Quick start
//!
//! ```no_run
//! use gottem_core::RouteCatalogBuilder;
//! use gottem_routes_builtin::register_all;
//!
//! let catalog = register_all(RouteCatalogBuilder::new())
//!     .expect("builtin routes load")
//!     .build();
//! assert!(catalog.len() >= 4); // Spider Cloud (4 routes) + Firecrawl (2) by default
//! ```
//!
//! # Per-vendor selection
//!
//! Disable defaults and pick only what you need:
//!
//! ```toml
//! gottem-routes-builtin = { version = "0.1", default-features = false, features = ["zyte", "brightdata"] }
//! ```
//!
//! # Adding a new vendor
//!
//! 1. Drop `routes/<vendor>.toml` into this crate.
//! 2. Add `<vendor>` to `[features]` in `Cargo.toml`.
//! 3. Add `add_<vendor>` and a `cfg`-gated branch in `register_all`.
//! 4. Add a test in `tests/`.
//!
//! No new Rust types, no new release of `gottem-core` — that's the whole point of the
//! data-driven route design.

#![forbid(unsafe_code)]
#![deny(rust_2018_idioms)]

use gottem_core::{FetchError, RouteCatalogBuilder};

/// Load every vendor enabled by Cargo features into the given builder.
///
/// Returns the (possibly extended) builder so calls are chainable. Vendor-by-vendor
/// helpers ([`add_spider_cloud`], [`add_firecrawl`], etc.) are exposed individually if
/// you want finer control.
pub fn register_all(builder: RouteCatalogBuilder) -> Result<RouteCatalogBuilder, FetchError> {
    let mut b = builder;
    #[cfg(feature = "spider-cloud")]
    {
        b = add_spider_cloud(b)?;
    }
    #[cfg(feature = "firecrawl")]
    {
        b = add_firecrawl(b)?;
    }
    #[cfg(feature = "brightdata")]
    {
        b = add_brightdata(b)?;
    }
    #[cfg(feature = "zyte")]
    {
        b = add_zyte(b)?;
    }
    #[cfg(feature = "zenrows")]
    {
        b = add_zenrows(b)?;
    }
    #[cfg(feature = "scrapingbee")]
    {
        b = add_scrapingbee(b)?;
    }
    #[cfg(feature = "brightdata-browser")]
    {
        b = add_brightdata_browser(b)?;
    }
    #[cfg(feature = "browserless")]
    {
        b = add_browserless(b)?;
    }
    #[cfg(feature = "spider-browser")]
    {
        b = add_spider_browser(b)?;
    }
    #[cfg(feature = "apify")]
    {
        b = add_apify(b)?;
    }
    #[cfg(feature = "oxylabs")]
    {
        b = add_oxylabs(b)?;
    }
    #[cfg(feature = "two-captcha")]
    {
        b = add_two_captcha(b)?;
    }
    #[cfg(feature = "browserbase")]
    {
        b = add_browserbase(b)?;
    }
    #[cfg(feature = "browser-use")]
    {
        b = add_browser_use(b)?;
    }
    #[cfg(feature = "crawlbase")]
    {
        b = add_crawlbase(b)?;
    }
    #[cfg(feature = "diffbot")]
    {
        b = add_diffbot(b)?;
    }
    #[cfg(feature = "dataforseo")]
    {
        b = add_dataforseo(b)?;
    }
    Ok(b)
}

/// Spider Cloud — JSONL streaming, Bearer auth. 4 routes spanning T4 (HTTP) → T7 (smart unblocker).
/// Requires env var `SPIDER_CLOUD_API_KEY`.
#[cfg(feature = "spider-cloud")]
pub fn add_spider_cloud(b: RouteCatalogBuilder) -> Result<RouteCatalogBuilder, FetchError> {
    b.add_toml(include_str!("../routes/spider_cloud.toml"))
}

/// Firecrawl — JSON API, Bearer auth. 2 routes: T4 (HTTP) and T5 (JS render).
/// Requires env var `FIRECRAWL_API_KEY`.
#[cfg(feature = "firecrawl")]
pub fn add_firecrawl(b: RouteCatalogBuilder) -> Result<RouteCatalogBuilder, FetchError> {
    b.add_toml(include_str!("../routes/firecrawl.toml"))
}

/// Brightdata Web Unlocker — JSON API, Bearer auth. One route at T7.
/// Requires env var `BRIGHTDATA_TOKEN`.
#[cfg(feature = "brightdata")]
pub fn add_brightdata(b: RouteCatalogBuilder) -> Result<RouteCatalogBuilder, FetchError> {
    b.add_toml(include_str!("../routes/brightdata.toml"))
}

/// Zyte API — JSON API, HTTP Basic auth (key as user). One route at T7.
/// Requires env var `ZYTE_API_KEY`.
#[cfg(feature = "zyte")]
pub fn add_zyte(b: RouteCatalogBuilder) -> Result<RouteCatalogBuilder, FetchError> {
    b.add_toml(include_str!("../routes/zyte.toml"))
}

/// ZenRows — GET with query-string auth via endpoint templating. 3 routes T4-T6.
/// Requires env var `ZENROWS_API_KEY`.
#[cfg(feature = "zenrows")]
pub fn add_zenrows(b: RouteCatalogBuilder) -> Result<RouteCatalogBuilder, FetchError> {
    b.add_toml(include_str!("../routes/zenrows.toml"))
}

/// ScrapingBee — GET with query-string auth via endpoint templating. 3 routes T4-T6.
/// Requires env var `SCRAPINGBEE_API_KEY`.
#[cfg(feature = "scrapingbee")]
pub fn add_scrapingbee(b: RouteCatalogBuilder) -> Result<RouteCatalogBuilder, FetchError> {
    b.add_toml(include_str!("../routes/scrapingbee.toml"))
}

/// Brightdata Scraping Browser — CDP, WsUserinfo auth. One route at T8.
/// Requires env var `BRIGHTDATA_BROWSER` (format `brd-customer-X-zone-Y:password`).
#[cfg(feature = "brightdata-browser")]
pub fn add_brightdata_browser(b: RouteCatalogBuilder) -> Result<RouteCatalogBuilder, FetchError> {
    b.add_toml(include_str!("../routes/brightdata_browser.toml"))
}

/// Browserless CDP — token in URL query. One route at T8.
/// Requires env var `BROWSERLESS_TOKEN`.
#[cfg(feature = "browserless")]
pub fn add_browserless(b: RouteCatalogBuilder) -> Result<RouteCatalogBuilder, FetchError> {
    b.add_toml(include_str!("../routes/browserless.toml"))
}

/// Spider Browser Cloud — CDP, api_key in URL query. One route at T8.
/// Requires env var `SPIDER_CLOUD_API_KEY`.
#[cfg(feature = "spider-browser")]
pub fn add_spider_browser(b: RouteCatalogBuilder) -> Result<RouteCatalogBuilder, FetchError> {
    b.add_toml(include_str!("../routes/spider_browser.toml"))
}

/// Apify — Bearer auth, sync actor run returning dataset items. One route at T9
/// using the `apify/website-content-crawler` actor. Requires env var `APIFY_API_TOKEN`.
#[cfg(feature = "apify")]
pub fn add_apify(b: RouteCatalogBuilder) -> Result<RouteCatalogBuilder, FetchError> {
    b.add_toml(include_str!("../routes/apify.toml"))
}

/// Oxylabs Web Scraper API — HTTP Basic auth. One route at T9 with browser rendering.
/// Requires env vars `OXYLABS_USER` and `OXYLABS_PASS`.
#[cfg(feature = "oxylabs")]
pub fn add_oxylabs(b: RouteCatalogBuilder) -> Result<RouteCatalogBuilder, FetchError> {
    b.add_toml(include_str!("../routes/oxylabs.toml"))
}

/// 2Captcha solver — returns a captcha TOKEN (not page HTML). Use in chains:
/// fetch primary → extract siteKey → solve via this route → replay with token.
/// Requires env var `2CAPTCHA_API_KEY` (set via dotenv / `env` prefix / CI secret —
/// names beginning with a digit aren't POSIX-valid and shells refuse `export` for them).
#[cfg(feature = "two-captcha")]
pub fn add_two_captcha(b: RouteCatalogBuilder) -> Result<RouteCatalogBuilder, FetchError> {
    b.add_toml(include_str!("../routes/two_captcha.toml"))
}

/// Browserbase — CDP via WebSocket, apiKey + projectId in URL template. One route at T9.
/// Requires env vars `BROWSERBASE_API_KEY` and `BROWSERBASE_PROJECT_ID`.
#[cfg(feature = "browserbase")]
pub fn add_browserbase(b: RouteCatalogBuilder) -> Result<RouteCatalogBuilder, FetchError> {
    b.add_toml(include_str!("../routes/browserbase.toml"))
}

/// Browser Use Cloud — AI agent that runs a natural-language browser task.
/// Async-only: submits to `/api/v1/run-task` and returns the task ID. Callers poll
/// `/api/v1/task/{id}` for the final output until a polling adapter ships.
/// Requires env var `BROWSER_USE_API_KEY`.
#[cfg(feature = "browser-use")]
pub fn add_browser_use(b: RouteCatalogBuilder) -> Result<RouteCatalogBuilder, FetchError> {
    b.add_toml(include_str!("../routes/browser_use.toml"))
}

/// Crawlbase Crawling API — GET with token in query string, raw HTML response.
/// One route at T4 (standard / datacenter IPs, no JS). True PAYG: only pay for
/// successful requests, no monthly commitment. Requires env var `CRAWLBASE_TOKEN`.
#[cfg(feature = "crawlbase")]
pub fn add_crawlbase(b: RouteCatalogBuilder) -> Result<RouteCatalogBuilder, FetchError> {
    b.add_toml(include_str!("../routes/crawlbase.toml"))
}

/// Diffbot Extract (Article API) — GET with token in query string, structured
/// JSON response. One route at T5 with JS rendering. Free tier ships 10k
/// credits/mo (1 credit/page). Requires env var `DIFFBOT_TOKEN`.
#[cfg(feature = "diffbot")]
pub fn add_diffbot(b: RouteCatalogBuilder) -> Result<RouteCatalogBuilder, FetchError> {
    b.add_toml(include_str!("../routes/diffbot.toml"))
}

/// DataForSEO — POST with HTTP Basic auth, JSON response. Pay-as-you-go SERP
/// + SEO data APIs; prepaid funds never expire ($50 min top-up). Per-request
/// cost is extracted live from `$.cost` (dollars) so the static estimate
/// only matters as a fallback. One route at T7 covering Google Organic SERP
/// Live Regular. Requires env vars `DATAFORSEO_LOGIN` + `DATAFORSEO_PASSWORD`.
#[cfg(feature = "dataforseo")]
pub fn add_dataforseo(b: RouteCatalogBuilder) -> Result<RouteCatalogBuilder, FetchError> {
    b.add_toml(include_str!("../routes/dataforseo.toml"))
}

/// Raw access to the embedded TOML strings. Useful for displaying the shipped catalog
/// in CLI output (e.g. `gottem routes show <vendor>`) or for tests.
pub mod embedded {
    #[cfg(feature = "spider-cloud")]
    pub const SPIDER_CLOUD: &str = include_str!("../routes/spider_cloud.toml");
    #[cfg(feature = "firecrawl")]
    pub const FIRECRAWL: &str = include_str!("../routes/firecrawl.toml");
    #[cfg(feature = "brightdata")]
    pub const BRIGHTDATA: &str = include_str!("../routes/brightdata.toml");
    #[cfg(feature = "zyte")]
    pub const ZYTE: &str = include_str!("../routes/zyte.toml");
    #[cfg(feature = "zenrows")]
    pub const ZENROWS: &str = include_str!("../routes/zenrows.toml");
    #[cfg(feature = "scrapingbee")]
    pub const SCRAPINGBEE: &str = include_str!("../routes/scrapingbee.toml");
    #[cfg(feature = "brightdata-browser")]
    pub const BRIGHTDATA_BROWSER: &str = include_str!("../routes/brightdata_browser.toml");
    #[cfg(feature = "browserless")]
    pub const BROWSERLESS: &str = include_str!("../routes/browserless.toml");
    #[cfg(feature = "spider-browser")]
    pub const SPIDER_BROWSER: &str = include_str!("../routes/spider_browser.toml");
    #[cfg(feature = "apify")]
    pub const APIFY: &str = include_str!("../routes/apify.toml");
    #[cfg(feature = "oxylabs")]
    pub const OXYLABS: &str = include_str!("../routes/oxylabs.toml");
    #[cfg(feature = "two-captcha")]
    pub const TWO_CAPTCHA: &str = include_str!("../routes/two_captcha.toml");
    #[cfg(feature = "browserbase")]
    pub const BROWSERBASE: &str = include_str!("../routes/browserbase.toml");
    #[cfg(feature = "browser-use")]
    pub const BROWSER_USE: &str = include_str!("../routes/browser_use.toml");
    #[cfg(feature = "crawlbase")]
    pub const CRAWLBASE: &str = include_str!("../routes/crawlbase.toml");
    #[cfg(feature = "diffbot")]
    pub const DIFFBOT: &str = include_str!("../routes/diffbot.toml");
    #[cfg(feature = "dataforseo")]
    pub const DATAFORSEO: &str = include_str!("../routes/dataforseo.toml");
}
