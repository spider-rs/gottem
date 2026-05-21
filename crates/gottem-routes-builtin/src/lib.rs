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
    #[cfg(feature = "spider-cloud")]       { b = add_spider_cloud(b)?; }
    #[cfg(feature = "firecrawl")]          { b = add_firecrawl(b)?; }
    #[cfg(feature = "brightdata")]         { b = add_brightdata(b)?; }
    #[cfg(feature = "zyte")]               { b = add_zyte(b)?; }
    #[cfg(feature = "zenrows")]            { b = add_zenrows(b)?; }
    #[cfg(feature = "scrapingbee")]        { b = add_scrapingbee(b)?; }
    #[cfg(feature = "brightdata-browser")] { b = add_brightdata_browser(b)?; }
    #[cfg(feature = "browserless")]        { b = add_browserless(b)?; }
    #[cfg(feature = "spider-browser")]     { b = add_spider_browser(b)?; }
    #[cfg(feature = "apify")]              { b = add_apify(b)?; }
    #[cfg(feature = "oxylabs")]            { b = add_oxylabs(b)?; }
    #[cfg(feature = "two-captcha")]        { b = add_two_captcha(b)?; }
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
}
