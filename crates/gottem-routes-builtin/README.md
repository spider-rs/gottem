# gottem-routes-builtin

Built-in vendor route catalogs for [gottem](https://github.com/spider-rs/gottem): 22 routes across 13 vendors, spanning tiers T0–T9.

## What it does

Each vendor is a Cargo feature flag. Routes ship as embedded TOML (`include_str!`) and load into a `gottem_core::RouteCatalogBuilder` at runtime — no file I/O, no network during catalog construction.

Vendors covered include Spider Cloud, Firecrawl, ZenRows, ScrapingBee, Zyte, Brightdata (Web Unlocker and Scraping Browser), Browserless, Spider Browser Cloud, Apify, Oxylabs, 2Captcha, Browserbase, and Browser Use.

## Quick start

```rust
use gottem_core::RouteCatalogBuilder;
use gottem_routes_builtin::register_all;

let catalog = register_all(RouteCatalogBuilder::new())
    .expect("builtin routes load")
    .build();
// Spider Cloud (4 routes) + Firecrawl (2) are enabled by default.
```

## Per-vendor selection

Disable defaults and pick only what you need:

```toml
gottem-routes-builtin = { version = "0.1", default-features = false, features = ["zyte", "brightdata"] }
```

Use the `all` feature to enable every vendor. Per-vendor helpers (`add_spider_cloud`, `add_firecrawl`, etc.) are also exposed individually.

## Adding a vendor

Drop `routes/<vendor>.toml` into the crate, add the feature flag, and wire a `cfg`-gated branch into `register_all`. No new Rust types, no new release of `gottem-core` — that is the point of the data-driven route design.

## Part of gottem

The built-in route catalog for the [gottem](https://github.com/spider-rs/gottem) workspace.

## License

Apache-2.0 OR MIT.
