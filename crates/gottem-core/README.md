# gottem-core

Universal scraper foundation: route catalog, the `Adapter` trait, and a ladder/race/hedge orchestrator. Powered by [`spider`](https://github.com/spider-rs/spider).

## What it does

`gottem-core` is the foundation of [gottem](https://github.com/spider-rs/gottem), one library that talks to every major scraping vendor through a single tiered ladder. It defines three things:

- `Route`, a vendor endpoint described as data, loaded from TOML with no per-vendor code path. Routes carry a tier, cost, auth spec, body template, and parse and validate rules.
- `Adapter`, a small finite set of protocols (`DirectHttp`, `HttpJson`, `HttpJsonlStream`, `ChromeCdp`, `SpiderLocal`, `Custom`). Adapters are code, routes are config. Adding a vendor is a TOML row, not a release.
- `Orchestrator`, which drives requests against a `RouteCatalog` in three modes. Ladder is cheapest-first and escalates on failure, race runs routes in parallel and takes the first valid response, and hedge fires staggered backups.

It also re-exports spider's `RetryStrategy` trait family, `HedgeTracker`, `AntiBotTech`, and `RequestProxy`, and adds `WaterfallStats`, a learned per-`(route, domain)` ordering that skips the ladder warmup once a route has proven itself.

## Example

```rust
use std::sync::Arc;
use gottem_core::{
    AdapterRegistry, Budget, CancelToken, Capabilities, LadderStrategy,
    Orchestrator, RouteCatalogBuilder, ScrapeRequest, Tier,
};
use url::Url;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let catalog = Arc::new(
        gottem_routes_builtin::register_all(RouteCatalogBuilder::new())?.build()
    );

    let mut registry = AdapterRegistry::new();
    gottem_adapters_http::register_all(&mut registry, None);
    registry.register(gottem_adapters_spider::SpiderAdapter::arc());

    let orch = Orchestrator::new(
        catalog.clone(),
        Arc::new(registry),
        Arc::new(Budget::new(1_000)), // $0.10 ceiling, in milli-cents
    );

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

## Part of gottem

This crate is the core of the [gottem](https://github.com/spider-rs/gottem) workspace. See the umbrella project for adapters, built-in vendor routes, and the `gottem` CLI.

## License

Apache-2.0 OR MIT.
