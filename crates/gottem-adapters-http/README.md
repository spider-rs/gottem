# gottem-adapters-http

HTTP adapters for [gottem](https://github.com/spider-rs/gottem). They dispatch routes against cloud scraping vendor REST APIs.

## What it does

Three adapters, all sharing one `reqwest::Client` for connection pooling:

- `DirectHttpAdapter` (`direct_http`) sends a plain GET or POST and passes the body through as-is.
- `HttpJsonAdapter` (`http_json`) POSTs a JSON body rendered from the route's `BodyTemplate` (with `{{url}}` substitution) and parses the JSON response per the route's `ResponseParse` spec. Covers Firecrawl, ZenRows, ScrapingBee, Zyte, and Brightdata Web Unlocker.
- `HttpJsonlStreamAdapter` (`http_jsonl_stream`) works like `http_json` but treats the response as newline-delimited JSONL. Spider's `/scrape` endpoint needs this, since it streams chunked JSONL.

Every adapter wraps its `send()` and `bytes()` futures in `tokio::select!` against the orchestrator's `CancelToken`, so a winning race or a Ctrl-C aborts the in-flight request cleanly.

## Example

```rust
use std::sync::Arc;
use gottem_core::AdapterRegistry;
use gottem_adapters_http::register_all;

let mut reg = AdapterRegistry::new();
register_all(&mut reg, None); // None = default client; Some(client) to plug your own
let reg = Arc::new(reg);
```

## Part of gottem

One of the adapter crates in the [gottem](https://github.com/spider-rs/gottem) workspace.

## License

Apache-2.0 OR MIT.
