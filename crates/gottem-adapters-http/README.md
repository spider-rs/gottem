# gottem-adapters-http

HTTP adapters for [gottem](https://github.com/spider-rs/gottem): dispatch routes against cloud scraping vendor REST APIs.

## What it does

Provides three adapters, all sharing one `reqwest::Client` for connection pooling:

- **`DirectHttpAdapter`** (`direct_http`) — plain GET/POST. Body passed through as-is.
- **`HttpJsonAdapter`** (`http_json`) — POST a JSON body rendered from the route's `BodyTemplate` (with `{{url}}` substitution), parse the JSON response per the route's `ResponseParse` spec. Covers Firecrawl, ZenRows, ScrapingBee, Zyte, Brightdata Web Unlocker.
- **`HttpJsonlStreamAdapter`** (`http_jsonl_stream`) — same as `http_json` but the response is treated as newline-delimited JSONL. Required by Spider's `/scrape` endpoint, which streams chunked JSONL.

Every adapter wraps its `send()` and `bytes()` futures in `tokio::select!` against the orchestrator's `CancelToken`, so a winning race or Ctrl-C aborts the in-flight request cleanly.

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

One of the adapter crates of the [gottem](https://github.com/spider-rs/gottem) workspace.

## License

Apache-2.0 OR MIT.
