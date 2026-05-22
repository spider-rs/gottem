# gottem-adapters-chrome

CDP adapter for [gottem](https://github.com/spider-rs/gottem): drives a remote (or local) Chrome over the WebSocket DevTools Protocol. Covers tier T8.

## What it does

Implements the `chrome_cdp` adapter — connects to a Chrome instance via WebSocket and drives it with the DevTools Protocol. It uses `spider::chromiumoxide` (re-exported by spider's `chrome` feature), guaranteeing a single chromiumoxide build across the workspace with no duplicate protocol crates.

Each `execute` call opens a fresh CDP connection so that vendor sessions are properly isolated. The browser handle is dropped at function exit, closing the WebSocket and ending per-session billing.

## Vendors covered

| Vendor                      | Endpoint shape                                      |
|-----------------------------|-----------------------------------------------------|
| Brightdata Scraping Browser | `wss://brd.superproxy.io:9222`                      |
| Browserless                 | `wss://chrome.browserless.io?token={{env:TOKEN}}`   |
| Spider Browser Cloud        | `wss://browser.spider.cloud/v1/browser?api_key=...` |
| Local Chrome                | `ws://localhost:9222/devtools/browser/<id>`         |

## Cancellation

Connect, navigate, and content extraction are each guarded by `tokio::select!` against the orchestrator's `CancelToken`, with a bounded connect timeout (15s default). The chromiumoxide event handler task is aborted once the fetch resolves.

## Example

```rust
use gottem_core::AdapterRegistry;
use gottem_adapters_chrome::ChromeCdpAdapter;

let mut registry = AdapterRegistry::new();
registry.register(ChromeCdpAdapter::arc());
```

## Part of gottem

One of the adapter crates of the [gottem](https://github.com/spider-rs/gottem) workspace.

## License

Apache-2.0 OR MIT.
