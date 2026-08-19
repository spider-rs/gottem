# gottem-adapters-chrome

CDP adapter for [gottem](https://github.com/spider-rs/gottem). Drives a remote or local Chrome over the WebSocket DevTools Protocol, covering tier T8.

## What it does

Implements the `chrome_cdp` adapter. It connects to a Chrome instance over WebSocket and drives it with the DevTools Protocol, using `spider::chromiumoxide` (re-exported by spider's `chrome` feature) so the workspace has exactly one chromiumoxide build and no duplicate protocol crates.

Each `execute` call opens a fresh CDP connection, which keeps vendor sessions isolated. The browser handle drops at function exit, closing the WebSocket and ending per-session billing.

## Vendors covered

| Vendor                      | Endpoint shape                                      |
|-----------------------------|-----------------------------------------------------|
| Brightdata Scraping Browser | `wss://brd.superproxy.io:9222`                      |
| Browserless                 | `wss://chrome.browserless.io?token={{env:TOKEN}}`   |
| Spider Browser Cloud        | `wss://browser.spider.cloud/v1/browser?api_key=...` |
| Local Chrome                | `ws://localhost:9222/devtools/browser/<id>`         |

## Cancellation

Connect, navigate, and content extraction each run under `tokio::select!` against the orchestrator's `CancelToken`, with a bounded connect timeout (15s default). The chromiumoxide event handler task aborts once the fetch resolves.

## Example

```rust
use gottem_core::AdapterRegistry;
use gottem_adapters_chrome::ChromeCdpAdapter;

let mut registry = AdapterRegistry::new();
registry.register(ChromeCdpAdapter::arc());
```

## Part of gottem

One of the adapter crates in the [gottem](https://github.com/spider-rs/gottem) workspace.

## License

Apache-2.0 OR MIT.
