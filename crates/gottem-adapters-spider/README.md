# gottem-adapters-spider

Single-page fetch adapter for [gottem](https://github.com/spider-rs/gottem), backed by [`spider`](https://github.com/spider-rs/spider). Covers tiers T0–T3.

## What it does

Implements the `SpiderLocal` adapter kind by dispatching through `spider::page::Page::new_page`, the most direct primitive in spider's API. One URL in, one `Page` out. No crawl scheduler, no link discovery, no broadcast channels.

You get spider's hardened HTTP client (cookies, UA generation, encoding handling, TLS), predictable status-code propagation where an upstream 5xx surfaces as the real status, and identical behavior on Linux and macOS.

This adapter fetches a single URL only. It doesn't crawl, and headers are baked into the shared `spider::Client` at construction time. For per-request headers, use `gottem-adapters-http`.

## Tier coverage

| Tier | What you configure                | What spider provides            |
|------|-----------------------------------|---------------------------------|
| T0   | bare `endpoint`                   | reqwest HTTP via spider's Client |
| T1   | proxy list on the Client builder  | rotating datacenter proxy        |
| T2   | residential proxy on the Client   | residential pool                 |

## Example

```rust
use gottem_core::AdapterRegistry;
use gottem_adapters_spider::SpiderAdapter;

let mut registry = AdapterRegistry::new();
registry.register(SpiderAdapter::arc());
// or plug in a pre-configured spider::Client (proxies, custom UA):
// registry.register(SpiderAdapter::arc_with_client(my_client));
```

## Part of gottem

One of the adapter crates in the [gottem](https://github.com/spider-rs/gottem) workspace.

## License

Apache-2.0 OR MIT.
