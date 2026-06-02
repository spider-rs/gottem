//! Local probe: drive a remote (Kernel) browser via spider::Website over CDP and
//! time it, to validate the chrome_intercept fix for the slow-crawl hang.
//!
//!   cargo run -p gottem-adapters-chrome --example kernel_spider_probe -- <cdp_ws_url> [url]
//!
//! Env: INTERCEPT=0 disables chrome_intercept (to reproduce the slow path).
//!      RUST_LOG=spider=info for spider's own logs.

use spider::features::chrome_common::RequestInterceptConfiguration;
use spider::tokio;
use spider::website::Website;
use std::time::Instant;

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let cdp = args.get(1).expect("usage: <cdp_ws_url> [url]").clone();
    let url = args
        .get(2)
        .cloned()
        .unwrap_or_else(|| "https://example.com".to_string());
    let intercept = std::env::var("INTERCEPT").map(|v| v != "0").unwrap_or(true);
    eprintln!("intercept={intercept} url={url}");

    let mut b = Website::new(&url);
    b.with_limit(1)
        .with_stealth(false)
        .with_fingerprint(false)
        .with_chrome_connection(Some(cdp));
    if intercept {
        b.with_chrome_intercept(RequestInterceptConfiguration::new(true));
    }
    let mut website = b.build().expect("build");

    let mut rx = website.subscribe(16);
    let t = Instant::now();
    website.crawl().await;
    let elapsed = t.elapsed();

    let mut n = 0;
    let mut bytes = 0usize;
    let mut status = 0u16;
    loop {
        match rx.try_recv() {
            Ok(p) => {
                n += 1;
                bytes = p.get_html_bytes_u8().len();
                status = p.status_code.as_u16();
            }
            Err(spider::tokio::sync::broadcast::error::TryRecvError::Lagged(_)) => continue,
            Err(_) => break,
        }
    }
    eprintln!("DONE elapsed={elapsed:?} pages={n} status={status} html_bytes={bytes}");
}
