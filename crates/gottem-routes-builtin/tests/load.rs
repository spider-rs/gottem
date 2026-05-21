//! Tests that each vendor's embedded TOML loads cleanly into a [`RouteCatalog`]
//! and produces the expected route IDs / tiers / adapter kinds.

use gottem_core::{AdapterKind, RouteCatalogBuilder, Tier};

#[cfg(feature = "spider-cloud")]
#[test]
fn spider_cloud_loads_4_routes_tiers_4_through_7() {
    let catalog = gottem_routes_builtin::add_spider_cloud(RouteCatalogBuilder::new())
        .expect("spider_cloud.toml parses")
        .build();
    assert_eq!(catalog.len(), 4);
    for id in [
        "spider.cloud.http",
        "spider.cloud.chrome",
        "spider.cloud.chrome.residential",
        "spider.cloud.smart",
    ] {
        let route = catalog.get(id).unwrap_or_else(|| panic!("missing route {id}"));
        assert_eq!(route.adapter, AdapterKind::HttpJsonlStream, "{id} should use jsonl stream");
    }
    assert_eq!(catalog.get("spider.cloud.http").unwrap().tier, Tier::T4);
    assert_eq!(catalog.get("spider.cloud.smart").unwrap().tier, Tier::T7);
    // T7 smart should declare residential + stealth caps.
    let smart = catalog.get("spider.cloud.smart").unwrap();
    assert!(smart.caps.js);
    assert!(smart.caps.residential);
    assert!(smart.caps.stealth);
}

#[cfg(feature = "firecrawl")]
#[test]
fn firecrawl_loads_2_routes_with_js_tier_marked() {
    let catalog = gottem_routes_builtin::add_firecrawl(RouteCatalogBuilder::new())
        .expect("firecrawl.toml parses")
        .build();
    assert_eq!(catalog.len(), 2);
    let scrape = catalog.get("firecrawl.scrape").unwrap();
    let scrape_js = catalog.get("firecrawl.scrape.js").unwrap();
    assert_eq!(scrape.tier, Tier::T4);
    assert_eq!(scrape_js.tier, Tier::T5);
    assert!(!scrape.caps.js, "T4 should not require js");
    assert!(scrape_js.caps.js, "T5 should require js");
    assert_eq!(scrape.adapter, AdapterKind::HttpJson);
}

#[cfg(feature = "brightdata")]
#[test]
fn brightdata_unlocker_loads_at_t7() {
    let catalog = gottem_routes_builtin::add_brightdata(RouteCatalogBuilder::new())
        .expect("brightdata.toml parses")
        .build();
    assert_eq!(catalog.len(), 1);
    let r = catalog.get("brightdata.unlocker").unwrap();
    assert_eq!(r.tier, Tier::T7);
    assert_eq!(r.adapter, AdapterKind::HttpJson);
    assert!(r.caps.residential);
    assert!(r.caps.stealth);
}

#[cfg(feature = "zenrows")]
#[test]
fn zenrows_loads_with_endpoint_template() {
    let catalog = gottem_routes_builtin::add_zenrows(RouteCatalogBuilder::new())
        .expect("zenrows.toml parses")
        .build();
    assert_eq!(catalog.len(), 3);
    let basic = catalog.get("zenrows.basic").unwrap();
    assert!(basic.endpoint.is_template(), "zenrows endpoint must be a template");
    assert!(basic.endpoint.as_str().contains("{{env:ZENROWS_API_KEY}}"));
    assert!(basic.endpoint.as_str().contains("{{url}}"));
    assert_eq!(basic.tier, Tier::T4);
    assert_eq!(catalog.get("zenrows.premium").unwrap().tier, Tier::T6);
}

#[cfg(feature = "scrapingbee")]
#[test]
fn scrapingbee_loads_with_endpoint_template() {
    let catalog = gottem_routes_builtin::add_scrapingbee(RouteCatalogBuilder::new())
        .expect("scrapingbee.toml parses")
        .build();
    assert_eq!(catalog.len(), 3);
    let basic = catalog.get("scrapingbee.basic").unwrap();
    assert!(basic.endpoint.is_template());
    assert!(basic.endpoint.as_str().contains("{{env:SCRAPINGBEE_API_KEY}}"));
    let premium = catalog.get("scrapingbee.premium").unwrap();
    assert!(premium.caps.js && premium.caps.residential);
}

#[cfg(feature = "zyte")]
#[test]
fn zyte_loads_with_basic_auth_no_password() {
    use gottem_core::AuthSpec;
    let catalog = gottem_routes_builtin::add_zyte(RouteCatalogBuilder::new())
        .expect("zyte.toml parses")
        .build();
    let r = catalog.get("zyte.api").unwrap();
    assert_eq!(r.tier, Tier::T7);
    match &r.auth {
        AuthSpec::Basic { user_env, pass_env } => {
            assert_eq!(user_env, "ZYTE_API_KEY");
            assert!(pass_env.is_none(), "Zyte uses key-as-user without password");
        }
        other => panic!("expected Basic auth for Zyte, got {other:?}"),
    }
}

#[test]
fn register_all_succeeds_with_default_features() {
    // With default features (spider-cloud + firecrawl) we should get 6 routes total.
    // With --all-features (also brightdata + zyte) we should get 8.
    let catalog = gottem_routes_builtin::register_all(RouteCatalogBuilder::new())
        .expect("all builtin routes load")
        .build();

    let mut expected = 0usize;
    if cfg!(feature = "spider-cloud")       { expected += 4; }
    if cfg!(feature = "firecrawl")          { expected += 2; }
    if cfg!(feature = "brightdata")         { expected += 1; }
    if cfg!(feature = "zyte")               { expected += 1; }
    if cfg!(feature = "zenrows")            { expected += 3; }
    if cfg!(feature = "scrapingbee")        { expected += 3; }
    if cfg!(feature = "brightdata-browser") { expected += 1; }
    if cfg!(feature = "browserless")        { expected += 1; }
    if cfg!(feature = "spider-browser")     { expected += 1; }
    if cfg!(feature = "apify")              { expected += 1; }
    if cfg!(feature = "oxylabs")            { expected += 1; }
    if cfg!(feature = "two-captcha")        { expected += 1; }

    assert_eq!(catalog.len(), expected, "route count mismatch");
}

#[cfg(feature = "two-captcha")]
#[test]
fn two_captcha_loads_with_custom_adapter_and_captcha_caps() {
    use gottem_core::{AdapterKind, AuthSpec};
    let catalog = gottem_routes_builtin::add_two_captcha(RouteCatalogBuilder::new())
        .expect("two_captcha.toml parses")
        .build();
    let r = catalog.get("captcha.2captcha").unwrap();
    assert_eq!(r.tier, Tier::T9);
    match &r.adapter {
        AdapterKind::Custom(name) => assert_eq!(name.as_ref(), "captcha_2captcha"),
        other => panic!("expected Custom(captcha_2captcha), got {other:?}"),
    }
    match &r.auth {
        AuthSpec::Bearer { env } => assert_eq!(env, "TWO_CAPTCHA_API_KEY"),
        other => panic!("expected Bearer auth, got {other:?}"),
    }
    assert!(r.caps.captcha, "captcha route must advertise captcha capability");
}

#[cfg(feature = "apify")]
#[test]
fn apify_loads_with_bearer_and_first_item_jsonpath() {
    use gottem_core::{AdapterKind, AuthSpec, ResponseParse};
    let catalog = gottem_routes_builtin::add_apify(RouteCatalogBuilder::new())
        .expect("apify.toml parses")
        .build();
    let r = catalog.get("apify.web_scraper").unwrap();
    assert_eq!(r.tier, Tier::T9);
    assert_eq!(r.adapter, AdapterKind::HttpJson);
    match &r.auth {
        AuthSpec::Bearer { env } => assert_eq!(env, "APIFY_API_TOKEN"),
        other => panic!("expected Bearer for Apify, got {other:?}"),
    }
    match &r.parse {
        ResponseParse::JsonPath { path } => assert_eq!(path, "$[0].markdown"),
        other => panic!("expected JsonPath for Apify, got {other:?}"),
    }
    assert!(r.endpoint.as_str().contains("apify~website-content-crawler"));
}

#[cfg(feature = "oxylabs")]
#[test]
fn oxylabs_loads_with_basic_auth_and_results_jsonpath() {
    use gottem_core::{AdapterKind, AuthSpec, ResponseParse};
    let catalog = gottem_routes_builtin::add_oxylabs(RouteCatalogBuilder::new())
        .expect("oxylabs.toml parses")
        .build();
    let r = catalog.get("oxylabs.scraper").unwrap();
    assert_eq!(r.tier, Tier::T9);
    assert_eq!(r.adapter, AdapterKind::HttpJson);
    match &r.auth {
        AuthSpec::Basic { user_env, pass_env } => {
            assert_eq!(user_env, "OXYLABS_USER");
            assert_eq!(pass_env.as_deref(), Some("OXYLABS_PASS"));
        }
        other => panic!("expected Basic for Oxylabs, got {other:?}"),
    }
    match &r.parse {
        ResponseParse::JsonPath { path } => assert_eq!(path, "$.results[0].content"),
        other => panic!("expected JsonPath for Oxylabs, got {other:?}"),
    }
}

#[cfg(all(feature = "brightdata-browser", feature = "browserless", feature = "spider-browser"))]
#[test]
fn chrome_routes_load_at_t8_with_ws_endpoints() {
    use gottem_core::{AdapterKind, AuthSpec, RouteCatalogBuilder, Tier};
    let catalog = RouteCatalogBuilder::new()
        .add_toml(gottem_routes_builtin::embedded::BRIGHTDATA_BROWSER)
        .unwrap()
        .add_toml(gottem_routes_builtin::embedded::BROWSERLESS)
        .unwrap()
        .add_toml(gottem_routes_builtin::embedded::SPIDER_BROWSER)
        .unwrap()
        .build();

    let brd = catalog.get("brightdata.scraping_browser").unwrap();
    assert_eq!(brd.adapter, AdapterKind::ChromeCdp);
    assert_eq!(brd.tier, Tier::T8);
    assert!(brd.endpoint.as_str().starts_with("wss://"));
    match &brd.auth {
        AuthSpec::WsUserinfo { env } => assert_eq!(env, "BRIGHTDATA_BROWSER"),
        other => panic!("expected WsUserinfo for brightdata, got {other:?}"),
    }

    let bless = catalog.get("browserless.cdp").unwrap();
    assert!(bless.endpoint.is_template(), "browserless uses query-template auth");
    assert!(bless.endpoint.as_str().contains("{{env:BROWSERLESS_TOKEN}}"));

    let sbc = catalog.get("spider.browser_cloud").unwrap();
    assert!(sbc.endpoint.as_str().contains("{{env:SPIDER_CLOUD_API_KEY}}"));
    assert!(sbc.caps.fingerprint, "spider browser cloud advertises fingerprinting");
}

#[test]
fn tiers_are_ordered_within_catalog() {
    // Sanity: across the catalog, every route at tier N is cheaper-or-equal-cost
    // to every route at tier N+1. This is the cheapest-first ordering guarantee
    // for the LadderStrategy.
    let catalog = gottem_routes_builtin::register_all(RouteCatalogBuilder::new())
        .expect("load")
        .build();
    let mut max_cost_seen = 0u64;
    for tier in [Tier::T4, Tier::T5, Tier::T6, Tier::T7, Tier::T8, Tier::T9] {
        if let Some(min_cost) = catalog.at_tier(tier).iter().map(|r| r.cost).min() {
            assert!(
                min_cost >= max_cost_seen,
                "tier {tier:?} min cost {min_cost} < previous tier max {max_cost_seen}"
            );
            if let Some(max_cost) = catalog.at_tier(tier).iter().map(|r| r.cost).max() {
                max_cost_seen = max_cost_seen.max(max_cost);
            }
        }
    }
}
