//! gottem CLI — universal scraper that reliably gets the data.
//!
//! Subcommands:
//!   gottem fetch <url>            — cheapest-first ladder (default), escalates on failure
//!   gottem probe <url>            — sequential tier walk, report which tier yields content
//!   gottem routes list            — tabular catalog dump
//!   gottem routes validate        — verify env vars for every route's auth
//!   gottem routes show <id>       — full detail for one route

#![forbid(unsafe_code)]

use std::path::PathBuf;
use std::sync::{Arc, OnceLock};
use std::time::Instant;

use anyhow::{anyhow, bail, Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use gottem_core::{
    AdapterRegistry, AuthSpec, Budget, CancelToken, Capabilities, HedgeConfig, LadderStrategy,
    Orchestrator, Route, RouteCatalog, RouteCatalogBuilder, ScrapeRequest, ScrapeResponse, Tier,
    Validator,
};
use url::Url;

// ============================================================================
// CLI definition
// ============================================================================

#[derive(Parser)]
#[command(
    name = "gottem",
    version,
    about = "Universal scraper that reliably gets the data. Tiered ladder across vendors with race + budget.",
    long_about = None,
)]
struct Cli {
    #[command(subcommand)]
    command: Cmd,

    /// Path to an additional routes TOML file, layered on top of the builtin catalog.
    /// Routes with duplicate ids in the user TOML override the builtin ones.
    #[arg(long, global = true)]
    config: Option<PathBuf>,
}

#[derive(Subcommand)]
enum Cmd {
    /// Fetch a URL using the cheapest-first ladder (or race mode).
    Fetch(FetchArgs),
    /// Try each tier in order and report which one returns valid content (mirror of spider-cli's probe_tiers.py).
    Probe(ProbeArgs),
    /// Inspect the route catalog.
    Routes {
        #[command(subcommand)]
        action: RoutesAction,
    },
}

#[derive(Subcommand)]
enum RoutesAction {
    /// List every route in the catalog, sorted by tier then cost.
    List,
    /// Verify that every route's auth env var is present in the environment.
    Validate,
    /// Show every field for one route by id.
    Show { id: String },
}

// ----- fetch ----------------------------------------------------------------

#[derive(clap::Args)]
struct FetchArgs {
    /// URL to scrape.
    url: String,

    /// Mode: ladder = cheapest-first sequential, race = parallel across selected routes.
    #[arg(long, value_enum, default_value_t = Mode::Ladder)]
    mode: Mode,

    /// Hard ceiling on per-fetch cost in milli-cents (10 = $0.001, 1000 = $0.10).
    #[arg(long, default_value_t = 1000)]
    budget_mc: u64,

    /// Lowest tier the ladder may use (0-9).
    #[arg(long, default_value_t = 0)]
    tier_min: u8,

    /// Highest tier the ladder may use (0-9).
    #[arg(long, default_value_t = 9)]
    tier_max: u8,

    /// Explicit comma-separated list of route IDs (for race mode, or to override the ladder selection).
    #[arg(long, value_delimiter = ',')]
    routes: Vec<String>,

    /// Require JS rendering — skips routes whose caps.js is false.
    #[arg(long)]
    require_js: bool,

    /// Maximum number of retries the ladder may attempt.
    #[arg(long, default_value_t = 5)]
    max_retries: u32,

    /// Hedge delay base (ms). Route 0 fires at t=0, route i fires at t = i * delay.
    /// Adaptive: the actual delay shrinks when latency EMA shows slow tails.
    #[arg(long, default_value_t = 3000)]
    hedge_delay_ms: u64,

    /// Number of hedge attempts above the primary. `hedge_count=2` fires primary + 2 backups.
    #[arg(long, default_value_t = 1)]
    hedge_count: usize,

    /// Output format.
    #[arg(long, value_enum, default_value_t = Format::Content)]
    format: Format,

    /// Print tier / route / cost / elapsed metadata to stderr before content.
    #[arg(long)]
    show_meta: bool,

    /// Run against the hosted gottem API (api.gottem.dev) instead of executing
    /// the route ladder locally. Needs an API key (--api-key or GOTTEM_API_KEY).
    #[arg(long)]
    remote: bool,

    /// gottem API key (`gtm_…`) for --remote. Falls back to $GOTTEM_API_KEY.
    #[arg(long, env = "GOTTEM_API_KEY", hide_env_values = true)]
    api_key: Option<String>,
}

#[derive(ValueEnum, Clone, Copy, Debug)]
enum Mode {
    Ladder,
    Race,
    Hedge,
}

#[derive(ValueEnum, Clone, Copy, Debug)]
enum Format {
    /// Just the extracted content (markdown/text) to stdout.
    Content,
    /// One-line JSON object with url/status/tier/route/cost/elapsed_ms/content.
    Json,
}

// ----- probe ----------------------------------------------------------------

#[derive(clap::Args)]
struct ProbeArgs {
    /// URL to probe.
    url: String,

    /// Lowest tier to try (0-9).
    #[arg(long, default_value_t = 0)]
    tier_min: u8,

    /// Highest tier to try (0-9).
    #[arg(long, default_value_t = 9)]
    tier_max: u8,

    /// Minimum content bytes for a route to count as a success (mirrors probe_tiers.py).
    #[arg(long, default_value_t = 500)]
    min_bytes: usize,

    /// Print a 200-character content preview when a route succeeds.
    #[arg(long, default_value_t = true)]
    preview: bool,
}

// ============================================================================
// main
// ============================================================================

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let config = cli.config.as_deref();
    match cli.command {
        Cmd::Fetch(args) => run_fetch(args, config).await,
        Cmd::Probe(args) => run_probe(args, config).await,
        Cmd::Routes { action } => run_routes(action, config),
    }
}

// ============================================================================
// setup: catalog + adapters — built lazily, per command
// ============================================================================
//
// Each builder is invoked only by the commands that actually need it: `routes`
// reads the catalog and never touches a network client; `fetch --remote` runs on
// the hosted API and builds neither catalog nor adapters. Constructing the adapter
// stack eagerly for every subcommand wasted a full `spider::Client` (UA generator,
// TLS config) plus a reqwest client on `routes list` and `--remote` invocations.

/// Load the route catalog: builtin routes layered with an optional user TOML.
/// This is all the `routes` subcommands need — no HTTP/spider clients constructed.
fn build_catalog(config_path: Option<&std::path::Path>) -> Result<Arc<RouteCatalog>> {
    let mut builder = RouteCatalogBuilder::new();
    builder = gottem_routes_builtin::register_all(builder)
        .map_err(|e| anyhow!("loading builtin routes: {e}"))?;
    if let Some(path) = config_path {
        let toml =
            std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        builder = builder
            .add_toml(&toml)
            .map_err(|e| anyhow!("parsing user routes from {}: {e}", path.display()))?;
    }
    Ok(Arc::new(builder.build()))
}

/// Construct the adapter stack. Eagerly builds a shared `reqwest::Client` and a
/// `spider::Client`, so this runs only for commands that actually dispatch routes
/// locally (`fetch` without `--remote`, and `probe`).
fn build_adapters() -> Arc<AdapterRegistry> {
    // One shared reqwest::Client across every HTTP-flavored adapter — one connection
    // pool, one DNS cache, one TLS session cache for the whole gottem stack. Spider
    // and Chrome have their own underlying clients (spider::Website + chromiumoxide),
    // which is unavoidable; everything else shares this pool.
    let shared_http_client = gottem_adapters_http::build_default_client();

    let mut registry = AdapterRegistry::new();
    gottem_adapters_http::register_all(&mut registry, Some(shared_http_client.clone()));
    registry.register(gottem_adapters_spider::SpiderAdapter::arc());
    #[cfg(feature = "chrome")]
    registry.register(gottem_adapters_chrome::ChromeCdpAdapter::arc());
    registry.register(
        gottem_adapters_captcha::Captcha2CaptchaAdapter::arc_with_client(
            shared_http_client.clone(),
        ),
    );
    registry.register(
        gottem_adapters_browseruse::BrowserUseAdapter::arc_with_client(shared_http_client),
    );

    Arc::new(registry)
}

// ============================================================================
// fetch
// ============================================================================

async fn run_fetch(args: FetchArgs, config_path: Option<&std::path::Path>) -> Result<()> {
    // --remote: hand the fetch to the hosted API instead of running locally. Nothing
    // local is built — no catalog, no adapters, no spider/reqwest client.
    if args.remote {
        return run_fetch_remote(args).await;
    }
    let catalog = build_catalog(config_path)?;
    let adapters = build_adapters();
    let url = Url::parse(&args.url).with_context(|| format!("invalid URL: {}", args.url))?;
    let tier_min = Tier::from_u8(args.tier_min).map_err(|e| anyhow!(e))?;
    let tier_max = Tier::from_u8(args.tier_max).map_err(|e| anyhow!(e))?;

    let mut req = ScrapeRequest::get(url);
    if args.require_js {
        req.required_caps.js = true;
    }

    let budget = Arc::new(Budget::new(args.budget_mc));
    let orch = Arc::new(Orchestrator::new(
        catalog.clone(),
        adapters.clone(),
        budget.clone(),
    ));
    let cancel = install_signal_handler();

    let started = Instant::now();
    let resp = match args.mode {
        Mode::Ladder => {
            let strategy = Arc::new(LadderStrategy::new(
                catalog.clone(),
                tier_min,
                tier_max,
                req.required_caps,
                args.max_retries,
            ));
            orch.fetch_cheap(req, strategy, cancel).await?
        }
        Mode::Race => {
            let ids: Vec<String> = if !args.routes.is_empty() {
                args.routes.clone()
            } else {
                catalog
                    .at_tier(tier_min)
                    .iter()
                    .map(|r| r.id.to_string())
                    .collect()
            };
            if ids.is_empty() {
                bail!("race mode needs --routes or at least one route at tier {tier_min:?}");
            }
            let id_refs: Vec<&str> = ids.iter().map(String::as_str).collect();
            orch.fetch_race(req, &id_refs, cancel).await?
        }
        Mode::Hedge => {
            let strategy = Arc::new(LadderStrategy::new(
                catalog.clone(),
                tier_min,
                tier_max,
                req.required_caps,
                args.max_retries,
            ));
            let hedge_cfg = HedgeConfig {
                delay: std::time::Duration::from_millis(args.hedge_delay_ms),
                max_hedges: args.hedge_count,
                enabled: true,
            };
            orch.fetch_hedge(req, strategy, hedge_cfg, cancel).await?
        }
    };
    let elapsed = started.elapsed();

    match args.format {
        Format::Content => {
            if args.show_meta {
                emit_meta_stderr(&resp, elapsed, budget.spent());
            }
            if let Some(c) = resp.content_str() {
                print!("{c}");
            }
        }
        Format::Json => {
            let v = serde_json::json!({
                "url": resp.url.as_str(),
                "status": resp.status,
                "route": resp.route_id.as_ref(),
                "tier": u8::from(resp.tier),
                "cost_milli": resp.cost_milli,
                "elapsed_ms": elapsed.as_millis() as u64,
                "content_bytes": resp.content_len(),
                "content": resp.content_str_lossy(),
                "budget_spent_milli": budget.spent(),
            });
            println!("{v}");
        }
    }
    Ok(())
}

fn emit_meta_stderr(resp: &ScrapeResponse, elapsed: std::time::Duration, budget_spent: u64) {
    eprintln!(
        "[{tier:?}] {route}  cost=${cost}  status={status}  bytes={bytes}  elapsed={ms}ms  budget_spent=${spent}",
        tier = resp.tier,
        route = resp.route_id,
        cost = fmt_cost(resp.cost_milli),
        status = resp.status,
        bytes = resp.content_len(),
        ms = elapsed.as_millis(),
        spent = fmt_cost(budget_spent),
    );
}

/// One process-wide `reqwest::Client` for `--remote` calls — built once, so
/// the connection pool / DNS cache / TLS sessions are reused.
fn http_client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(reqwest::Client::new)
}

/// `gottem fetch --remote` — run the fetch on the hosted API (api.gottem.dev)
/// instead of the local ladder. The same flags map onto the request body.
async fn run_fetch_remote(args: FetchArgs) -> Result<()> {
    let key = args
        .api_key
        .as_deref()
        .filter(|k| !k.is_empty())
        .context("--remote needs an API key — pass --api-key or set GOTTEM_API_KEY")?;
    let base =
        std::env::var("GOTTEM_API_URL").unwrap_or_else(|_| "https://api.gottem.dev".to_string());
    let mode = match args.mode {
        Mode::Ladder => "ladder",
        Mode::Race => "race",
        Mode::Hedge => "hedge",
    };
    let mut body = serde_json::json!({
        "url": args.url,
        "mode": mode,
        "require_js": args.require_js,
        "tier_min": args.tier_min,
        "tier_max": args.tier_max,
        "budget_mc": args.budget_mc,
    });
    if !args.routes.is_empty() {
        body["routes"] = serde_json::json!(args.routes);
    }

    let resp = http_client()
        .post(format!("{base}/scrape"))
        .header("authorization", format!("Bearer {key}"))
        .json(&body)
        .send()
        .await
        .context("request to the gottem API")?;
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        anyhow::bail!("gottem API returned {status}: {text}");
    }
    let parsed: serde_json::Value =
        serde_json::from_str(&text).context("parsing the gottem API response")?;

    match args.format {
        Format::Json => println!("{text}"),
        Format::Content => {
            if args.show_meta {
                eprintln!(
                    "route={} provider={} tier={} elapsed_ms={} credits_charged={}",
                    parsed["route"].as_str().unwrap_or("—"),
                    parsed["provider"].as_str().unwrap_or("—"),
                    parsed["tier"],
                    parsed["elapsed_ms"],
                    parsed["credits_charged"],
                );
            }
            println!("{}", parsed["content"].as_str().unwrap_or(""));
        }
    }
    Ok(())
}

// ============================================================================
// probe
// ============================================================================

async fn run_probe(args: ProbeArgs, config_path: Option<&std::path::Path>) -> Result<()> {
    let catalog = build_catalog(config_path)?;
    let adapters = build_adapters();
    let url = Url::parse(&args.url).with_context(|| format!("invalid URL: {}", args.url))?;
    let tier_min = Tier::from_u8(args.tier_min).map_err(|e| anyhow!(e))?;
    let tier_max = Tier::from_u8(args.tier_max).map_err(|e| anyhow!(e))?;

    let req = ScrapeRequest::get(url.clone());
    let orch = Arc::new(Orchestrator::new(
        catalog.clone(),
        adapters.clone(),
        Arc::new(Budget::unlimited()),
    ));
    let cancel = install_signal_handler();

    println!("Probing: {url}\n");

    let mut winner: Option<(Arc<Route>, ScrapeResponse, std::time::Duration)> = None;
    for tier in Tier::ALL {
        if (tier as u8) < (tier_min as u8) || (tier as u8) > (tier_max as u8) {
            continue;
        }
        for route in catalog.at_tier(tier) {
            print!(
                "  [T{tier_n}] {id} (${cost}) ... ",
                tier_n = u8::from(tier),
                id = route.id,
                cost = fmt_cost(route.cost),
            );
            use std::io::Write;
            let _ = std::io::stdout().flush();

            let started = Instant::now();
            match orch.execute_once(route, &req, 0, &cancel).await {
                Ok(resp) => {
                    let bytes = resp.content_len();
                    let elapsed = started.elapsed();
                    let validators_ok = route
                        .validate
                        .iter()
                        .all(|v| v.check(&resp.body, resp.content_str()).is_ok())
                        && bytes >= args.min_bytes;
                    if validators_ok {
                        println!("OK — {bytes} bytes ({}ms)", elapsed.as_millis());
                        if args.preview {
                            if let Some(c) = resp.content_str() {
                                let prev: String =
                                    c.chars().take(200).collect::<String>().replace('\n', " ");
                                println!("         preview: {prev}…");
                            }
                        }
                        winner = Some((route.clone(), resp, elapsed));
                        break;
                    } else {
                        println!("FAIL — short ({bytes} bytes)");
                    }
                }
                Err(e) => {
                    println!("FAIL — {e}");
                }
            }
        }
        if winner.is_some() {
            break;
        }
    }

    println!();
    match winner {
        Some((route, _, _)) => {
            println!(
                "RESULT: Use {id} at T{tier} (${cost})",
                id = route.id,
                tier = u8::from(route.tier),
                cost = fmt_cost(route.cost),
            );
            Ok(())
        }
        None => {
            println!("RESULT: all tiers exhausted, no route returned valid content");
            std::process::exit(1);
        }
    }
}

// ============================================================================
// routes list / validate / show
// ============================================================================

fn run_routes(action: RoutesAction, config_path: Option<&std::path::Path>) -> Result<()> {
    let catalog = build_catalog(config_path)?;
    match action {
        RoutesAction::List => routes_list(&catalog),
        RoutesAction::Validate => routes_validate(&catalog),
        RoutesAction::Show { id } => routes_show(&catalog, &id),
    }
}

fn routes_list(catalog: &RouteCatalog) -> Result<()> {
    println!(
        "{:<38} {:<5} {:>9} {:<22} {:<10} AUTH ENV",
        "ID", "TIER", "COST", "ADAPTER", "JS"
    );
    println!("{}", "-".repeat(110));
    let mut routes: Vec<_> = catalog.all().collect();
    // Mirror the catalog's internal sort: (tier, cost, priority, id). Spider Cloud's
    // priority=0 surfaces it ahead of equal-cost peers within a tier.
    routes.sort_by_key(|r| (r.tier, r.cost, r.priority, r.id.to_string()));
    for r in routes {
        println!(
            "{:<38} T{:<4} {:>9} {:<22} {:<10} {}",
            r.id,
            u8::from(r.tier),
            format!("${}", fmt_cost(r.cost)),
            r.adapter.as_str(),
            if r.caps.js { "yes" } else { "—" },
            auth_env_label(r),
        );
    }
    println!("\n{} routes total.", catalog.len());
    Ok(())
}

fn routes_validate(catalog: &RouteCatalog) -> Result<()> {
    let mut warnings = 0usize;
    let mut required: Vec<(String, String)> = Vec::new(); // (route_id, env_name)
    for r in catalog.all() {
        for env_name in route_env_vars(r) {
            required.push((r.id.to_string(), env_name));
        }
    }
    for (route_id, env_name) in &required {
        if std::env::var(env_name).is_err() {
            eprintln!("WARN  {route_id}: missing env var {env_name}");
            warnings += 1;
        }
    }
    let envs: std::collections::HashSet<&str> = required.iter().map(|(_, e)| e.as_str()).collect();
    println!(
        "OK    {} routes, {} unique env vars, {warnings} missing",
        catalog.len(),
        envs.len()
    );
    if warnings > 0 {
        std::process::exit(2);
    }
    Ok(())
}

fn routes_show(catalog: &RouteCatalog, id: &str) -> Result<()> {
    let r = catalog
        .get(id)
        .ok_or_else(|| anyhow!("no route with id {id:?}"))?;
    println!("id           : {}", r.id);
    println!("adapter      : {}", r.adapter.as_str());
    println!("endpoint     : {}", r.endpoint);
    println!("method       : {}", r.method.as_str());
    println!("tier         : T{}", u8::from(r.tier));
    println!("cost         : ${}", fmt_cost(r.cost));
    println!("timeout_ms   : {}", r.timeout_ms);
    println!("concurrency  : {}", r.concurrency);
    println!("auth         : {}", describe_auth(&r.auth));
    println!(
        "caps         : js={} residential={} stealth={} captcha={}",
        r.caps.js, r.caps.residential, r.caps.stealth, r.caps.captcha
    );
    if !r.headers.is_empty() {
        println!("headers      :");
        for (k, v) in &r.headers {
            println!("  {k}: {v}");
        }
    }
    println!("validators   : {}", describe_validators(&r.validate));
    Ok(())
}

// ============================================================================
// helpers
// ============================================================================

fn install_signal_handler() -> CancelToken {
    let cancel = CancelToken::new();
    let cancel_clone = cancel.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            cancel_clone.cancel();
        }
    });
    cancel
}

fn fmt_cost(milli: u64) -> String {
    let dollars = milli as f64 / 10_000.0;
    if dollars < 0.001 {
        "0.0000".into()
    } else {
        format!("{:.4}", dollars)
    }
}

fn env_vars_for_auth(auth: &AuthSpec) -> Vec<String> {
    match auth {
        AuthSpec::None => vec![],
        AuthSpec::Bearer { env } => vec![env.clone()],
        AuthSpec::ApiKey { env, .. } => vec![env.clone()],
        AuthSpec::Basic { user_env, pass_env } => {
            let mut v = vec![user_env.clone()];
            if let Some(p) = pass_env {
                v.push(p.clone());
            }
            v
        }
        AuthSpec::WsUserinfo { env } => vec![env.clone()],
    }
}

/// Extract `{{env:NAME}}` references from an endpoint template.
fn env_vars_for_template(template: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut rest = template;
    while let Some(start) = rest.find("{{env:") {
        let after = &rest[start + 6..];
        let end = match after.find("}}") {
            Some(e) => e,
            None => break,
        };
        out.push(after[..end].to_string());
        rest = &after[end + 2..];
    }
    out
}

/// Combined env-var list: AuthSpec + endpoint template references. Deduped, preserves order.
fn route_env_vars(route: &Route) -> Vec<String> {
    let mut envs = env_vars_for_auth(&route.auth);
    for e in env_vars_for_template(route.endpoint.as_str()) {
        if !envs.contains(&e) {
            envs.push(e);
        }
    }
    envs
}

fn auth_env_label(route: &Route) -> String {
    let envs = route_env_vars(route);
    if envs.is_empty() {
        "—".into()
    } else {
        envs.join(",")
    }
}

fn describe_auth(auth: &AuthSpec) -> String {
    match auth {
        AuthSpec::None => "none".into(),
        AuthSpec::Bearer { env } => format!("Bearer ${{${env}}}"),
        AuthSpec::ApiKey {
            header,
            prefix,
            env,
        } => {
            let p = prefix.as_deref().unwrap_or("");
            format!("ApiKey header={header} value='{p}${{${env}}}'")
        }
        AuthSpec::Basic { user_env, pass_env } => match pass_env {
            Some(p) => format!("Basic user=${{${user_env}}} pass=${{${p}}}"),
            None => format!("Basic user=${{${user_env}}} (key-as-user, no pass)"),
        },
        AuthSpec::WsUserinfo { env } => format!("WsUserinfo ${{${env}}}"),
    }
}

fn describe_validators(vs: &[Validator]) -> String {
    if vs.is_empty() {
        return "—".into();
    }
    vs.iter()
        .map(|v| match v {
            Validator::MinBytes { n } => format!("min_bytes={n}"),
            Validator::MaxBytes { n } => format!("max_bytes={n}"),
            Validator::MustContain { needle } => format!("must_contain={needle:?}"),
            Validator::NoWafSignature => "no_waf_signature".into(),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

#[allow(dead_code)]
fn _ensure_caps_used(_c: Capabilities) {}
