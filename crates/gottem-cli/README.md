# gottem-cli

The `gottem` command-line binary: a universal scraper that reliably gets the data.

## What it does

`gottem` talks to every major scraping vendor — and your local browser — through a single tiered ladder. You give it a URL; it tries the cheapest route first, escalates when blocked, races vendors when speed matters, and stops when it gets clean content. Powered by [`spider`](https://github.com/spider-rs/spider).

This crate is the CLI front end for the [gottem](https://github.com/spider-rs/gottem) library workspace.

## Install

```bash
cargo install gottem-cli
```

This installs the `gottem` binary on your PATH.

## Subcommands

```
gottem fetch <url>        cheapest-first ladder (default), escalates on failure
gottem probe <url>        sequential tier walk, report which tier yields content
gottem routes list        tabular catalog dump
gottem routes show <id>   full detail for one route
gottem routes validate    verify env vars for every route's auth
```

## Examples

```bash
# Inspect what's available — no API keys needed.
gottem routes list
gottem routes show spider.cloud.smart

# Tell gottem which vendor keys you have, then fetch.
export FIRECRAWL_API_KEY=fc-...
gottem fetch https://example.com --show-meta

# Race routes in parallel — fastest valid response wins.
gottem fetch https://example.com --mode race --routes firecrawl.scrape,spider.cloud.http,zenrows.basic

# Hedge: start cheap, fire a staggered backup after a delay.
gottem fetch https://example.com --mode hedge --hedge-delay-ms 2000

# Cap cost and pin the tier band.
gottem fetch https://example.com --budget-mc 100 --tier-min 4 --tier-max 7

# Probe every tier on a target URL.
gottem probe https://hard-to-scrape.test

# Layer custom routes on top of the builtin catalog.
gottem --config routes.toml fetch https://example.com
```

## Run on the hosted API — `--remote`

Don't want to manage vendor keys or run a browser? Point the CLI at the hosted
API ([gottem.dev](https://gottem.dev)) with `--remote` — same flags, run on
`api.gottem.dev`:

```bash
export GOTTEM_API_KEY=gtm_your_key_here   # create a key at gottem.dev

gottem fetch --remote https://example.com
gottem fetch --remote --mode race --show-meta https://example.com
gottem fetch --remote --format json https://example.com

# Or pass the key explicitly:
gottem fetch --remote --api-key gtm_your_key_here https://example.com
```

`$GOTTEM_API_URL` overrides the base URL (default `https://api.gottem.dev`).

## Part of gottem

The CLI of the [gottem](https://github.com/spider-rs/gottem) workspace.

## License

Apache-2.0 OR MIT.
