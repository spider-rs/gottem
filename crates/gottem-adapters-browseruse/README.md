# gottem-adapters-browseruse

Browser Use Cloud adapter for [gottem](https://github.com/spider-rs/gottem). An AI browser agent exposed as a composable `Adapter`.

## What it does

Browser Use Cloud runs a natural-language browser task and returns the agent's final output. Its API is async-only, so this crate handles the whole submit-then-poll state machine inside one `Adapter::execute` call:

1. `POST /api/v1/run-task` submits a natural-language task and returns `{"id": "..."}`.
2. `GET /api/v1/task/{id}` polls until `status == "finished"`, then reads `output`.

It mirrors the 2Captcha adapter's submit-then-poll pattern with longer timing defaults, since AI agent runs usually take one to five minutes.

The submission body comes from the route's `BodyTemplate`, so `{{url}}`, `{{method}}`, and `{{env:NAME}}` placeholders work exactly as they do for `http_json`. The poll loop is bounded, and the orchestrator's `CancelToken` guards every `.await`.

## Example route

```toml
[route.body]
kind     = "json"
template = '''{"task":"Visit {{url}} and return the main content as markdown.","use_proxy":true}'''
```

## Part of gottem

One of the adapter crates in the [gottem](https://github.com/spider-rs/gottem) workspace.

## License

Apache-2.0 OR MIT.
