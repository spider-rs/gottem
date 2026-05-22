# gottem-adapters-browseruse

Browser Use Cloud adapter for [gottem](https://github.com/spider-rs/gottem): an AI browser agent exposed as a composable `Adapter`.

## What it does

Browser Use Cloud runs a natural-language browser task and returns the agent's final output. Its API is async-only, so this crate handles the full submit-then-poll state machine inside one `Adapter::execute` call:

1. `POST /api/v1/run-task` — submit a natural-language task, receive `{"id": "..."}`.
2. `GET /api/v1/task/{id}` — poll until `status == "finished"`, then read `output`.

It mirrors the 2Captcha adapter's submit-then-poll pattern but with longer timing defaults, since AI agent runs typically take one to five minutes.

The submission body comes from the route's `BodyTemplate`, so `{{url}}`, `{{method}}`, and `{{env:NAME}}` placeholders all work as they do for `http_json`. The poll loop is bounded, and every `.await` is guarded by the orchestrator's `CancelToken`.

## Example route

```toml
[route.body]
kind     = "json"
template = '''{"task":"Visit {{url}} and return the main content as markdown.","use_proxy":true}'''
```

## Part of gottem

One of the adapter crates of the [gottem](https://github.com/spider-rs/gottem) workspace.

## License

Apache-2.0 OR MIT.
