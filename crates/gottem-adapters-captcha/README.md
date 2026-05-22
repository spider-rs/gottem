# gottem-adapters-captcha

CAPTCHA solver adapter for [gottem](https://github.com/spider-rs/gottem): the 2Captcha service exposed as a composable `Adapter`. Tier T9.

## What it does

Solving a CAPTCHA is a two-step protocol, so it does not fit the single-request HTTP adapters. This crate handles the full submit-then-poll state machine inside one `Adapter::execute` call:

1. `POST https://2captcha.com/in.php` — submit the challenge, receive a task id.
2. `GET https://2captcha.com/res.php?action=get&id=<task>` — poll until the solver returns `OK|<token>`.
3. Return the token as `content` on the `ScrapeResponse`.

The poll loop is bounded (`max_polls × poll_interval`, never infinite), every `.await` is guarded by the orchestrator's `CancelToken`, and missing env or extras map to typed `FetchError`s rather than panics.

## Composing a solver chain

`captcha.2captcha` is a route like any other. The typical pattern:

1. Run the primary fetch through the ladder.
2. Detect a CAPTCHA challenge in the response.
3. Call the captcha route, passing `siteKey` + `captchaType` (`recaptcha_v2`, `hcaptcha`, `turnstile`) in `req.extra`.
4. Replay the original request with the solved token embedded (cookie / form field / header — vendor-specific).

## Part of gottem

One of the adapter crates of the [gottem](https://github.com/spider-rs/gottem) workspace.

## License

Apache-2.0 OR MIT.
