# gottem-adapters-captcha

CAPTCHA solver adapter for [gottem](https://github.com/spider-rs/gottem). The 2Captcha service exposed as a composable `Adapter`, tier T9.

## What it does

Solving a CAPTCHA is a two-step protocol, so it doesn't fit the single-request HTTP adapters. This crate handles the whole submit-then-poll state machine inside one `Adapter::execute` call:

1. `POST https://2captcha.com/in.php` submits the challenge and returns a task id.
2. `GET https://2captcha.com/res.php?action=get&id=<task>` polls until the solver returns `OK|<token>`.
3. The token comes back as `content` on the `ScrapeResponse`.

The poll loop is bounded by `max_polls × poll_interval` and never runs forever, the orchestrator's `CancelToken` guards every `.await`, and missing env or extras map to typed `FetchError`s rather than panics.

## Composing a solver chain

`captcha.2captcha` is a route like any other. The usual pattern:

1. Run the primary fetch through the ladder.
2. Detect a CAPTCHA challenge in the response.
3. Call the captcha route, passing `siteKey` and `captchaType` (`recaptcha_v2`, `hcaptcha`, `turnstile`) in `req.extra`.
4. Replay the original request with the solved token embedded, as a cookie, form field, or header, depending on the vendor.

## Part of gottem

One of the adapter crates in the [gottem](https://github.com/spider-rs/gottem) workspace.

## License

Apache-2.0 OR MIT.
