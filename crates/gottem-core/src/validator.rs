use serde::{Deserialize, Serialize};

/// Content-quality gate applied to every successful adapter response. If any validator
/// fails, the orchestrator treats the response as if it had failed and consults the
/// retry strategy for escalation.
///
/// The default `min_bytes = 500` mirrors `spider-cli/python/probe_tiers.py` —
/// short responses are almost always WAF challenges or empty pages.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Validator {
    /// Content (or body if content is None) must be at least `n` bytes.
    MinBytes { n: usize },
    /// Content / body must be at most `n` bytes.
    MaxBytes { n: usize },
    /// Content must contain the literal `needle`.
    MustContain { needle: String },
    /// Body must NOT contain a known WAF signature.
    NoWafSignature,
}

impl Validator {
    pub fn check(&self, body: &[u8], content: Option<&str>) -> Result<(), String> {
        match self {
            Self::MinBytes { n } => {
                let len = content.map(str::len).unwrap_or(body.len());
                if len < *n {
                    Err(format!("content too short: {len} bytes < {n}"))
                } else {
                    Ok(())
                }
            }
            Self::MaxBytes { n } => {
                let len = content.map(str::len).unwrap_or(body.len());
                if len > *n {
                    Err(format!("content too long: {len} > {n}"))
                } else {
                    Ok(())
                }
            }
            Self::MustContain { needle } => {
                let hay = content.unwrap_or_else(|| std::str::from_utf8(body).unwrap_or(""));
                if hay.contains(needle.as_str()) {
                    Ok(())
                } else {
                    Err(format!("missing required substring {needle:?}"))
                }
            }
            Self::NoWafSignature => {
                // Cheap markers. For deep detection use `spider::page::AntiBotTech`.
                const MARKERS: &[&[u8]] = &[
                    b"Just a moment",
                    b"Checking your browser",
                    b"cloudflare-static",
                    b"Cloudflare Ray ID",
                    b"Distil",
                    b"DataDome",
                    b"Imperva",
                    b"PerimeterX",
                    b"Akamai",
                    b"reCAPTCHA",
                ];
                for m in MARKERS {
                    if find(body, m) {
                        return Err(format!(
                            "WAF signature: {}",
                            std::str::from_utf8(m).unwrap_or("?")
                        ));
                    }
                }
                Ok(())
            }
        }
    }
}

fn find(hay: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() {
        return true;
    }
    if hay.len() < needle.len() {
        return false;
    }
    hay.windows(needle.len()).any(|w| w == needle)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn min_bytes() {
        let v = Validator::MinBytes { n: 10 };
        assert!(v.check(b"hi", None).is_err());
        assert!(v.check(b"abcdefghijklmnop", None).is_ok());
    }

    #[test]
    fn no_waf_signature_catches_cloudflare() {
        let v = Validator::NoWafSignature;
        assert!(v.check(b"<html>Just a moment...</html>", None).is_err());
        assert!(v.check(b"<html>real content</html>", None).is_ok());
    }
}
