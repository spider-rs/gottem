use serde::{Deserialize, Serialize};

/// Capabilities a route can provide or a request can require.
///
/// Used by [`LadderStrategy`](crate::LadderStrategy) to filter routes: a request that
/// requires `js = true` will skip routes whose [`Capabilities::js`] is false.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(default)]
pub struct Capabilities {
    pub js: bool,
    /// Exits through a residential / consumer-ISP proxy pool.
    pub residential: bool,
    /// Exits through a shared **datacenter** proxy pool — i.e. the vendor
    /// fetches from its own infrastructure IPs, never the caller's. Distinct
    /// from `residential`: a request can require "any proxy, datacenter is
    /// fine" without paying residential rates.
    pub datacenter_proxy: bool,
    /// The route can target a specific country — set on routes that declare a
    /// [`GeoSpec`](crate::route::GeoSpec) mapping, so a request carrying
    /// `geo` only ladders onto routes that will actually honor it.
    pub geo: bool,
    pub captcha: bool,
    pub stealth: bool,
    pub fingerprint: bool,
}

impl Capabilities {
    /// Whether `provided` satisfies `self`'s required caps (each required cap is present).
    pub fn satisfied_by(&self, provided: &Capabilities) -> bool {
        (!self.js || provided.js)
            && (!self.residential || provided.residential)
            && (!self.datacenter_proxy || provided.datacenter_proxy)
            && (!self.geo || provided.geo)
            && (!self.captcha || provided.captcha)
            && (!self.stealth || provided.stealth)
            && (!self.fingerprint || provided.fingerprint)
    }
}
