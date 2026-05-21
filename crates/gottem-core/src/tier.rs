use serde::{Deserialize, Serialize};

/// The escalation ladder. Lower tiers are cheaper and faster; higher tiers can
/// bypass increasingly aggressive anti-bot defenses at increasing cost.
///
/// Built-in built-up convention (not enforced by core):
///
/// | Tier | Typical providers | Cost ⁄ page |
/// |------|-------------------|-------------|
/// | T0   | Direct reqwest                                         | $0         |
/// | T1   | reqwest + datacenter proxy rotation                    | $0.0001    |
/// | T2   | reqwest + residential proxy rotation                   | $0.001     |
/// | T3   | Local Chrome (chromiumoxide, spider::Website)          | $0 compute |
/// | T4   | Spider Cloud HTTP, Firecrawl basic                     | $0.001     |
/// | T5   | Spider Cloud Chrome, Firecrawl JS, ScrapingBee         | $0.005     |
/// | T6   | Spider Cloud Chrome + residential, ZenRows premium     | $0.0075    |
/// | T7   | Spider Smart Unblocker, Brightdata Unblocker, Zyte     | $0.01      |
/// | T8   | Brightdata Scraping Browser, Browserless CDP           | $0.015     |
/// | T9   | Oxylabs, Apify actors, CAPTCHA solver chains           | $0.05+     |
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "u8", into = "u8")]
#[repr(u8)]
pub enum Tier {
    T0 = 0,
    T1 = 1,
    T2 = 2,
    T3 = 3,
    T4 = 4,
    T5 = 5,
    T6 = 6,
    T7 = 7,
    T8 = 8,
    T9 = 9,
}

impl Tier {
    pub const ALL: [Tier; 10] = [
        Tier::T0,
        Tier::T1,
        Tier::T2,
        Tier::T3,
        Tier::T4,
        Tier::T5,
        Tier::T6,
        Tier::T7,
        Tier::T8,
        Tier::T9,
    ];

    /// One tier higher. Returns `None` at T9.
    pub fn next(self) -> Option<Tier> {
        let n = self as u8 + 1;
        if n > 9 {
            None
        } else {
            Some(Self::from_u8(n).unwrap())
        }
    }

    pub fn from_u8(n: u8) -> Result<Tier, &'static str> {
        Self::ALL
            .get(n as usize)
            .copied()
            .ok_or("tier out of range (0..=9)")
    }
}

impl TryFrom<u8> for Tier {
    type Error = &'static str;
    fn try_from(n: u8) -> Result<Self, Self::Error> {
        Self::from_u8(n)
    }
}

impl From<Tier> for u8 {
    fn from(t: Tier) -> u8 {
        t as u8
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordering_and_next() {
        assert!(Tier::T0 < Tier::T1);
        assert_eq!(Tier::T0.next(), Some(Tier::T1));
        assert_eq!(Tier::T9.next(), None);
    }

    #[test]
    fn from_u8() {
        assert_eq!(Tier::from_u8(0), Ok(Tier::T0));
        assert_eq!(Tier::from_u8(9), Ok(Tier::T9));
        assert!(Tier::from_u8(10).is_err());
    }
}
