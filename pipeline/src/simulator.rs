//! What a `simulator` build changes. It runs the liquidator against a local PocketIC
//! that the `liquidium-cross-chain-local` workbench starts and supervises.

use std::env;

use candid::Principal;

use crate::finalizers::multi_venue::{ICPSWAP_VENUE_ID, MEXC_VENUE_ID};

/// True in a `simulator` build. A default build never reads a `SIM_*` setting.
pub const ENABLED: bool = cfg!(feature = "simulator");

/// The WAL error of a settled row that keeps its collateral.
pub const RETAINED_COLLATERAL: &str =
    "simulator: liquidation settled; collateral retained, exchange finalisation disabled";

/// The PocketIC root key that the workbench exports to the file in `SIM_IC_ROOT_KEY`.
pub fn ic_root_key() -> Result<Vec<u8>, String> {
    let path = env::var("SIM_IC_ROOT_KEY").map_err(|error| format!("SIM_IC_ROOT_KEY: {error}"))?;
    std::fs::read(path).map_err(|error| format!("PocketIC root key: {error}"))
}

/// A simulator build talks only to a PocketIC on this machine.
pub fn require_loopback_ic_url(ic_url: &str) -> Result<(), String> {
    let url = reqwest::Url::parse(ic_url).map_err(|error| format!("invalid simulator IC URL: {error}"))?;
    if !is_loopback_http(&url) {
        return Err("simulator builds require a loopback PocketIC URL".to_string());
    }
    Ok(())
}

/// The local MEXC origin from `SIM_MEXC_URL`. A default build uses the real MEXC.
pub fn mexc_api_base() -> Result<Option<String>, String> {
    if !ENABLED {
        return Ok(None);
    }
    let raw = env::var("SIM_MEXC_URL").map_err(|_| "simulator MEXC requires SIM_MEXC_URL".to_string())?;
    local_origin(&raw).map(Some)
}

/// `raw` as a bare loopback HTTP origin. Signed calls and metadata reads must both
/// stay local, or route discovery would mix real and simulated books.
fn local_origin(raw: &str) -> Result<String, String> {
    let url = reqwest::Url::parse(raw).map_err(|error| format!("SIM_MEXC_URL: {error}"))?;
    let bare = url.username().is_empty()
        && url.password().is_none()
        && url.path() == "/"
        && url.query().is_none()
        && url.fragment().is_none();
    if !is_loopback_http(&url) || !bare {
        return Err("SIM_MEXC_URL must be a loopback HTTP origin".to_string());
    }
    Ok(raw.trim_end_matches('/').to_string())
}

/// Plain HTTP on 127.0.0.1, the only network a simulator build may reach.
fn is_loopback_http(url: &reqwest::Url) -> bool {
    url.scheme() == "http" && url.host_str() == Some("127.0.0.1")
}

/// ICPSwap runs with `SIM_ICPSWAP_FACTORY`, and MEXC with `SIM_MEXC_URL`.
/// With neither, the liquidator keeps the seized collateral.
pub fn enabled_swap_venues() -> Result<Vec<String>, String> {
    let mut venues = Vec::new();
    if let Ok(factory) = env::var("SIM_ICPSWAP_FACTORY") {
        Principal::from_text(&factory).map_err(|error| format!("SIM_ICPSWAP_FACTORY: {error}"))?;
        venues.push(ICPSWAP_VENUE_ID.to_string());
    }
    if env::var_os("SIM_MEXC_URL").is_some() {
        venues.push(MEXC_VENUE_ID.to_string());
    }
    Ok(venues)
}

/// The local MEXC holds ck assets itself, so it skips the production bridge
/// unless `SIM_MEXC_BRIDGE` asks for the local one.
pub fn mexc_uses_bridge() -> bool {
    !ENABLED || env::var_os("SIM_MEXC_BRIDGE").is_some()
}

#[cfg(test)]
mod tests {
    use super::{local_origin, require_loopback_ic_url};

    #[test]
    fn only_a_loopback_http_ic_url_is_accepted() {
        assert!(require_loopback_ic_url("http://127.0.0.1:8080").is_ok());
        for url in [
            "https://127.0.0.1:8080",
            "http://localhost:8080",
            "https://icp-api.io",
            "not a url",
        ] {
            assert!(require_loopback_ic_url(url).is_err(), "{url}");
        }
    }

    #[test]
    fn the_local_mexc_must_be_a_bare_loopback_origin() {
        assert_eq!(
            local_origin("http://127.0.0.1:23009/").unwrap(),
            "http://127.0.0.1:23009"
        );
        for url in [
            "https://127.0.0.1:23009",
            "http://api.mexc.com",
            "http://127.0.0.1:23009/api",
            "http://127.0.0.1:23009/?x=1",
            "http://user@127.0.0.1:23009",
        ] {
            assert!(local_origin(url).is_err(), "{url}");
        }
    }
}
