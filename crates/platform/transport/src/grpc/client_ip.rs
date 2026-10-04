//! The client's IP address behind trusted proxies.
//!
//! Each trusted proxy in front of the server (the ALB: one) **appends** the address it
//! saw to `X-Forwarded-For`; entries further left are whatever the client sent and are
//! never trusted. With `trusted_hops` proxies, the client is the entry that many places
//! from the right. Without the header (a direct connection: the mesh, local runs), the
//! peer address is the client.

use std::net::{IpAddr, SocketAddr};

use http::HeaderMap;

/// Default number of trusted proxies appending to `X-Forwarded-For` (the ALB).
pub const DEFAULT_TRUSTED_PROXY_HOPS: usize = 1;

const FORWARDED_FOR: &str = "x-forwarded-for";

/// The client address from `headers` (see the module docs), else `peer`. `None` when the
/// header has fewer entries than `trusted_hops`, or the entry is not an address: the
/// caller treats the client as unknown rather than trusting a client-written entry.
pub fn client_ip(headers: &HeaderMap, peer: Option<SocketAddr>, trusted_hops: usize) -> Option<IpAddr> {
    let forwarded: Vec<&str> = headers
        .get_all(FORWARDED_FOR)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .collect();
    if trusted_hops > 0 && !forwarded.is_empty() {
        return forwarded
            .len()
            .checked_sub(trusted_hops)
            .and_then(|i| forwarded.get(i))
            .and_then(|v| v.parse().ok());
    }
    peer.map(|addr| addr.ip())
}

/// [`client_ip`] for a tonic request (metadata + peer address).
pub fn request_client_ip<T>(request: &tonic::Request<T>, trusted_hops: usize) -> Option<IpAddr> {
    client_ip(request.metadata().as_ref(), request.remote_addr(), trusted_hops)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(values: &[&str]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for v in values {
            h.append(FORWARDED_FOR, v.parse().unwrap());
        }
        h
    }

    #[test]
    fn the_client_is_the_entry_the_trusted_proxy_appended() {
        // A client spoofing the header: the ALB appends the real address last.
        let h = headers(&["6.6.6.6, 203.0.113.9"]);
        assert_eq!(client_ip(&h, None, 1), Some("203.0.113.9".parse().unwrap()));
        assert_eq!(client_ip(&h, None, 2), Some("6.6.6.6".parse().unwrap()));
        assert_eq!(client_ip(&h, None, 3), None, "fewer entries than trusted hops: unknown");
        assert_eq!(client_ip(&headers(&["6.6.6.6", "2001:db8::1"]), None, 1), Some("2001:db8::1".parse().unwrap()));
        assert_eq!(client_ip(&headers(&["garbage"]), None, 1), None);
    }

    #[test]
    fn without_the_header_the_peer_is_the_client() {
        let peer: SocketAddr = "10.1.2.3:51000".parse().unwrap();
        assert_eq!(client_ip(&HeaderMap::new(), Some(peer), 1), Some(peer.ip()));
        assert_eq!(client_ip(&HeaderMap::new(), None, 1), None);
        // Zero trusted hops: the header is ignored altogether.
        assert_eq!(client_ip(&headers(&["6.6.6.6"]), Some(peer), 0), Some(peer.ip()));
    }
}
