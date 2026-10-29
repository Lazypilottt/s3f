//! DNS resolution that spreads connections across a host's addresses.
//!
//! S3 caps throughput per connection and answers each DNS query with a
//! handful of front-end IPs out of a much larger fleet. The default
//! resolver hands those addresses to the connector in the same order every
//! time, so nearly every connection lands on the first one. This resolver
//! instead remembers every address a host has returned recently and rotates
//! the starting point on each new connection, so a pool of connections fans
//! out across many servers — the same idea the AWS Common Runtime uses to
//! reach multi-gigabit throughput.
//!
//! Only new connections consult the resolver; pooled ones are reused as-is.

use core::time::Duration;
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex, PoisonError},
    time::Instant,
};

use reqwest::dns::{Addrs, Name, Resolve, Resolving};

/// Minimum time between fresh lookups of the same host. Each lookup can
/// return new addresses, so a busy client keeps harvesting while it ramps
/// up connections.
const REFRESH_INTERVAL: Duration = Duration::from_millis(250);

/// Addresses not seen in a lookup for this long are dropped.
const ADDR_TTL: Duration = Duration::from_secs(300);

/// Most addresses remembered per host.
const MAX_ADDRS: usize = 64;

/// One remembered address.
#[derive(Debug, Clone, Copy)]
struct Seen {
    addr: SocketAddr,
    last_seen: Instant,
}

/// Everything known about one host.
#[derive(Debug)]
struct HostAddrs {
    addrs: Vec<Seen>,
    /// Rotates the starting address for each connection.
    cursor: usize,
    /// Whether the system resolver listed IPv6 first last time.
    prefer_v6: bool,
    refreshed: Instant,
}

impl HostAddrs {
    /// Merge a fresh lookup result, expiring stale addresses.
    fn merge(&mut self, fresh: &[SocketAddr], now: Instant) {
        if let Some(first) = fresh.first() {
            self.prefer_v6 = first.is_ipv6();
        }
        for addr in fresh {
            if let Some(seen) = self.addrs.iter_mut().find(|s| s.addr == *addr) {
                seen.last_seen = now;
            } else {
                self.addrs.push(Seen {
                    addr: *addr,
                    last_seen: now,
                });
            }
        }
        self.addrs.retain(|s| now.saturating_duration_since(s.last_seen) < ADDR_TTL);
        if self.addrs.len() > MAX_ADDRS {
            self.addrs.sort_by_key(|s| core::cmp::Reverse(s.last_seen));
            self.addrs.truncate(MAX_ADDRS);
        }
        self.refreshed = now;
    }

    /// All addresses, preferred family first, each family rotated so
    /// consecutive connections start on different servers. The rest stay
    /// in the list as fallbacks if the first one fails.
    fn rotated(&mut self) -> Vec<SocketAddr> {
        let (mut primary, mut fallback): (Vec<_>, Vec<_>) =
            self.addrs.iter().map(|s| s.addr).partition(|a| a.is_ipv6() == self.prefer_v6);
        let cursor = self.cursor;
        self.cursor = self.cursor.wrapping_add(1);
        for family in [&mut primary, &mut fallback] {
            if let Some(shift) = cursor.checked_rem(family.len()) {
                family.rotate_left(shift);
            }
        }
        primary.extend(fallback);
        primary
    }
}

/// A [`Resolve`] implementation that spreads connections across addresses.
#[derive(Debug, Default, Clone)]
pub(crate) struct SpreadResolver {
    hosts: Arc<Mutex<HashMap<String, HostAddrs>>>,
}

impl SpreadResolver {
    /// Rotated addresses for `host`, refreshing from the system resolver
    /// when the cached set is missing or due for a refresh.
    async fn resolve_host(&self, host: String) -> std::io::Result<Vec<SocketAddr>> {
        let now = Instant::now();
        let due = self
            .lock()
            .get(&host)
            .is_none_or(|h| now.saturating_duration_since(h.refreshed) >= REFRESH_INTERVAL);

        if due {
            match tokio::net::lookup_host((host.as_str(), 0)).await {
                Ok(found) => {
                    let fresh: Vec<SocketAddr> = found.collect();
                    self.lock()
                        .entry(host.clone())
                        .or_insert_with(|| {
                            HostAddrs {
                                addrs: Vec::new(),
                                cursor: 0,
                                prefer_v6: false,
                                refreshed: now,
                            }
                        })
                        .merge(&fresh, now);
                },
                // A failed refresh is fine while cached addresses remain.
                Err(e) if !self.lock().contains_key(&host) => return Err(e),
                Err(_) => {},
            }
        }

        let mut hosts = self.lock();
        let addrs = hosts.get_mut(&host).map(HostAddrs::rotated).unwrap_or_default();
        drop(hosts);
        if addrs.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("no addresses for {host}"),
            ));
        }
        Ok(addrs)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, HostAddrs>> {
        // The map stays consistent even if a holder panicked mid-update.
        self.hosts.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Resolve for SpreadResolver {
    #[inline]
    fn resolve(&self, name: Name) -> Resolving {
        let this = self.clone();
        let host = name.as_str().to_owned();
        Box::pin(async move {
            let addrs: Addrs = Box::new(this.resolve_host(host).await?.into_iter());
            Ok(addrs)
        })
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    use super::*;

    fn v4(last: u8) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, last)), 0)
    }

    fn v6(last: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V6(Ipv6Addr::new(0xFD00, 0, 0, 0, 0, 0, 0, last)), 0)
    }

    fn host() -> HostAddrs {
        HostAddrs {
            addrs: Vec::new(),
            cursor: 0,
            prefer_v6: false,
            refreshed: Instant::now(),
        }
    }

    #[test]
    fn harvests_addresses_across_lookups() {
        let mut h = host();
        let now = Instant::now();
        h.merge(&[v4(1), v4(2)], now);
        h.merge(&[v4(2), v4(3)], now);
        assert_eq!(h.addrs.len(), 3);
    }

    #[test]
    fn rotation_starts_each_connection_elsewhere() {
        let mut h = host();
        h.merge(&[v4(1), v4(2), v4(3)], Instant::now());
        let firsts: Vec<SocketAddr> = (0..3).map(|_| h.rotated()[0]).collect();
        assert_eq!(firsts, vec![v4(1), v4(2), v4(3)]);
        // Every rotation still offers all addresses as fallbacks.
        assert_eq!(h.rotated().len(), 3);
    }

    #[test]
    fn preferred_family_comes_first() {
        let mut h = host();
        h.merge(&[v6(1), v4(1), v6(2)], Instant::now());
        let order = h.rotated();
        assert!(order[0].is_ipv6() && order[1].is_ipv6());
        assert!(order[2].is_ipv4());
    }

    #[test]
    fn stale_addresses_expire_and_set_is_bounded() {
        let mut h = host();
        let start = Instant::now();
        h.merge(&[v4(1)], start);
        let later = start + ADDR_TTL + Duration::from_secs(1);
        h.merge(&[v4(2)], later);
        assert_eq!(h.addrs.iter().map(|s| s.addr).collect::<Vec<_>>(), vec![v4(2)]);

        let many: Vec<SocketAddr> = (0..=u8::MAX).map(v4).collect();
        h.merge(&many, later);
        assert_eq!(h.addrs.len(), MAX_ADDRS);
    }

    #[tokio::test]
    async fn resolves_localhost() {
        let r = SpreadResolver::default();
        let addrs = r.resolve_host("localhost".into()).await.unwrap();
        assert!(addrs.iter().all(|a| a.ip().is_loopback()));
    }

    #[tokio::test]
    async fn unknown_host_errors() {
        let r = SpreadResolver::default();
        assert!(r.resolve_host("does-not-exist.invalid".into()).await.is_err());
    }
}
