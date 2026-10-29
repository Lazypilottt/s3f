//! Hedged requests — race a backup copy of a request that is slow to answer.
//!
//! A batch transfer finishes when its slowest request does, and most slow
//! S3 requests are not slow servers but unlucky ones: a busy front end, a
//! dropped packet, a cold connection. Sent again on another connection,
//! such a request usually answers at normal speed. So when a GET has not
//! produced response headers within the 95th percentile of recent
//! latencies, a second copy goes out and whichever answers first wins
//! (Dean & Barroso, "The Tail at Scale", 2013). AWS recommends the same for
//! S3: retry slow requests aggressively, on a new connection.
//!
//! Hedges are capped at [`BUDGET_PERCENT`] of requests, so a server that is
//! slow across the board sees little extra load. Only idempotent requests
//! (GET, HEAD) are hedged; the backup is the identical signed request,
//! which S3 accepts. The backup opens or reuses a different connection,
//! and new connections rotate across S3 front-end IPs (see
//! [`super::dns`]), so it usually reaches a different server.

use core::{pin::pin, time::Duration};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex, PoisonError},
    time::Instant,
};

use futures_util::future::{Either, select};

use crate::trace::maybe_debug;

/// Latency samples kept per request class.
const WINDOW: usize = 256;

/// Samples needed before the threshold follows measured latency.
const MIN_SAMPLES: usize = 32;

/// Threshold used until enough samples exist. Generous, so warm-up only
/// hedges requests that are clearly stuck.
const INITIAL_THRESHOLD: Duration = Duration::from_secs(1);

/// Never hedge sooner than this, however fast the backend is.
const MIN_THRESHOLD: Duration = Duration::from_millis(20);

/// Never wait longer than this before hedging.
const MAX_THRESHOLD: Duration = Duration::from_secs(5);

/// How often a waiting request re-checks the threshold and budget. The
/// threshold moves as samples arrive: requests that started before there
/// were any must not stay on the warm-up value.
const RECHECK: Duration = Duration::from_millis(25);

/// Recompute the percentile after this many new samples.
const RECOMPUTE_EVERY: usize = 16;

/// Most hedges as a share of all requests.
const BUDGET_PERCENT: u32 = 10;

/// Hedges available before any have been earned.
const BUDGET_BURST: u32 = 10;

/// Requests just past their threshold may hedge only while the bank holds
/// more than this; below it, the budget is kept for requests far past
/// theirs. Early latency samples come from the fastest requests of the
/// first wave, so a cold start makes many requests look slightly slow, and
/// without this they would spend the budget a real straggler needs.
const BUDGET_RESERVE: u32 = 20;

/// A request this many times past its threshold may spend the reserve.
const LATE_FACTOR: u32 = 4;

/// Most hedges that can be banked. Earned budget must outlast a batch:
/// its requests all start early, but stragglers show up at the end.
const BUDGET_BANKED: u32 = 100;

/// Requests with different latency profiles, tracked separately.
#[derive(Debug, Clone, Copy)]
enum Class {
    /// `ListObjectsV2` pages: the server walks its index before answering.
    List,
    /// Object GETs, whole or ranged.
    Object,
}

impl Class {
    fn of(req: &reqwest::Request) -> Self {
        if req.url().query().is_some_and(|q| q.contains("list-type=")) {
            Self::List
        } else {
            Self::Object
        }
    }

    const fn index(self) -> usize {
        match self {
            Self::List => 0,
            Self::Object => 1,
        }
    }
}

/// Recent latencies of one request class.
#[derive(Debug, Default)]
struct Window {
    samples: VecDeque<Duration>,
    since_recompute: usize,
    /// Cached 95th percentile, once there are enough samples.
    p95: Option<Duration>,
}

impl Window {
    fn record(&mut self, latency: Duration) {
        if self.samples.len() == WINDOW {
            self.samples.pop_front();
        }
        self.samples.push_back(latency);
        self.since_recompute = self.since_recompute.saturating_add(1);
        if self.samples.len() >= MIN_SAMPLES
            && (self.p95.is_none() || self.since_recompute >= RECOMPUTE_EVERY)
        {
            self.since_recompute = 0;
            let mut sorted: Vec<Duration> = self.samples.iter().copied().collect();
            let idx = sorted.len().saturating_mul(95).checked_div(100).unwrap_or(0);
            let idx = idx.min(sorted.len().saturating_sub(1));
            let (_, p95, _) = sorted.select_nth_unstable(idx);
            self.p95 = Some(*p95);
        }
    }

    fn threshold(&self) -> Duration {
        self.p95.map_or(INITIAL_THRESHOLD, |p| p.clamp(MIN_THRESHOLD, MAX_THRESHOLD))
    }
}

/// Shared hedging state for one client.
#[derive(Debug)]
struct State {
    windows: [Window; 2],
    /// Budget in hundredths of a hedge: each request earns
    /// [`BUDGET_PERCENT`], each hedge costs 100.
    credits: u32,
}

impl Default for State {
    fn default() -> Self {
        Self {
            windows: [Window::default(), Window::default()],
            credits: BUDGET_BURST.saturating_mul(100),
        }
    }
}

/// Decides when to hedge, based on latencies seen so far.
#[derive(Debug, Default)]
struct Hedger {
    state: Mutex<State>,
}

impl Hedger {
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        // Counters stay usable even if a holder panicked mid-update.
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Count a request towards the hedging budget.
    fn on_request(&self) {
        let mut state = self.lock();
        state.credits =
            state.credits.saturating_add(BUDGET_PERCENT).min(BUDGET_BANKED.saturating_mul(100));
    }

    /// How long a request of `class` may take before it is hedged.
    fn threshold(&self, class: Class) -> Duration {
        self.lock().windows.get(class.index()).map_or(INITIAL_THRESHOLD, Window::threshold)
    }

    /// Take one hedge from the budget, if any is left. Only `very_late`
    /// requests may dip into the reserve.
    fn try_spend(&self, very_late: bool) -> bool {
        let floor = if very_late { 0 } else { BUDGET_RESERVE.saturating_mul(100) };
        let mut state = self.lock();
        let left = state.credits.checked_sub(100).filter(|left| *left >= floor);
        state.credits = left.unwrap_or(state.credits);
        drop(state);
        left.is_some()
    }

    fn record(&self, class: Class, latency: Duration) {
        if let Some(w) = self.lock().windows.get_mut(class.index()) {
            w.record(latency);
        }
    }
}

/// HTTP client that can hedge slow requests. Cheap to clone; clones share
/// the connection pool and the latency history.
#[derive(Debug, Clone)]
pub(crate) struct HttpClient {
    hedger: Arc<Hedger>,
    inner: reqwest::Client,
}

impl From<reqwest::Client> for HttpClient {
    #[inline]
    fn from(inner: reqwest::Client) -> Self {
        Self {
            hedger: Arc::default(),
            inner,
        }
    }
}

impl HttpClient {
    /// The underlying `reqwest` client, for building requests.
    pub(crate) const fn inner(&self) -> &reqwest::Client {
        &self.inner
    }

    /// Send `req`. With `hedge` set, an idempotent request that has not
    /// answered by the hedging threshold is sent a second time, and the
    /// first response to arrive is returned.
    ///
    /// # Errors
    ///
    /// Returns the transport error if every copy sent fails.
    pub(crate) async fn execute(
        &self, req: reqwest::Request, hedge: bool,
    ) -> reqwest::Result<reqwest::Response> {
        let idempotent = matches!(*req.method(), http::Method::GET | http::Method::HEAD);
        let backup = if hedge && idempotent { req.try_clone() } else { None };
        let Some(backup) = backup else {
            return self.inner.execute(req).await;
        };

        let class = Class::of(&req);
        self.hedger.on_request();
        let start = Instant::now();
        let mut primary = pin!(self.inner.execute(req));

        // Wait for the threshold, then for budget. A request still waiting
        // when budget frees up is the one that most needs a hedge, so
        // waiting requests keep asking.
        loop {
            let due = self.hedger.threshold(class).saturating_sub(start.elapsed());
            let nap = if due.is_zero() { RECHECK } else { due.min(RECHECK) };
            match select(primary.as_mut(), pin!(tokio::time::sleep(nap))).await {
                Either::Left((result, _)) => {
                    if result.is_ok() {
                        self.hedger.record(class, start.elapsed());
                    }
                    return result;
                },
                Either::Right(((), _)) => {},
            }
            let (waited, threshold) = (start.elapsed(), self.hedger.threshold(class));
            if waited >= threshold
                && self.hedger.try_spend(waited >= threshold.saturating_mul(LATE_FACTOR))
            {
                break;
            }
        }

        maybe_debug!(url = %backup.url(), waited = ?start.elapsed(), "hedging slow request");
        let secondary = pin!(self.inner.execute(backup));
        let (first, other) = match select(primary, secondary).await {
            Either::Left((result, other)) | Either::Right((result, other)) => (result, other),
        };
        // A copy that failed outright says nothing about the other one.
        let result = match first {
            Ok(resp) => Ok(resp),
            Err(_) => other.await,
        };
        if result.is_ok() {
            // The primary's own latency is unknown once it lost; what it
            // took to get an answer is a lower bound, which keeps the tail
            // from vanishing out of the window.
            self.hedger.record(class, start.elapsed());
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn threshold_starts_generous_then_tracks_p95() {
        let mut w = Window::default();
        assert_eq!(w.threshold(), INITIAL_THRESHOLD);
        for i in 1..=100_u64 {
            w.record(Duration::from_millis(i));
        }
        let t = w.threshold();
        assert!(t >= Duration::from_millis(90) && t <= Duration::from_millis(100), "{t:?}");
    }

    #[test]
    fn threshold_is_clamped() {
        let mut w = Window::default();
        for _ in 0..MIN_SAMPLES {
            w.record(Duration::from_micros(100));
        }
        assert_eq!(w.threshold(), MIN_THRESHOLD);
        for _ in 0..WINDOW {
            w.record(Duration::from_secs(60));
        }
        assert_eq!(w.threshold(), MAX_THRESHOLD);
    }

    #[test]
    fn window_forgets_old_samples() {
        let mut w = Window::default();
        for _ in 0..WINDOW {
            w.record(Duration::from_secs(2));
        }
        for _ in 0..WINDOW {
            w.record(Duration::from_millis(30));
        }
        assert_eq!(w.threshold(), Duration::from_millis(30));
    }

    #[test]
    fn budget_caps_hedges_at_a_share_of_requests() {
        let h = Hedger::default();
        let mut hedges = 0_u32;
        for _ in 0..1000 {
            h.on_request();
            if h.try_spend(true) {
                hedges = hedges.saturating_add(1);
            }
        }
        // The burst plus one hedge per 100 / BUDGET_PERCENT requests.
        assert!(hedges <= BUDGET_BURST + 1000 * BUDGET_PERCENT / 100, "{hedges}");
        assert!(hedges >= 1000 * BUDGET_PERCENT / 100, "{hedges}");
    }

    #[test]
    fn reserve_is_kept_for_very_late_requests() {
        let h = Hedger::default();
        assert!(!h.try_spend(false), "marginal hedge spent the reserve");
        assert!(h.try_spend(true), "very late request found no budget");

        // Bank 30 more hedges: marginal requests may spend down to the reserve.
        for _ in 0..300 {
            h.on_request();
        }
        let mut marginal = 0_u32;
        while h.try_spend(false) {
            marginal = marginal.saturating_add(1);
        }
        assert_eq!(marginal, BUDGET_BURST - 1 + 30 - BUDGET_RESERVE);
    }

    #[test]
    fn classes_are_tracked_separately() {
        let h = Hedger::default();
        for _ in 0..MIN_SAMPLES {
            h.record(Class::List, Duration::from_millis(400));
            h.record(Class::Object, Duration::from_millis(30));
        }
        assert_eq!(h.threshold(Class::List), Duration::from_millis(400));
        assert_eq!(h.threshold(Class::Object), Duration::from_millis(30));
    }
}
