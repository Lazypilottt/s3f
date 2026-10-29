//! Key-space arithmetic for partitioned listing.
//!
//! S3 returns keys in UTF-8 byte order, so the space of possible keys is a
//! totally ordered line. To list it in parallel, we cut that line into
//! disjoint ranges and list each one with `start-after`.
//!
//! Split points come from treating key suffixes as numbers. The digits are
//! not raw bytes but ranks in the [`Alphabet`] actually seen in a page of
//! keys: hex hashes become base-17 numbers, zero-padded counters base-11,
//! and so on. That keeps estimates inside the populated part of the space
//! instead of drifting into byte values no key uses.
//!
//! Estimates only decide *where* to cut, never *what* is listed: every
//! point is re-checked against the range it splits, an empty range costs a
//! single request, and a dense one splits again later.

/// Characters of the shared prefix to back off before extrapolating, so
/// estimates can carry into the next "digit" instead of stopping at the
/// edge of the subtree the page came from.
const CARRY_ROOM: usize = 4;

/// Upper bound on digits per number, whatever the alphabet size.
const MAX_DIGITS: usize = 32;

/// The printable-ASCII characters seen in a set of keys, used as digits.
///
/// Bytes outside the alphabet (other ASCII, control bytes, UTF-8
/// continuation bytes) map to the closest alphabet character below them,
/// which keeps the mapping order-preserving. Split points are built only
/// from alphabet characters, so they are always valid keys.
#[derive(Debug, Clone)]
pub(crate) struct Alphabet {
    /// Sorted, unique characters.
    chars: Vec<u8>,
    /// Digits per number: the most that fit in a `u128` in this base.
    digits: usize,
}

impl Alphabet {
    /// Collect the alphabet from `keys`, ignoring their first `skip` bytes.
    pub(crate) fn from_keys<K: AsRef<str>>(keys: impl IntoIterator<Item = K>, skip: usize) -> Self {
        let mut seen = [false; 128];
        for key in keys {
            for &b in key.as_ref().as_bytes().iter().skip(skip) {
                if let Some(slot) = seen.get_mut(usize::from(b)) {
                    *slot = true;
                }
            }
        }
        let chars: Vec<u8> = (0x20_u8..=0x7E).filter(|&b| seen[usize::from(b)]).collect();

        let base = u128::try_from(chars.len()).unwrap_or(u128::MAX).saturating_add(1);
        let mut digits = 0;
        let mut capacity: u128 = 1;
        while digits < MAX_DIGITS
            && let Some(next) = capacity.checked_mul(base)
        {
            capacity = next;
            digits = digits.saturating_add(1);
        }

        Self {
            chars,
            digits,
        }
    }

    /// Number base: one digit per character plus `0` for "end of key".
    fn base(&self) -> u128 {
        u128::try_from(self.chars.len()).unwrap_or(u128::MAX).saturating_add(1)
    }

    /// Digit value of byte `b`: 1 + rank of the largest character `<= b`,
    /// or 0 if `b` sorts below the whole alphabet.
    fn digit(&self, b: u8) -> u128 {
        let rank = self.chars.partition_point(|&c| c <= b);
        u128::try_from(rank).unwrap_or(0)
    }

    /// Read `suffix` as a fixed-width number in this alphabet.
    fn to_num(&self, suffix: &[u8]) -> u128 {
        let base = self.base();
        let mut n: u128 = 0;
        for i in 0..self.digits {
            let d = suffix.get(i).map_or(0, |&b| self.digit(b));
            n = n.saturating_mul(base).saturating_add(d);
        }
        n
    }

    /// Inverse of [`to_num`](Self::to_num), appended to `base_key`.
    ///
    /// Trailing zero digits are dropped (a shorter key). A zero digit in the
    /// middle has no exact spelling, so it becomes the smallest character —
    /// slightly larger than intended, which callers' range checks absorb.
    fn to_key(&self, base_key: &str, mut n: u128) -> String {
        let base = self.base();
        let mut digits = vec![0_u128; self.digits];
        for slot in digits.iter_mut().rev() {
            *slot = n.checked_rem(base).unwrap_or(0);
            n = n.checked_div(base).unwrap_or(0);
        }
        let len = digits.iter().rposition(|d| *d != 0).map_or(0, |i| i.saturating_add(1));

        let mut out = String::with_capacity(base_key.len().saturating_add(len));
        out.push_str(base_key);
        for &d in digits.iter().take(len) {
            let idx = usize::try_from(d).unwrap_or(0).saturating_sub(1);
            if let Some(&c) = self.chars.get(idx) {
                out.push(char::from(c));
            }
        }
        out
    }
}

/// Length of the longest common prefix of `a` and `b`, in bytes.
fn common_prefix_len(a: &str, b: &str) -> usize {
    a.bytes().zip(b.bytes()).take_while(|(x, y)| x == y).count()
}

/// Largest char boundary of `s` at or below `i`.
fn floor_boundary(s: &str, i: usize) -> usize {
    let mut i = i.min(s.len());
    while !s.is_char_boundary(i) {
        i = i.saturating_sub(1);
    }
    i
}

/// Keep only points strictly between `lo` and `hi`, strictly increasing.
fn monotonic(points: Vec<String>, lo: &str, hi: Option<&str>) -> Vec<String> {
    let mut out: Vec<String> = Vec::with_capacity(points.len());
    for p in points {
        let above = out.last().map_or(lo, String::as_str);
        if p.as_str() > above && hi.is_none_or(|h| p.as_str() < h) {
            out.push(p);
        }
    }
    out
}

/// Up to `count` split points evenly spaced strictly between `lo` and `hi`.
///
/// `first..=lo` is the page just read, which gives the local key density.
/// Pieces are kept at least `min_pages` pages wide by that estimate, since
/// every range costs one extra request that reads past its end.
pub(crate) fn interpolate(
    first: &str, lo: &str, hi: &str, count: usize, min_pages: u128, alphabet: &Alphabet,
) -> Vec<String> {
    let cut = floor_boundary(lo, common_prefix_len(lo, hi));
    let (Some(base_key), Some(lo_rest), Some(hi_rest)) =
        (lo.get(..cut), lo.as_bytes().get(cut..), hi.as_bytes().get(cut..))
    else {
        return Vec::new();
    };
    // If the page started outside this subtree, it was wider than the whole
    // remaining range: not worth splitting.
    let Some(first_rest) = first.as_bytes().get(cut..).filter(|_| first.starts_with(base_key))
    else {
        return Vec::new();
    };
    let (f, l, h) =
        (alphabet.to_num(first_rest), alphabet.to_num(lo_rest), alphabet.to_num(hi_rest));
    let span = h.saturating_sub(l);
    let page_width = l.saturating_sub(f).max(1);
    let est_pages = span.checked_div(page_width).unwrap_or(0);
    let by_density = est_pages.checked_div(min_pages.max(1)).unwrap_or(0);
    let pieces = u128::try_from(count).unwrap_or(u128::MAX).saturating_add(1).min(by_density);
    let step = span.checked_div(pieces).unwrap_or(0);
    if pieces < 2 || step == 0 {
        return Vec::new();
    }

    let points = (1..pieces)
        .map_while(|i| step.checked_mul(i).and_then(|d| l.checked_add(d)))
        .map(|n| alphabet.to_key(base_key, n))
        .collect();
    monotonic(points, lo, Some(hi))
}

/// Up to `count` split points past `last`, each `stride` pages apart,
/// assuming keys keep the density seen between `first` and `last`.
///
/// `floor` is the length of the listing prefix: every key starts with it,
/// so arithmetic never needs to reach further left.
pub(crate) fn extrapolate(
    first: &str, last: &str, count: usize, stride: u128, floor: usize, alphabet: &Alphabet,
) -> Vec<String> {
    let shared = common_prefix_len(first, last);
    let cut = floor_boundary(last, shared.saturating_sub(CARRY_ROOM).max(floor));
    let (Some(base_key), Some(first_rest), Some(last_rest)) =
        (last.get(..cut), first.as_bytes().get(cut..), last.as_bytes().get(cut..))
    else {
        return Vec::new();
    };
    let (f, l) = (alphabet.to_num(first_rest), alphabet.to_num(last_rest));
    let gap = l.saturating_sub(f).saturating_mul(stride);
    if gap == 0 {
        return Vec::new();
    }

    let count = u128::try_from(count).unwrap_or(u128::MAX);
    let points = (1..=count)
        .map_while(|i| gap.checked_mul(i).and_then(|d| l.checked_add(d)))
        .map(|n| alphabet.to_key(base_key, n))
        .collect();
    monotonic(points, last, None)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn alpha(keys: &[&str]) -> Alphabet {
        Alphabet::from_keys(keys.iter().copied(), 0)
    }

    fn assert_valid_splits(points: &[String], lo: &str, hi: Option<&str>) {
        assert!(points.windows(2).all(|w| w[0] < w[1]), "not increasing: {points:?}");
        assert!(points.iter().all(|p| p.as_str() > lo), "not above {lo}: {points:?}");
        if let Some(hi) = hi {
            assert!(points.iter().all(|p| p.as_str() < hi), "not below {hi}: {points:?}");
        }
    }

    #[test]
    fn alphabet_maps_in_order() {
        let a = alpha(&["0123456789abcdef"]);
        assert_eq!(a.base(), 17);
        assert!(a.to_num(b"0") < a.to_num(b"1"));
        assert!(a.to_num(b"9") < a.to_num(b"a"));
        assert!(a.to_num(b"f") < a.to_num(b"f0"));
        // Bytes outside the alphabet fall back to the nearest one below.
        assert_eq!(a.digit(b'g'), a.digit(b'f'));
        assert_eq!(a.digit(b' '), 0);
    }

    #[test]
    fn to_key_round_trips() {
        let a = alpha(&["0123456789"]);
        let n = a.to_num(b"4096");
        assert_eq!(a.to_key("p/", n), "p/4096");
    }

    #[test]
    fn interpolate_numbered_keys_evenly() {
        let a = alpha(&["file-0123456789"]);
        let points = interpolate("file-000000", "file-000000", "file-900000", 2, 1, &a);
        assert_eq!(points, vec!["file-300000", "file-600000"]);
    }

    #[test]
    fn interpolate_points_are_strictly_inside() {
        let a = alpha(&["abcdefghijklmnopqrstuvwxyz"]);
        let points = interpolate("a", "a", "b", 8, 1, &a);
        assert_eq!(points.len(), 8);
        assert_valid_splits(&points, "a", Some("b"));
    }

    #[test]
    fn interpolate_adjacent_keys_yields_nothing() {
        let a = alpha(&["ab"]);
        assert!(interpolate("a", "a", "a", 4, 1, &a).is_empty());
        assert!(interpolate("a", "b", "a", 4, 1, &a).is_empty());
    }

    #[test]
    fn interpolate_respects_density() {
        // The page just read covered 000..100, so ~4 pages remain before
        // 500: one cut keeps both pieces >= 2 pages wide.
        let a = alpha(&["0123456789"]);
        assert_eq!(interpolate("000", "100", "500", 15, 2, &a), vec!["300"]);
        assert!(interpolate("000", "100", "200", 15, 2, &a).is_empty());
    }

    #[test]
    fn extrapolate_counter_keys() {
        // A page covering 0000..0999 predicts ranges near 1000-wide strides.
        let keys: Vec<String> = (0..1000).map(|i| format!("logs/{i:08}.json")).collect();
        let a = Alphabet::from_keys(keys.iter().map(String::as_str), 5);
        let points = extrapolate(&keys[0], &keys[999], 3, 1, 5, &a);
        assert_eq!(points.len(), 3);
        assert_valid_splits(&points, &keys[999], None);
        // Each point should land within a few strides of the page.
        assert!(points.iter().all(|p| p.as_str() < "logs/00010000"), "{points:?}");
    }

    #[test]
    fn extrapolate_hex_keys_carries_past_first_digit() {
        // First page of uniformly spread hashes: covers 0000.. to 0ccc..,
        // so the next ranges must start at 1.., 2.., not stay under 0.
        let a = alpha(&["0123456789abcdef"]);
        let points = extrapolate("0001", "0ccc", 4, 1, 0, &a);
        assert_eq!(points.len(), 4);
        assert_valid_splits(&points, "0ccc", None);
        assert!(points.last().unwrap().as_str() > "2", "{points:?}");
        assert!(points.last().unwrap().as_str() < "4", "{points:?}");
    }

    #[test]
    fn extrapolate_same_key_yields_nothing() {
        let a = alpha(&["k"]);
        assert!(extrapolate("k", "k", 4, 1, 0, &a).is_empty());
    }

    #[test]
    fn extrapolate_never_leaves_the_listing_prefix_floor() {
        let a = alpha(&["0123456789"]);
        let points = extrapolate("p/10", "p/19", 4, 1, 2, &a);
        assert!(points.iter().all(|p| p.starts_with("p/")), "{points:?}");
    }

    #[test]
    fn non_ascii_prefix_is_kept() {
        let keys = ["données/0001", "données/0999"];
        let a = Alphabet::from_keys(keys, "données/".len());
        let points = extrapolate(keys[0], keys[1], 2, 1, "données/".len(), &a);
        assert_eq!(points.len(), 2);
        assert!(points.iter().all(|p| p.starts_with("données/")));
        assert_valid_splits(&points, keys[1], None);
    }

    #[test]
    fn split_inside_multibyte_char_stays_valid() {
        // "è" (C3 A8) and "é" (C3 A9) share a lead byte; the base key must
        // not keep half a character.
        let a = alpha(&["a"]);
        let points = interpolate("a", "a\u{e8}", "a\u{e9}", 1, 1, &a);
        assert_valid_splits(&points, "a\u{e8}", Some("a\u{e9}"));
    }

    #[test]
    fn empty_alphabet_is_harmless() {
        let a = Alphabet::from_keys(core::iter::empty::<&str>(), 0);
        assert!(extrapolate("a", "b", 4, 1, 0, &a).is_empty());
        assert!(interpolate("a", "a", "b", 4, 1, &a).is_empty());
    }
}
