//! Time sources.
//!
//! The engine reads "now" for retention cutoffs, `now()` in queries and
//! default write timestamps. Tests inject a [`ManualClock`] so those paths are
//! deterministic.

use std::fmt;
use std::sync::atomic::{AtomicI64, Ordering};

use crate::model::{now_nanos, Timestamp};

/// Source of the current time in nanoseconds since the Unix epoch.
pub trait Clock: Send + Sync {
    /// Current time.
    fn now(&self) -> Timestamp;
}

/// Wall-clock time.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Timestamp {
        now_nanos()
    }
}

/// A clock that only moves when told to.
#[derive(Default)]
pub struct ManualClock {
    now: AtomicI64,
}

impl ManualClock {
    /// Create a clock frozen at `now`.
    pub fn new(now: Timestamp) -> Self {
        ManualClock {
            now: AtomicI64::new(now),
        }
    }

    /// Jump to `now`.
    pub fn set(&self, now: Timestamp) {
        self.now.store(now, Ordering::SeqCst);
    }

    /// Move forward by `delta` nanoseconds.
    pub fn advance(&self, delta: i64) {
        self.now.fetch_add(delta, Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Timestamp {
        self.now.load(Ordering::SeqCst)
    }
}

impl fmt::Debug for ManualClock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ManualClock").field("now", &self.now()).finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manual_clock_moves_only_when_told() {
        let c = ManualClock::new(100);
        assert_eq!(c.now(), 100);
        c.advance(5);
        assert_eq!(c.now(), 105);
        c.set(-1);
        assert_eq!(c.now(), -1);
    }

    #[test]
    fn system_clock_is_after_2020() {
        assert!(SystemClock.now() > 1_577_836_800_000_000_000);
    }
}
