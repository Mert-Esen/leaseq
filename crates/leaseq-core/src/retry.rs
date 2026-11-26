//! Retry scheduling.

use std::time::Duration;

use crate::JobId;

/// Capped exponential backoff with deterministic jitter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Delay after the first failed attempt.
    pub base: Duration,
    /// Upper bound for any delay.
    pub max: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            base: Duration::from_secs(1),
            max: Duration::from_secs(300),
        }
    }
}

impl RetryPolicy {
    /// Delay before retrying `id` after its `attempt`-th failure (starting at 1).
    ///
    /// The result lies in the upper quarter of the exponential step, so jobs that
    /// fail together spread out, while the same inputs always give the same delay.
    pub fn delay(&self, id: JobId, attempt: u32) -> Duration {
        let doublings = attempt.saturating_sub(1).min(31);
        let step = self.base.saturating_mul(1 << doublings).min(self.max);
        let millis = u64::try_from(step.as_millis()).unwrap_or(u64::MAX);
        let spread = millis / 4 + 1;
        let jitter = mix(id.0 ^ (u64::from(attempt) << 32)) % spread;
        Duration::from_millis(millis.saturating_sub(jitter))
    }
}

/// The `SplitMix64` finaliser: a fast, well-distributed integer hash.
fn mix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grows_exponentially_and_stays_bounded() {
        let policy = RetryPolicy {
            base: Duration::from_millis(100),
            max: Duration::from_secs(1),
        };
        for (attempt, step) in [
            (1, 100),
            (2, 200),
            (3, 400),
            (4, 800),
            (5, 1000),
            (99, 1000),
        ] {
            let delay = policy.delay(JobId(7), attempt).as_millis();
            assert!(
                delay <= step && delay >= step * 3 / 4,
                "attempt {attempt}: {delay}"
            );
        }
    }

    #[test]
    fn huge_values_do_not_overflow() {
        let policy = RetryPolicy {
            base: Duration::MAX,
            max: Duration::MAX,
        };
        let _ = policy.delay(JobId(u64::MAX), u32::MAX);
    }
}
