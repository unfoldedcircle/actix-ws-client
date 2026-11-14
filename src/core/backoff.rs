//! Pure, deterministic backoff policy.

use std::time::Duration;

/// Backoff configuration, extracted from [`crate::WebSocketConfig`].
#[derive(Clone, Debug)]
pub(crate) struct BackoffCfg {
    /// Delay of the first retry.
    pub initial: Duration,
    /// Upper bound for the (un-jittered) delay.
    pub max: Duration,
    /// Multiplier applied per failed attempt (>= 1.0).
    pub factor: f64,
}

/// Maximum jitter magnitude: the delay is reduced by at most 20%.
const JITTER_MAGNITUDE: f64 = 0.2;

/// Computes the backoff delay for retry `attempt` (1-based).
///
/// - The first retry (`attempt == 1`) equals `cfg.initial` (before jitter).
/// - The delay grows by `cfg.factor` per attempt and is capped at `cfg.max`.
/// - `jitter` must be in `0.0..1.0` and is supplied by the caller (production draws it
///   from `fastrand`, tests pass fixed values). It is applied as
///   `delay * (1.0 - jitter * 0.2)`, i.e. the delay is reduced by up to 20%.
pub(crate) fn backoff(attempt: u32, cfg: &BackoffCfg, jitter: f64) -> Duration {
    let exponent = attempt.saturating_sub(1);
    let factor = if cfg.factor.is_finite() {
        cfg.factor.max(1.0)
    } else {
        1.0
    };
    let initial_ms = cfg.initial.as_secs_f64() * 1000.0;
    let max_ms = cfg.max.as_secs_f64() * 1000.0;

    // Cap the exponent computation itself: factor^exponent can overflow f64 range for
    // large attempt counts; `min` against max_ms afterwards handles the +inf case too.
    let delay_ms = (initial_ms * factor.powi(exponent.min(i32::MAX as u32) as i32)).min(max_ms);

    let jitter = jitter.clamp(0.0, 1.0);
    let jittered_ms = delay_ms * (1.0 - jitter * JITTER_MAGNITUDE);

    Duration::from_millis(jittered_ms as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> BackoffCfg {
        BackoffCfg {
            initial: Duration::from_millis(500),
            max: Duration::from_secs(30),
            factor: 2.0,
        }
    }

    #[test]
    fn first_retry_equals_initial_backoff() {
        assert_eq!(backoff(1, &cfg(), 0.0), Duration::from_millis(500));
    }

    #[test]
    fn growth_is_monotonic_until_cap() {
        let cfg = cfg();
        let mut previous = Duration::ZERO;
        for attempt in 1..=20 {
            let delay = backoff(attempt, &cfg, 0.0);
            assert!(
                delay >= previous,
                "attempt {attempt}: {delay:?} < {previous:?}"
            );
            previous = delay;
        }
    }

    #[test]
    fn exponential_progression() {
        let cfg = cfg();
        assert_eq!(backoff(2, &cfg, 0.0), Duration::from_secs(1));
        assert_eq!(backoff(3, &cfg, 0.0), Duration::from_secs(2));
        assert_eq!(backoff(4, &cfg, 0.0), Duration::from_secs(4));
    }

    #[test]
    fn cap_is_respected() {
        let cfg = cfg();
        for attempt in 1..=64 {
            assert!(backoff(attempt, &cfg, 0.0) <= cfg.max);
        }
        assert_eq!(backoff(64, &cfg, 0.0), cfg.max);
    }

    #[test]
    fn jitter_reduces_delay_by_at_most_20_percent() {
        let cfg = cfg();
        // jitter = 1.0 -> exactly -20%
        assert_eq!(backoff(1, &cfg, 1.0), Duration::from_millis(400));
        // jitter = 0.5 -> -10%
        assert_eq!(backoff(1, &cfg, 0.5), Duration::from_millis(450));
        // out-of-range values are clamped
        assert_eq!(backoff(1, &cfg, 2.0), Duration::from_millis(400));
        assert_eq!(backoff(1, &cfg, -1.0), Duration::from_millis(500));
    }

    #[test]
    fn attempt_zero_is_treated_as_first_attempt() {
        assert_eq!(backoff(0, &cfg(), 0.0), Duration::from_millis(500));
    }

    #[test]
    fn factor_below_one_is_clamped() {
        let cfg = BackoffCfg {
            initial: Duration::from_millis(500),
            max: Duration::from_secs(30),
            factor: 0.5,
        };
        assert_eq!(backoff(5, &cfg, 0.0), Duration::from_millis(500));
    }
}
