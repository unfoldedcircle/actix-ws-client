//! Pure keep-alive tracker: counts consecutive unanswered pings.
//!
//! A ping counts as answered when any frame is received after it, not only its pong: a
//! peer that is sending data is alive.
//!
//! Liveness is evaluated on the ping tick only — there is no second monitor timer and
//! no moving reference instant. The detection budget is exactly
//! `max_missed_pongs * ping_interval` after the first unanswered ping.

/// Outcome of a keep-alive tick.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum KeepAliveOutcome {
    /// Send the next ping.
    SendPing,
    /// Too many consecutive pings unanswered: drop the connection and reconnect.
    Reconnect,
}

/// Tracks unanswered pings across keep-alive ticks.
#[derive(Debug)]
pub(crate) struct KeepAlive {
    awaiting_pong: bool,
    missed: u32,
    max_missed: u32,
}

impl KeepAlive {
    pub(crate) fn new(max_missed: u32) -> Self {
        Self {
            awaiting_pong: false,
            missed: 0,
            max_missed: max_missed.max(1),
        }
    }

    /// Evaluates the previous ping, then decides whether to send the next one.
    pub(crate) fn on_tick(&mut self) -> KeepAliveOutcome {
        if self.awaiting_pong {
            self.missed += 1;
            if self.missed >= self.max_missed {
                return KeepAliveOutcome::Reconnect;
            }
        }
        self.awaiting_pong = true;
        KeepAliveOutcome::SendPing
    }

    /// A frame was received (a pong or any other frame): the connection is alive.
    pub(crate) fn on_activity(&mut self) {
        self.awaiting_pong = false;
        self.missed = 0;
    }

    /// Resets all tracking (new connection).
    pub(crate) fn reset(&mut self) {
        self.awaiting_pong = false;
        self.missed = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reconnect_after_exactly_max_missed_pongs() {
        let mut ka = KeepAlive::new(2);
        // tick 1: nothing outstanding -> ping
        assert_eq!(ka.on_tick(), KeepAliveOutcome::SendPing);
        // tick 2: 1 missed (< 2) -> ping again
        assert_eq!(ka.on_tick(), KeepAliveOutcome::SendPing);
        // tick 3: 2 missed (== max) -> reconnect
        assert_eq!(ka.on_tick(), KeepAliveOutcome::Reconnect);
    }

    #[test]
    fn pong_resets_the_missed_counter() {
        let mut ka = KeepAlive::new(2);
        assert_eq!(ka.on_tick(), KeepAliveOutcome::SendPing);
        assert_eq!(ka.on_tick(), KeepAliveOutcome::SendPing); // 1 missed
        ka.on_activity();
        // counter cleared: the budget starts over
        assert_eq!(ka.on_tick(), KeepAliveOutcome::SendPing);
        assert_eq!(ka.on_tick(), KeepAliveOutcome::SendPing);
        assert_eq!(ka.on_tick(), KeepAliveOutcome::Reconnect);
    }

    #[test]
    fn answered_pings_never_reconnect() {
        let mut ka = KeepAlive::new(1);
        for _ in 0..100 {
            assert_eq!(ka.on_tick(), KeepAliveOutcome::SendPing);
            ka.on_activity();
        }
    }

    #[test]
    fn max_missed_is_at_least_one() {
        let mut ka = KeepAlive::new(0);
        assert_eq!(ka.on_tick(), KeepAliveOutcome::SendPing);
        assert_eq!(ka.on_tick(), KeepAliveOutcome::Reconnect);
    }

    #[test]
    fn reset_clears_pending_state() {
        let mut ka = KeepAlive::new(1);
        assert_eq!(ka.on_tick(), KeepAliveOutcome::SendPing);
        ka.reset();
        assert_eq!(ka.on_tick(), KeepAliveOutcome::SendPing);
    }
}
