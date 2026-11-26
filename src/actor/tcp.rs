//! TCP connection establishment of the production transport.
//!
//! A host name can resolve to several addresses, and some of them may not answer at
//! all — typically an IPv6 address on a network without a working IPv6 route. Trying
//! the addresses one after another would spend the whole connect timeout on the first
//! dead address. Instead, the addresses are raced ("happy eyeballs", RFC 8305): the
//! next address is tried as soon as the previous one failed, or after a short delay
//! while it is still pending. The first established connection wins.
//!
//! The caller limits the total time with the connect timeout.

use futures::StreamExt;
use futures::stream::FuturesUnordered;
use std::io;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::TcpStream;

/// How long a pending connection attempt is given before the next address is tried
/// in parallel (RFC 8305 §5 "Connection Attempt Delay").
const ATTEMPT_DELAY: Duration = Duration::from_millis(250);

/// Resolves `host` and connects to the first of its addresses that answers.
///
/// `host` is a host name or an IP address without brackets. `nodelay` disables
/// Nagle's algorithm on the connection.
pub(crate) async fn connect(host: &str, port: u16, nodelay: bool) -> io::Result<TcpStream> {
    let addrs = tokio::net::lookup_host((host, port)).await?.collect();
    let stream = race(interleave(addrs), ATTEMPT_DELAY, TcpStream::connect).await?;
    stream.set_nodelay(nodelay)?;
    Ok(stream)
}

/// Orders the addresses so that the address families alternate, starting with the
/// family the resolver prefers (RFC 8305 §4). The order within a family is kept.
fn interleave(addrs: Vec<SocketAddr>) -> Vec<SocketAddr> {
    let Some(first) = addrs.first() else {
        return addrs;
    };
    let prefer_v6 = first.is_ipv6();
    let (mut preferred, mut other): (Vec<_>, Vec<_>) =
        addrs.into_iter().partition(|a| a.is_ipv6() == prefer_v6);
    let mut ordered = Vec::with_capacity(preferred.len() + other.len());
    let mut preferred = preferred.drain(..);
    let mut other = other.drain(..);
    loop {
        match (preferred.next(), other.next()) {
            (None, None) => break,
            (a, b) => ordered.extend(a.into_iter().chain(b)),
        }
    }
    ordered
}

/// Connects with `connect` to the first address that answers.
///
/// The addresses are tried in order. The next one is started when the previous one
/// failed, or after `delay` while attempts are still pending. Returns the first
/// established connection, or the last error if no address could be connected.
async fn race<T, F, Fut>(addrs: Vec<SocketAddr>, delay: Duration, connect: F) -> io::Result<T>
where
    F: Fn(SocketAddr) -> Fut,
    Fut: Future<Output = io::Result<T>>,
{
    let mut addrs = addrs.into_iter();
    let mut attempts = FuturesUnordered::new();
    let mut last_error = None;
    loop {
        if attempts.is_empty() {
            match addrs.next() {
                Some(addr) => attempts.push(connect(addr)),
                None => break,
            }
        }
        // Restarts with every attempt that is started.
        let next_attempt = tokio::time::sleep(delay);
        tokio::select! {
            result = attempts.next() => match result {
                Some(Ok(connection)) => return Ok(connection),
                // The next address is started at once: at the top of the loop if
                // this was the only pending attempt, here if others are pending.
                Some(Err(error)) => {
                    last_error = Some(error);
                    if !attempts.is_empty()
                        && let Some(addr) = addrs.next()
                    {
                        attempts.push(connect(addr));
                    }
                }
                None => {}
            },
            () = next_attempt, if addrs.len() > 0 => {
                if let Some(addr) = addrs.next() {
                    attempts.push(connect(addr));
                }
            }
        }
    }
    Err(last_error.unwrap_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            "host name did not resolve to an address",
        )
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use tokio::net::TcpListener;
    use tokio::time::Instant;

    const DELAY: Duration = Duration::from_millis(250);

    fn addr(s: &str) -> SocketAddr {
        s.parse().unwrap()
    }

    /// How a scripted address behaves.
    #[derive(Clone, Copy)]
    enum Peer {
        /// Connects after the given time.
        Up(Duration),
        /// Refuses after the given time.
        Refuses(Duration),
        /// Never answers.
        Dead,
    }

    /// Races the scripted addresses (`10.0.0.<index>`); returns the result and the
    /// addresses that were tried with their start time.
    async fn run(peers: &[Peer]) -> (io::Result<SocketAddr>, Vec<(u8, Duration)>) {
        let start = Instant::now();
        let started = Arc::new(Mutex::new(Vec::new()));
        let addrs = (0..peers.len())
            .map(|i| addr(&format!("10.0.0.{i}:80")))
            .collect();
        let peers = peers.to_vec();
        let log = started.clone();
        let result = race(addrs, DELAY, move |addr: SocketAddr| {
            let SocketAddr::V4(v4) = addr else {
                unreachable!()
            };
            let index = v4.ip().octets()[3];
            log.lock().unwrap().push((index, start.elapsed()));
            let peer = peers[index as usize];
            async move {
                match peer {
                    Peer::Up(after) => {
                        tokio::time::sleep(after).await;
                        Ok(addr)
                    }
                    Peer::Refuses(after) => {
                        tokio::time::sleep(after).await;
                        Err(io::Error::new(
                            io::ErrorKind::ConnectionRefused,
                            format!("refused by {index}"),
                        ))
                    }
                    Peer::Dead => std::future::pending().await,
                }
            }
        })
        .await;
        let started = started.lock().unwrap().clone();
        (result, started)
    }

    const NOW: Duration = Duration::ZERO;

    fn ms(ms: u64) -> Duration {
        Duration::from_millis(ms)
    }

    #[tokio::test(start_paused = true)]
    async fn first_address_that_answers_is_used_alone() {
        let (result, started) = run(&[Peer::Up(ms(100)), Peer::Up(NOW)]).await;

        assert_eq!(result.unwrap(), addr("10.0.0.0:80"));
        assert_eq!(started, vec![(0, NOW)]);
    }

    /// The case this module exists for: the first address never answers.
    #[tokio::test(start_paused = true)]
    async fn dead_address_does_not_block_the_next_one() {
        let start = Instant::now();

        let (result, started) = run(&[Peer::Dead, Peer::Up(ms(20))]).await;

        assert_eq!(result.unwrap(), addr("10.0.0.1:80"));
        assert_eq!(started, vec![(0, NOW), (1, DELAY)]);
        assert_eq!(start.elapsed(), DELAY + ms(20));
    }

    #[tokio::test(start_paused = true)]
    async fn refused_address_is_followed_by_the_next_one_at_once() {
        let (result, started) = run(&[Peer::Refuses(ms(10)), Peer::Up(NOW)]).await;

        assert_eq!(result.unwrap(), addr("10.0.0.1:80"));
        assert_eq!(started, vec![(0, NOW), (1, ms(10))]);
    }

    #[tokio::test(start_paused = true)]
    async fn slow_address_can_still_win() {
        // The first address answers after the second one was started and refused.
        let (result, started) = run(&[Peer::Up(ms(400)), Peer::Refuses(ms(10)), Peer::Dead]).await;

        assert_eq!(result.unwrap(), addr("10.0.0.0:80"));
        // The third address follows the refusal of the second one at once.
        assert_eq!(started, vec![(0, NOW), (1, DELAY), (2, DELAY + ms(10))]);
    }

    #[tokio::test(start_paused = true)]
    async fn dead_addresses_are_tried_with_the_delay_between_them() {
        let (result, started) = run(&[Peer::Dead, Peer::Dead, Peer::Up(NOW)]).await;

        assert_eq!(result.unwrap(), addr("10.0.0.2:80"));
        assert_eq!(started, vec![(0, NOW), (1, DELAY), (2, DELAY * 2)]);
    }

    #[tokio::test(start_paused = true)]
    async fn last_error_is_reported_if_no_address_answers() {
        let (result, started) = run(&[Peer::Refuses(NOW), Peer::Refuses(ms(10))]).await;

        let error = result.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::ConnectionRefused);
        assert_eq!(error.to_string(), "refused by 1");
        assert_eq!(started.len(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn no_address_is_an_error() {
        let (result, started) = run(&[]).await;

        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::NotFound);
        assert!(started.is_empty());
    }

    #[test]
    fn address_families_alternate_starting_with_the_preferred_one() {
        let ordered = interleave(vec![
            addr("[2001:db8::1]:80"),
            addr("[2001:db8::2]:80"),
            addr("192.0.2.1:80"),
            addr("192.0.2.2:80"),
            addr("192.0.2.3:80"),
        ]);
        assert_eq!(
            ordered,
            vec![
                addr("[2001:db8::1]:80"),
                addr("192.0.2.1:80"),
                addr("[2001:db8::2]:80"),
                addr("192.0.2.2:80"),
                addr("192.0.2.3:80"),
            ]
        );

        let v4_first = interleave(vec![addr("192.0.2.1:80"), addr("[2001:db8::1]:80")]);
        assert_eq!(v4_first[0], addr("192.0.2.1:80"));
        assert!(interleave(Vec::new()).is_empty());
    }

    #[tokio::test]
    async fn connects_and_applies_the_nodelay_setting() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        let stream = connect("127.0.0.1", port, true).await.unwrap();
        assert!(stream.nodelay().unwrap());
        let stream = connect("127.0.0.1", port, false).await.unwrap();
        assert!(!stream.nodelay().unwrap());
    }

    #[tokio::test]
    async fn connect_error_keeps_its_kind() {
        // Bind and drop: nothing listens on the port anymore.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        let error = connect("127.0.0.1", port, true).await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::ConnectionRefused);
    }
}
