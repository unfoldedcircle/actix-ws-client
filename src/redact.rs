//! What the crate writes to the log and into error texts, without secrets.
//!
//! - Message contents are only logged by the message tracing, where the handler can
//!   redact them. Everything else logs their size.
//! - A URL can carry credentials as user info or in its query. Only its target is
//!   logged.

use crate::core::effect::{Callback, Effect};
use crate::handler::HandlerAction;
use std::fmt;

/// A URL without the parts that can carry credentials: user info, query and
/// fragment are replaced by a marker.
///
/// Works on the text, so that also an invalid URL or a relative reference can be
/// reported.
pub(crate) fn url(url: &str) -> String {
    let end = url.find(['?', '#']).unwrap_or(url.len());
    let (target, cut) = url.split_at(end);
    let mut redacted = String::with_capacity(url.len());

    // User info: `scheme://user:password@host/path` or `//user@host`.
    let user_info = target.find("//").and_then(|slashes| {
        let authority = slashes + 2;
        let authority_end = target[authority..]
            .find('/')
            .map_or(target.len(), |end| authority + end);
        target[authority..authority_end]
            .rfind('@')
            .map(|at| (authority, authority + at))
    });
    match user_info {
        Some((start, at)) => {
            redacted.push_str(&target[..start]);
            redacted.push_str("***");
            redacted.push_str(&target[at..]);
        }
        None => redacted.push_str(target),
    }
    if let Some(separator) = cut.chars().next() {
        redacted.push(separator);
        redacted.push_str("***");
    }
    redacted
}

/// An effect of the core for the log: message contents are replaced by their size.
pub(crate) struct EffectLog<'a>(pub &'a Effect);

impl fmt::Display for EffectLog<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            Effect::Emit(Callback::OnText(text)) => {
                write!(f, "Emit(OnText({} bytes))", text.len())
            }
            Effect::Emit(Callback::OnBinary(data)) => {
                write!(f, "Emit(OnBinary({} bytes))", data.len())
            }
            effect => write!(f, "{effect:?}"),
        }
    }
}

/// A handler action for the log: message contents are replaced by their size.
pub(crate) struct ActionLog<'a>(pub &'a HandlerAction);

impl fmt::Display for ActionLog<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            HandlerAction::SendText(text) => write!(f, "SendText({} bytes)", text.len()),
            HandlerAction::SendBinary(data) => write!(f, "SendBinary({} bytes)", data.len()),
            action => write!(f, "{action:?}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn url_without_secrets_is_unchanged() {
        for plain in [
            "ws://example.com",
            "wss://example.com:8443/socket/v1",
            "ws://[2001:db8::1]:8080/x",
            "/relative/path",
            "not a url",
            "",
        ] {
            assert_eq!(url(plain), plain);
        }
    }

    #[test]
    fn query_and_fragment_are_removed() {
        assert_eq!(
            url("wss://example.com/socket?token=secret&x=1"),
            "wss://example.com/socket?***"
        );
        assert_eq!(
            url("wss://example.com/socket#secret"),
            "wss://example.com/socket#***"
        );
        assert_eq!(url("/path?token=secret#frag"), "/path?***");
        assert_eq!(url("?token=secret"), "?***");
    }

    #[test]
    fn user_info_is_removed() {
        assert_eq!(
            url("wss://user:secret@example.com/socket"),
            "wss://***@example.com/socket"
        );
        assert_eq!(url("ws://token@example.com"), "ws://***@example.com");
        assert_eq!(url("//user:secret@example.com/x"), "//***@example.com/x");
        // A password may contain an `@`.
        assert_eq!(
            url("ws://user:p@ss@example.com/x"),
            "ws://***@example.com/x"
        );
        // An `@` in the path is not user info.
        assert_eq!(
            url("ws://example.com/users/@me"),
            "ws://example.com/users/@me"
        );
    }

    #[test]
    fn everything_at_once() {
        assert_eq!(
            url("wss://user:secret@example.com:8443/socket?token=secret#secret"),
            "wss://***@example.com:8443/socket?***"
        );
    }

    #[test]
    fn effects_are_logged_without_message_contents() {
        let text = Effect::Emit(Callback::OnText("secret".into()));
        assert_eq!(EffectLog(&text).to_string(), "Emit(OnText(6 bytes))");
        let binary = Effect::Emit(Callback::OnBinary(Bytes::from_static(b"\x01\x02")));
        assert_eq!(EffectLog(&binary).to_string(), "Emit(OnBinary(2 bytes))");
        // Everything else is logged as it is.
        let other = Effect::StartConnectionTask { generation: 7 };
        assert_eq!(EffectLog(&other).to_string(), format!("{other:?}"));
    }

    #[test]
    fn actions_are_logged_without_message_contents() {
        let text = HandlerAction::SendText("secret".into());
        assert_eq!(ActionLog(&text).to_string(), "SendText(6 bytes)");
        let binary = HandlerAction::SendBinary(Bytes::from_static(b"\x01\x02"));
        assert_eq!(ActionLog(&binary).to_string(), "SendBinary(2 bytes)");
        assert_eq!(
            ActionLog(&HandlerAction::Reconnect).to_string(),
            "Reconnect"
        );
    }
}
