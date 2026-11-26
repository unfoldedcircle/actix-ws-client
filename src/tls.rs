//! TLS backend selection and connector construction for `wss://` connections.
//!
//! The backend is selected with cargo features:
//!
//! | Feature                    | Backend                                              |
//! |----------------------------|------------------------------------------------------|
//! | `native-tls` (default)     | The platform's TLS library (OpenSSL on Linux)        |
//! | `rustls-platform-verifier` | rustls, certificates verified by the platform's trust store |
//!
//! If both features are enabled, `native-tls` is used unless a ready-made rustls
//! configuration is supplied with [`TlsConnector::Rustls`]. Without any TLS feature
//! only plain `ws://` connections are possible.

// Without a TLS backend the connector plumbing is compiled but never used.
#![cfg_attr(
    not(any(feature = "native-tls", feature = "rustls-platform-verifier")),
    allow(dead_code)
)]

use crate::Error;
use crate::config::WebSocketConfig;
use std::sync::{Arc, OnceLock};

/// The transport's TLS connector type.
#[cfg(any(feature = "native-tls", feature = "rustls-platform-verifier"))]
pub(crate) use tokio_tungstenite::Connector;

/// Placeholder without a TLS backend: no connector can exist.
#[cfg(not(any(feature = "native-tls", feature = "rustls-platform-verifier")))]
#[derive(Clone)]
pub(crate) enum Connector {}

/// An additional trusted root certificate for `wss://` connections.
///
/// Added to the trust store of the selected TLS backend, on top of the system roots.
/// Use it for a private certificate authority, or for the self-signed certificate of a
/// server (the certificate must still be valid for the server's host name).
#[derive(Clone, PartialEq, Eq)]
pub struct Certificate(CertificateData);

#[derive(Clone, PartialEq, Eq)]
enum CertificateData {
    Der(Vec<u8>),
    Pem(Vec<u8>),
}

impl Certificate {
    /// A DER-encoded X.509 certificate.
    ///
    /// The encoding is validated when the connection is established.
    pub fn from_der(der: impl Into<Vec<u8>>) -> Self {
        Self(CertificateData::Der(der.into()))
    }

    /// PEM-encoded X.509 certificates: one, or several in a row as in a CA bundle
    /// file. Every certificate is trusted.
    ///
    /// The encoding is validated when the connection is established.
    pub fn from_pem(pem: impl Into<Vec<u8>>) -> Self {
        Self(CertificateData::Pem(pem.into()))
    }
}

impl std::fmt::Debug for Certificate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            CertificateData::Der(data) => write!(f, "Certificate(DER, {} bytes)", data.len()),
            CertificateData::Pem(data) => write!(f, "Certificate(PEM, {} bytes)", data.len()),
        }
    }
}

/// A ready-made TLS configuration, see [`crate::WebSocketConfigBuilder::tls_connector`].
///
/// It replaces the connector the client would build itself and therefore cannot be
/// combined with `danger_accept_invalid_certs` or `root_certificates`.
#[cfg(any(feature = "native-tls", feature = "rustls-platform-verifier"))]
#[derive(Clone)]
#[non_exhaustive]
pub enum TlsConnector {
    /// A `native-tls` connector.
    #[cfg(feature = "native-tls")]
    NativeTls(native_tls::TlsConnector),
    /// A rustls client configuration.
    #[cfg(feature = "rustls-platform-verifier")]
    Rustls(std::sync::Arc<rustls::ClientConfig>),
}

#[cfg(any(feature = "native-tls", feature = "rustls-platform-verifier"))]
impl std::fmt::Debug for TlsConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            #[cfg(feature = "native-tls")]
            TlsConnector::NativeTls(_) => f.write_str("TlsConnector::NativeTls"),
            #[cfg(feature = "rustls-platform-verifier")]
            TlsConnector::Rustls(_) => f.write_str("TlsConnector::Rustls"),
        }
    }
}

/// TLS settings of a client, shared by all its connection attempts.
///
/// The connector is built on first use — only when a `wss://` target is actually
/// connected, so plain `ws://` connections never depend on the TLS setup — and then
/// reused for every further attempt.
#[derive(Clone)]
pub(crate) struct TlsContext(Arc<TlsContextInner>);

struct TlsContextInner {
    config: WebSocketConfig,
    connector: OnceLock<Option<Connector>>,
}

impl TlsContext {
    pub(crate) fn new(config: &WebSocketConfig) -> Self {
        Self(Arc::new(TlsContextInner {
            config: config.clone(),
            connector: OnceLock::new(),
        }))
    }

    /// Returns the TLS connector, building it on first use.
    ///
    /// `None` only without a TLS backend. A failed build is not cached.
    pub(crate) fn connector(&self) -> Result<Option<Connector>, Error> {
        if let Some(connector) = self.0.connector.get() {
            return Ok(connector.clone());
        }
        let connector = build_connector(&self.0.config)?;
        Ok(self.0.connector.get_or_init(|| connector).clone())
    }
}

/// Builds the TLS connector for the given configuration.
fn build_connector(config: &WebSocketConfig) -> Result<Option<Connector>, Error> {
    #[cfg(any(feature = "native-tls", feature = "rustls-platform-verifier"))]
    if let Some(connector) = &config.tls_connector {
        return Ok(Some(match connector.clone() {
            #[cfg(feature = "native-tls")]
            TlsConnector::NativeTls(connector) => Connector::NativeTls(connector),
            #[cfg(feature = "rustls-platform-verifier")]
            TlsConnector::Rustls(config) => Connector::Rustls(config),
        }));
    }
    build_default_connector(config)
}

#[cfg(feature = "native-tls")]
fn build_default_connector(config: &WebSocketConfig) -> Result<Option<Connector>, Error> {
    // Built also without any option: the transport would otherwise create its default
    // connector for every connection attempt, which loads the system CA roots again
    // each time.
    let mut builder = native_tls::TlsConnector::builder();
    if config.danger_accept_invalid_certs {
        builder
            .danger_accept_invalid_certs(true)
            .danger_accept_invalid_hostnames(true);
    }
    let invalid = |e| Error::Tls(format!("Invalid root certificate: {e}"));
    for certificate in &config.root_certificates {
        match &certificate.0 {
            CertificateData::Der(der) => {
                builder
                    .add_root_certificate(native_tls::Certificate::from_der(der).map_err(invalid)?);
            }
            // native-tls reads one certificate from a PEM text.
            CertificateData::Pem(pem) => {
                for block in pem_certificates(pem) {
                    builder.add_root_certificate(
                        native_tls::Certificate::from_pem(block).map_err(invalid)?,
                    );
                }
            }
        }
    }
    let connector = builder
        .build()
        .map_err(|e| Error::Tls(format!("Cannot build TLS connector: {e}")))?;
    Ok(Some(Connector::NativeTls(connector)))
}

/// Splits a PEM text into its certificates, each with its `BEGIN` and `END` line.
///
/// A text without a complete certificate is returned as it is, so that parsing it
/// reports the error.
#[cfg(feature = "native-tls")]
fn pem_certificates(pem: &[u8]) -> Vec<&[u8]> {
    const BEGIN: &[u8] = b"-----BEGIN CERTIFICATE-----";
    const END: &[u8] = b"-----END CERTIFICATE-----";
    let find = |from: usize, marker: &[u8]| {
        pem[from..]
            .windows(marker.len())
            .position(|window| window == marker)
            .map(|position| from + position)
    };

    let mut certificates = Vec::new();
    let mut position = 0;
    while let Some(begin) = find(position, BEGIN)
        && let Some(end) = find(begin, END)
    {
        position = end + END.len();
        certificates.push(&pem[begin..position]);
    }
    if certificates.is_empty() {
        certificates.push(pem);
    }
    certificates
}

#[cfg(all(not(feature = "native-tls"), feature = "rustls-platform-verifier"))]
fn build_default_connector(config: &WebSocketConfig) -> Result<Option<Connector>, Error> {
    use rustls::client::danger::ServerCertVerifier;
    use rustls::pki_types::CertificateDer;
    use rustls::pki_types::pem::PemObject;
    let provider = rustls::crypto::CryptoProvider::get_default()
        .cloned()
        .ok_or_else(|| {
            Error::Tls(
                "No rustls CryptoProvider installed: call CryptoProvider::install_default() at startup"
                    .into(),
            )
        })?;

    let verifier: Arc<dyn ServerCertVerifier> = if config.danger_accept_invalid_certs {
        Arc::new(danger::NoCertificateVerification::new(provider.clone()))
    } else {
        let invalid = |e| Error::Tls(format!("Invalid root certificate: {e}"));
        let mut roots = Vec::with_capacity(config.root_certificates.len());
        for certificate in &config.root_certificates {
            match &certificate.0 {
                CertificateData::Der(der) => roots.push(CertificateDer::from(der.clone())),
                CertificateData::Pem(pem) => {
                    let before = roots.len();
                    for certificate in CertificateDer::pem_slice_iter(pem) {
                        roots.push(certificate.map_err(invalid)?);
                    }
                    if roots.len() == before {
                        return Err(Error::Tls(
                            "Invalid root certificate: no certificate in the PEM text".into(),
                        ));
                    }
                }
            }
        }
        Arc::new(
            platform_verifier(roots, provider.clone())
                .map_err(|e| Error::Tls(format!("Cannot build certificate verifier: {e}")))?,
        )
    };

    let tls_config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| Error::Tls(format!("Cannot build TLS configuration: {e}")))?
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_no_client_auth();
    Ok(Some(Connector::Rustls(Arc::new(tls_config))))
}

/// The verifier of the platform's trust store, with additional trusted roots.
#[cfg(all(
    not(feature = "native-tls"),
    feature = "rustls-platform-verifier",
    not(target_os = "android")
))]
fn platform_verifier(
    roots: Vec<rustls::pki_types::CertificateDer<'static>>,
    provider: Arc<rustls::crypto::CryptoProvider>,
) -> Result<rustls_platform_verifier::Verifier, rustls::Error> {
    if roots.is_empty() {
        rustls_platform_verifier::Verifier::new(provider)
    } else {
        rustls_platform_verifier::Verifier::new_with_extra_roots(roots, provider)
    }
}

/// On Android the platform verifier only knows the trust store of the system.
#[cfg(all(
    not(feature = "native-tls"),
    feature = "rustls-platform-verifier",
    target_os = "android"
))]
fn platform_verifier(
    roots: Vec<rustls::pki_types::CertificateDer<'static>>,
    provider: Arc<rustls::crypto::CryptoProvider>,
) -> Result<rustls_platform_verifier::Verifier, rustls::Error> {
    if !roots.is_empty() {
        return Err(rustls::Error::General(
            "root_certificates are not supported on Android: add the certificate to \
             the trust store of the app, or pass a rustls configuration with \
             tls_connector"
                .into(),
        ));
    }
    rustls_platform_verifier::Verifier::new(provider)
}

#[cfg(not(any(feature = "native-tls", feature = "rustls-platform-verifier")))]
fn build_default_connector(_config: &WebSocketConfig) -> Result<Option<Connector>, Error> {
    Ok(None)
}

#[cfg(all(not(feature = "native-tls"), feature = "rustls-platform-verifier"))]
mod danger {
    use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
    use rustls::crypto::CryptoProvider;
    use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
    use rustls::{DigitallySignedStruct, SignatureScheme};
    use std::sync::Arc;

    /// Accepts every server certificate and host name
    /// (`danger_accept_invalid_certs`).
    #[derive(Debug)]
    pub(super) struct NoCertificateVerification(Arc<CryptoProvider>);

    impl NoCertificateVerification {
        pub(super) fn new(provider: Arc<CryptoProvider>) -> Self {
            Self(provider)
        }
    }

    impl ServerCertVerifier for NoCertificateVerification {
        fn verify_server_cert(
            &self,
            _end_entity: &CertificateDer<'_>,
            _intermediates: &[CertificateDer<'_>],
            _server_name: &ServerName<'_>,
            _ocsp_response: &[u8],
            _now: UnixTime,
        ) -> Result<ServerCertVerified, rustls::Error> {
            Ok(ServerCertVerified::assertion())
        }

        fn verify_tls12_signature(
            &self,
            _message: &[u8],
            _cert: &CertificateDer<'_>,
            _dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, rustls::Error> {
            Ok(HandshakeSignatureValid::assertion())
        }

        fn verify_tls13_signature(
            &self,
            _message: &[u8],
            _cert: &CertificateDer<'_>,
            _dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, rustls::Error> {
            Ok(HandshakeSignatureValid::assertion())
        }

        fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
            self.0.signature_verification_algorithms.supported_schemes()
        }
    }
}

#[cfg(all(test, feature = "native-tls"))]
mod tests {
    use super::*;

    const FIRST: &str = "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----";
    const SECOND: &str = "-----BEGIN CERTIFICATE-----\nMIIC\n-----END CERTIFICATE-----";

    fn texts(pem: &str) -> Vec<&str> {
        pem_certificates(pem.as_bytes())
            .into_iter()
            .map(|block| std::str::from_utf8(block).unwrap())
            .collect()
    }

    #[test]
    fn single_certificate_is_one_block() {
        assert_eq!(texts(FIRST), [FIRST]);
        // Text around the certificate is not part of it.
        let commented = format!("# my CA\n{FIRST}\n\n");
        assert_eq!(texts(&commented), [FIRST]);
    }

    #[test]
    fn bundle_is_split_into_its_certificates() {
        let bundle = format!("{FIRST}\n# next\n{SECOND}\n");
        assert_eq!(texts(&bundle), [FIRST, SECOND]);
        // Without a line break between them.
        let joined = format!("{FIRST}{SECOND}");
        assert_eq!(texts(&joined), [FIRST, SECOND]);
    }

    #[test]
    fn text_without_a_complete_certificate_is_kept_for_the_parser() {
        assert_eq!(texts("no certificate"), ["no certificate"]);
        assert_eq!(texts(""), [""]);
        let cut = "-----BEGIN CERTIFICATE-----\nMIIB";
        assert_eq!(texts(cut), [cut]);
    }

    #[test]
    fn default_connector_is_built_once_and_reused() {
        let tls = TlsContext::new(&WebSocketConfig::builder().build().unwrap());
        assert!(tls.connector().unwrap().is_some());
        assert!(
            tls.0.connector.get().is_some(),
            "cached for the next attempt"
        );
        assert!(tls.connector().unwrap().is_some());
    }
}
