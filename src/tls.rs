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

use crate::Error;
use crate::config::WebSocketConfig;

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

    /// A PEM-encoded X.509 certificate.
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

/// Builds the TLS connector for the given configuration.
///
/// Returns `None` if the transport's default applies: with `native-tls`, a connector
/// with system CA roots and full verification.
pub(crate) fn build_connector(config: &WebSocketConfig) -> Result<Option<Connector>, Error> {
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
    if !config.danger_accept_invalid_certs && config.root_certificates.is_empty() {
        return Ok(None);
    }
    let mut builder = native_tls::TlsConnector::builder();
    if config.danger_accept_invalid_certs {
        builder
            .danger_accept_invalid_certs(true)
            .danger_accept_invalid_hostnames(true);
    }
    for certificate in &config.root_certificates {
        let certificate = match &certificate.0 {
            CertificateData::Der(der) => native_tls::Certificate::from_der(der),
            CertificateData::Pem(pem) => native_tls::Certificate::from_pem(pem),
        }
        .map_err(|e| Error::Tls(format!("Invalid root certificate: {e}")))?;
        builder.add_root_certificate(certificate);
    }
    let connector = builder
        .build()
        .map_err(|e| Error::Tls(format!("Cannot build TLS connector: {e}")))?;
    Ok(Some(Connector::NativeTls(connector)))
}

#[cfg(all(not(feature = "native-tls"), feature = "rustls-platform-verifier"))]
fn build_default_connector(config: &WebSocketConfig) -> Result<Option<Connector>, Error> {
    use rustls::client::danger::ServerCertVerifier;
    use rustls::pki_types::CertificateDer;
    use rustls::pki_types::pem::PemObject;
    use std::sync::Arc;

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
        let mut roots = Vec::with_capacity(config.root_certificates.len());
        for certificate in &config.root_certificates {
            roots.push(match &certificate.0 {
                CertificateData::Der(der) => CertificateDer::from(der.clone()),
                CertificateData::Pem(pem) => CertificateDer::from_pem_slice(pem)
                    .map_err(|e| Error::Tls(format!("Invalid root certificate: {e}")))?,
            });
        }
        Arc::new(
            rustls_platform_verifier::Verifier::new_with_extra_roots(roots, provider.clone())
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
