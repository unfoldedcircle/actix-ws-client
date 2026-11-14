//! End-to-end TLS tests against a local `wss://` echo server.
//!
//! The server presents a certificate issued by a throw-away certificate authority,
//! which no trust store knows. The tests run against whichever TLS backend the crate
//! is built with (`native-tls` or `rustls-platform-verifier`).
#![cfg(any(feature = "native-tls", feature = "rustls-platform-verifier"))]

use actix::prelude::*;
use actix_ws_client::{
    Certificate, Connect, Error, GenericWebSocketClient, HandlerAction, SendText, TlsConnector,
    WebSocketConfig, WebSocketConfigBuilder, WebSocketMessageHandler,
};
use futures::{SinkExt, StreamExt};
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose, date_time_ymd,
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tokio_tungstenite::tungstenite::protocol::Message;

#[derive(Clone, Default)]
struct RecordingHandler {
    texts: Arc<Mutex<Vec<String>>>,
    errors: Arc<Mutex<Vec<Error>>>,
}

impl WebSocketMessageHandler for RecordingHandler {
    fn on_text(&mut self, text: String) -> Vec<HandlerAction> {
        self.texts.lock().unwrap().push(text);
        Vec::new()
    }
    fn on_error(&mut self, error: &Error) {
        self.errors.lock().unwrap().push(error.clone());
    }
}

/// A `wss://` echo server and the certificate authority that issued its certificate.
struct TlsServer {
    url: String,
    ca_der: Vec<u8>,
    ca_pem: String,
}

/// Validity of the test certificates: the current and the next year. Platform
/// verifiers reject server certificates with a long validity period.
fn validity() -> (i32, i32) {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let year = 1970 + (secs / 31_556_952) as i32;
    (year, year + 1)
}

async fn spawn_tls_echo_server() -> TlsServer {
    let (from_year, to_year) = validity();

    let ca_key = KeyPair::generate().unwrap();
    let mut ca_params = CertificateParams::new(Vec::new()).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    ca_params.not_before = date_time_ymd(from_year, 1, 1);
    ca_params.not_after = date_time_ymd(to_year, 12, 31);
    ca_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "actix-ws-client test CA");
    let ca = ca_params.self_signed(&ca_key).unwrap();
    let issuer = Issuer::from_params(&ca_params, &ca_key);

    let server_key = KeyPair::generate().unwrap();
    let mut server_params = CertificateParams::new(vec!["localhost".to_string()]).unwrap();
    server_params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    server_params.not_before = date_time_ymd(from_year, 1, 1);
    server_params.not_after = date_time_ymd(to_year, 12, 31);
    let server_cert = server_params.signed_by(&server_key, &issuer).unwrap();

    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let server_config = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            vec![CertificateDer::from(server_cert.der().to_vec())],
            PrivateKeyDer::try_from(server_key.serialize_der()).unwrap(),
        )
        .unwrap();
    let acceptor = TlsAcceptor::from(Arc::new(server_config));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let acceptor = acceptor.clone();
            tokio::spawn(async move {
                let Ok(stream) = acceptor.accept(stream).await else {
                    return; // handshake rejected by the client
                };
                let Ok(ws) = tokio_tungstenite::accept_async(stream).await else {
                    return;
                };
                let (mut sink, mut source) = ws.split();
                while let Some(Ok(msg)) = source.next().await {
                    match msg {
                        Message::Text(_) => {
                            if sink.send(msg).await.is_err() {
                                break;
                            }
                        }
                        Message::Close(_) => {
                            let _ = sink.send(Message::Close(None)).await;
                            break;
                        }
                        _ => {}
                    }
                }
            });
        }
    });

    TlsServer {
        url: format!("wss://localhost:{port}"),
        ca_der: ca.der().to_vec(),
        ca_pem: ca.pem(),
    }
}

/// rustls needs a process-wide crypto provider; a no-op for native-tls.
fn install_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

/// No reconnection: a failed connect is final, so the test sees its error.
fn config() -> WebSocketConfigBuilder {
    WebSocketConfig::builder().max_reconnect_attempts(0u32)
}

fn start_client(
    url: &str,
    config: WebSocketConfig,
    handler: RecordingHandler,
) -> Addr<GenericWebSocketClient<RecordingHandler>> {
    install_crypto_provider();
    GenericWebSocketClient::new("tls", url, config, handler)
        .unwrap()
        .start()
}

async fn assert_echo(
    client: &Addr<GenericWebSocketClient<RecordingHandler>>,
    handler: &RecordingHandler,
) {
    client.send(Connect).await.unwrap().unwrap();
    client.send(SendText::new("secure")).await.unwrap().unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while !handler.texts.lock().unwrap().iter().any(|t| t == "secure") {
        assert!(Instant::now() < deadline, "timed out waiting for the echo");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[actix_rt::test]
async fn untrusted_certificate_is_rejected_with_a_tls_error() {
    let server = spawn_tls_echo_server().await;
    let handler = RecordingHandler::default();
    let client = start_client(&server.url, config().build().unwrap(), handler.clone());

    let result = client.send(Connect).await.unwrap();

    assert!(matches!(result, Err(Error::Tls(_))), "got: {result:?}");
    assert!(matches!(
        handler.errors.lock().unwrap().as_slice(),
        [Error::Tls(_)]
    ));
}

#[actix_rt::test]
async fn danger_accept_invalid_certs_connects_to_an_untrusted_server() {
    let server = spawn_tls_echo_server().await;
    let handler = RecordingHandler::default();
    let config = config().danger_accept_invalid_certs(true).build().unwrap();
    let client = start_client(&server.url, config, handler.clone());

    assert_echo(&client, &handler).await;
}

/// Certificate *and* host name verification are disabled: the certificate is issued
/// for `localhost`, not for the IP address.
#[actix_rt::test]
async fn danger_accept_invalid_certs_ignores_the_host_name() {
    let server = spawn_tls_echo_server().await;
    let url = server.url.replace("localhost", "127.0.0.1");
    let handler = RecordingHandler::default();
    let config = config().danger_accept_invalid_certs(true).build().unwrap();
    let client = start_client(&url, config, handler.clone());

    assert_echo(&client, &handler).await;
}

#[actix_rt::test]
async fn der_root_certificate_is_trusted() {
    let server = spawn_tls_echo_server().await;
    let handler = RecordingHandler::default();
    let config = config()
        .add_root_certificate(Certificate::from_der(server.ca_der.clone()))
        .build()
        .unwrap();
    let client = start_client(&server.url, config, handler.clone());

    assert_echo(&client, &handler).await;
}

#[actix_rt::test]
async fn pem_root_certificate_is_trusted() {
    let server = spawn_tls_echo_server().await;
    let handler = RecordingHandler::default();
    let config = config()
        .root_certificates(vec![Certificate::from_pem(server.ca_pem.clone())])
        .build()
        .unwrap();
    let client = start_client(&server.url, config, handler.clone());

    assert_echo(&client, &handler).await;
}

/// A trusted root does not disable host name verification.
#[actix_rt::test]
async fn root_certificate_does_not_disable_host_name_verification() {
    let server = spawn_tls_echo_server().await;
    let url = server.url.replace("localhost", "127.0.0.1");
    let handler = RecordingHandler::default();
    let config = config()
        .add_root_certificate(Certificate::from_der(server.ca_der.clone()))
        .build()
        .unwrap();
    let client = start_client(&url, config, handler.clone());

    let result = client.send(Connect).await.unwrap();

    assert!(matches!(result, Err(Error::Tls(_))), "got: {result:?}");
}

#[actix_rt::test]
async fn invalid_root_certificate_fails_the_connect_with_a_tls_error() {
    let server = spawn_tls_echo_server().await;
    let handler = RecordingHandler::default();
    let config = config()
        .add_root_certificate(Certificate::from_pem("not a certificate"))
        .build()
        .unwrap();
    let client = start_client(&server.url, config, handler.clone());

    let result = client.send(Connect).await.unwrap();

    assert!(matches!(result, Err(Error::Tls(_))), "got: {result:?}");
}

/// A ready-made rustls configuration is used as is, also when native-tls is compiled in.
#[cfg(feature = "rustls-platform-verifier")]
#[actix_rt::test]
async fn ready_made_rustls_connector_is_used() {
    let server = spawn_tls_echo_server().await;
    let mut roots = rustls::RootCertStore::empty();
    roots
        .add(CertificateDer::from(server.ca_der.clone()))
        .unwrap();
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let tls_config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let handler = RecordingHandler::default();
    let config = config()
        .tls_connector(TlsConnector::Rustls(Arc::new(tls_config)))
        .build()
        .unwrap();
    let client = start_client(&server.url, config, handler.clone());

    assert_echo(&client, &handler).await;
}

#[cfg(feature = "native-tls")]
#[actix_rt::test]
async fn ready_made_native_tls_connector_is_used() {
    let server = spawn_tls_echo_server().await;
    let connector = native_tls::TlsConnector::builder()
        .add_root_certificate(native_tls::Certificate::from_der(&server.ca_der).unwrap())
        .build()
        .unwrap();
    let handler = RecordingHandler::default();
    let config = config()
        .tls_connector(TlsConnector::NativeTls(connector))
        .build()
        .unwrap();
    let client = start_client(&server.url, config, handler.clone());

    assert_echo(&client, &handler).await;
}

#[test]
fn tls_connector_cannot_be_combined_with_other_tls_options() {
    #[cfg(feature = "native-tls")]
    let connector = TlsConnector::NativeTls(native_tls::TlsConnector::new().unwrap());
    #[cfg(not(feature = "native-tls"))]
    let connector = {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        TlsConnector::Rustls(Arc::new(
            rustls::ClientConfig::builder_with_provider(provider)
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_root_certificates(rustls::RootCertStore::empty())
                .with_no_client_auth(),
        ))
    };

    let result = WebSocketConfig::builder()
        .tls_connector(connector)
        .danger_accept_invalid_certs(true)
        .build();

    assert!(result.is_err());
}
