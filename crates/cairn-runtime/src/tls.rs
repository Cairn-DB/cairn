//! TLS for the TCP transport (ADR 0018): rustls with the ring backend.
//!
//! Every node holds a certificate for the DNS name `node-<id>.cairn`, signed by the cluster's
//! CA. A node connecting to a peer checks that the peer's certificate is valid for the peer's
//! name; a node accepting a connection that claims to come from node `N` checks the client
//! certificate against `node-N.cairn`. Clients verify the node they connect to the same way and
//! may present a certificate of their own (required unless the node allows anonymous clients).
//!
//! A connection is used by two threads at once (one reads, one writes; replies to clients are
//! written from helper threads), so a [`Channel`] splits into a [`ChannelReader`] and a
//! [`ChannelWriter`]. They share the TLS state under a lock that is never held across a
//! blocking socket read. Encrypted output is written under a second lock, taken before the
//! first is released, so records reach the socket in the order they were sealed.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use cairn_core::error::IoErrorKind;
use cairn_core::{Error, NodeId, Result};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{ClientConfig, ClientConnection, Connection, RootCertStore, ServerConfig};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// DNS name a node's certificate must be valid for.
pub fn node_name(id: NodeId) -> String {
    format!("node-{}.cairn", id.get())
}

fn tls_err(e: impl std::fmt::Display) -> Error {
    Error::io(IoErrorKind::Other, format!("tls: {e}"))
}

fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

fn load_roots(ca: &Path) -> Result<RootCertStore> {
    let mut roots = RootCertStore::empty();
    for c in CertificateDer::pem_file_iter(ca).map_err(tls_err)? {
        roots.add(c.map_err(tls_err)?).map_err(tls_err)?;
    }
    if roots.is_empty() {
        return Err(tls_err(format!("no CA certificate in {}", ca.display())));
    }
    Ok(roots)
}

fn load_identity(
    cert: &Path,
    key: &Path,
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
    let chain: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(cert)
        .map_err(tls_err)?
        .collect::<std::result::Result<_, _>>()
        .map_err(tls_err)?;
    if chain.is_empty() {
        return Err(tls_err(format!("no certificate in {}", cert.display())));
    }
    let key = PrivateKeyDer::from_pem_file(key).map_err(tls_err)?;
    Ok((chain, key))
}

/// Server TLS for the HTTP API (ADR 0030): the certificate chain and key in PEM files, no
/// client certificate (clients authenticate with API keys), HTTP/1.1 announced over ALPN.
pub fn https_server_config(cert: &Path, key: &Path) -> Result<Arc<ServerConfig>> {
    let (chain, key) = load_identity(cert, key)?;
    let mut config = ServerConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .map_err(tls_err)?
        .with_no_client_auth()
        .with_single_cert(chain, key)
        .map_err(tls_err)?;
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(Arc::new(config))
}

/// TLS settings of a client (a node dialing peers, or an application).
#[derive(Clone, Debug)]
pub struct ClientTls {
    config: Arc<ClientConfig>,
}

impl ClientTls {
    /// Trusts the CA in `ca`; presents the certificate in `cert`/`key` if given.
    pub fn from_pem_files(ca: &Path, identity: Option<(&Path, &Path)>) -> Result<Self> {
        let roots = load_roots(ca)?;
        let builder = ClientConfig::builder_with_provider(provider())
            .with_safe_default_protocol_versions()
            .map_err(tls_err)?
            .with_root_certificates(roots);
        let config = match identity {
            Some((cert, key)) => {
                let (chain, key) = load_identity(cert, key)?;
                builder.with_client_auth_cert(chain, key).map_err(tls_err)?
            }
            None => builder.with_no_client_auth(),
        };
        Ok(ClientTls {
            config: Arc::new(config),
        })
    }

    /// From DER material (tests, embedded use).
    pub fn from_der(
        ca: CertificateDer<'static>,
        identity: Option<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)>,
    ) -> Result<Self> {
        let mut roots = RootCertStore::empty();
        roots.add(ca).map_err(tls_err)?;
        let builder = ClientConfig::builder_with_provider(provider())
            .with_safe_default_protocol_versions()
            .map_err(tls_err)?
            .with_root_certificates(roots);
        let config = match identity {
            Some((chain, key)) => builder.with_client_auth_cert(chain, key).map_err(tls_err)?,
            None => builder.with_no_client_auth(),
        };
        Ok(ClientTls {
            config: Arc::new(config),
        })
    }

    /// Handshakes as a client with `node` over `stream`.
    pub fn connect(&self, stream: TcpStream, node: NodeId) -> Result<Channel> {
        let name = ServerName::try_from(node_name(node)).map_err(tls_err)?;
        let conn = ClientConnection::new(self.config.clone(), name).map_err(tls_err)?;
        Channel::handshake(stream, Connection::Client(conn))
    }
}

/// TLS settings of a node: its server side (accepting peers and clients) and its client side
/// (dialing peers, forwarding requests).
#[derive(Clone, Debug)]
pub struct NodeTls {
    server: Arc<ServerConfig>,
    /// Client side, presenting the node's certificate.
    pub client: ClientTls,
}

impl NodeTls {
    /// Loads the cluster CA and this node's certificate and key. With `anonymous_clients`,
    /// application clients may connect without a certificate (peers always need one).
    pub fn from_pem_files(
        ca: &Path,
        cert: &Path,
        key: &Path,
        anonymous_clients: bool,
    ) -> Result<Self> {
        let roots = load_roots(ca)?;
        let (chain, key_der) = load_identity(cert, key)?;
        Self::build(roots, chain, key_der, anonymous_clients)
    }

    /// From DER material (tests, embedded use).
    pub fn from_der(
        ca: CertificateDer<'static>,
        chain: Vec<CertificateDer<'static>>,
        key: PrivateKeyDer<'static>,
        anonymous_clients: bool,
    ) -> Result<Self> {
        let mut roots = RootCertStore::empty();
        roots.add(ca).map_err(tls_err)?;
        Self::build(roots, chain, key, anonymous_clients)
    }

    fn build(
        roots: RootCertStore,
        chain: Vec<CertificateDer<'static>>,
        key: PrivateKeyDer<'static>,
        anonymous_clients: bool,
    ) -> Result<Self> {
        let roots = Arc::new(roots);
        let verifier =
            rustls::server::WebPkiClientVerifier::builder_with_provider(roots.clone(), provider());
        let verifier = if anonymous_clients {
            verifier.allow_unauthenticated()
        } else {
            verifier
        }
        .build()
        .map_err(tls_err)?;
        let server = ServerConfig::builder_with_provider(provider())
            .with_safe_default_protocol_versions()
            .map_err(tls_err)?
            .with_client_cert_verifier(verifier)
            .with_single_cert(chain.clone(), key.clone_key())
            .map_err(tls_err)?;
        let client = ClientConfig::builder_with_provider(provider())
            .with_safe_default_protocol_versions()
            .map_err(tls_err)?
            .with_root_certificates((*roots).clone())
            .with_client_auth_cert(chain, key)
            .map_err(tls_err)?;
        Ok(NodeTls {
            server: Arc::new(server),
            client: ClientTls {
                config: Arc::new(client),
            },
        })
    }

    /// Handshakes as a server over an accepted `stream`.
    pub fn accept(&self, stream: TcpStream) -> Result<Channel> {
        let conn = rustls::ServerConnection::new(self.server.clone()).map_err(tls_err)?;
        Channel::handshake(stream, Connection::Server(conn))
    }
}

struct Shared {
    tls: Mutex<Connection>,
    /// Serializes socket writes of sealed records (taken while `tls` is held).
    out: Mutex<TcpStream>,
}

/// An established connection, plaintext or TLS. Split it with [`Channel::split`].
pub struct Channel {
    sock: TcpStream,
    tls: Option<Arc<Shared>>,
    peer_certs: Vec<CertificateDer<'static>>,
}

impl Channel {
    /// A plaintext channel.
    pub fn plain(sock: TcpStream) -> Self {
        Channel {
            sock,
            tls: None,
            peer_certs: Vec::new(),
        }
    }

    fn handshake(mut sock: TcpStream, mut conn: Connection) -> Result<Self> {
        let io = |e: std::io::Error| Error::io(IoErrorKind::Other, format!("tls handshake: {e}"));
        // A peer that never completes the handshake must not hold a thread forever.
        sock.set_read_timeout(Some(Duration::from_secs(10)))
            .map_err(io)?;
        while conn.is_handshaking() {
            conn.complete_io(&mut sock).map_err(io)?;
        }
        while conn.wants_write() {
            conn.write_tls(&mut sock).map_err(io)?;
        }
        sock.set_read_timeout(None).map_err(io)?;
        let peer_certs = conn
            .peer_certificates()
            .map(|c| c.iter().map(|c| c.clone().into_owned()).collect())
            .unwrap_or_default();
        let out = sock.try_clone().map_err(io)?;
        Ok(Channel {
            sock,
            tls: Some(Arc::new(Shared {
                tls: Mutex::new(conn),
                out: Mutex::new(out),
            })),
            peer_certs,
        })
    }

    /// Whether the channel is encrypted.
    pub fn is_tls(&self) -> bool {
        self.tls.is_some()
    }

    /// Splits into a reader and a writer usable from different threads.
    pub fn split(self) -> Result<(ChannelReader, ChannelWriter)> {
        let w = self
            .sock
            .try_clone()
            .map_err(|e| Error::io(IoErrorKind::Other, e))?;
        Ok((
            ChannelReader {
                sock: self.sock,
                tls: self.tls.clone(),
                peer_certs: self.peer_certs,
            },
            ChannelWriter {
                sock: w,
                tls: self.tls,
            },
        ))
    }
}

/// Read half of a [`Channel`].
pub struct ChannelReader {
    sock: TcpStream,
    tls: Option<Arc<Shared>>,
    peer_certs: Vec<CertificateDer<'static>>,
}

impl ChannelReader {
    /// Whether the peer presented a certificate valid for node `id` (its chain was verified
    /// against the cluster CA during the handshake).
    pub fn peer_is_node(&self, id: NodeId) -> bool {
        let Some(leaf) = self.peer_certs.first() else {
            return false;
        };
        let Ok(name) = ServerName::try_from(node_name(id)) else {
            return false;
        };
        webpki::EndEntityCert::try_from(leaf)
            .is_ok_and(|c| c.verify_is_valid_for_subject_name(&name).is_ok())
    }
}

impl Read for ChannelReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let Some(shared) = &self.tls else {
            return self.sock.read(buf);
        };
        loop {
            {
                let mut conn = shared.tls.lock().expect("tls state");
                match conn.reader().read(buf) {
                    Ok(n) => return Ok(n),
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                    Err(e) => return Err(e),
                }
            }
            // Blocking read without the lock: the writer keeps sealing and sending meanwhile.
            let mut raw = [0u8; 16 * 1024];
            let n = self.sock.read(&mut raw)?;
            if n == 0 {
                return Ok(0);
            }
            let mut conn = shared.tls.lock().expect("tls state");
            let mut slice = &raw[..n];
            while !slice.is_empty() {
                conn.read_tls(&mut slice)?;
                conn.process_new_packets()
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            }
            if conn.wants_write() {
                // Protocol messages (alerts, key updates): sealed now, sent in order.
                let mut out = shared.out.lock().expect("tls out");
                while conn.wants_write() {
                    conn.write_tls(&mut *out)?;
                }
            }
        }
    }
}

/// Write half of a [`Channel`].
pub struct ChannelWriter {
    sock: TcpStream,
    tls: Option<Arc<Shared>>,
}

impl Write for ChannelWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let Some(shared) = &self.tls else {
            return self.sock.write(buf);
        };
        let mut conn = shared.tls.lock().expect("tls state");
        let n = conn.writer().write(buf)?;
        let mut sealed = Vec::new();
        while conn.wants_write() {
            conn.write_tls(&mut sealed)?;
        }
        // Take the output lock before releasing the state lock: records go out in seal order,
        // and the reader can process input while this thread waits on a full socket.
        let mut out = shared.out.lock().expect("tls out");
        drop(conn);
        out.write_all(&sealed)?;
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match &self.tls {
            None => self.sock.flush(),
            Some(shared) => shared.out.lock().expect("tls out").flush(),
        }
    }
}
