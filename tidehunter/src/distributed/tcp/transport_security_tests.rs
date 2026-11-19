//! The explicit trusted-network TCP mode: opt-in only, recorded, no TLS, and
//! never a downgrade of, or a silent fallback from, mutual-TLS TCP.
use super::tests::{certificates, config, metrics};
use super::*;
use crate::key_shape::KeyType;
use serde_json::json;

fn shape() -> KeyShape {
    KeyShape::new_single(4, 1, KeyType::uniform(1))
}

fn trusted_files() -> TlsFiles {
    TlsFiles {
        transport: Transport::TcpTrusted,
        ca: PathBuf::new(),
        cert: PathBuf::new(),
        key: PathBuf::new(),
        server_name: String::new(),
    }
}

#[test]
fn trusted_tcp_is_explicit_recorded_and_never_a_default() {
    // A configuration that names no transport is the mutual-TLS one.
    let omitted: TlsFiles = serde_json::from_value(json!({})).unwrap();
    assert_eq!(omitted.transport, Transport::Tcp);
    assert_eq!(Transport::default(), Transport::Tcp);
    assert_eq!(Transport::default().security_label(), "mutual_tls");

    // Only the explicit selector yields the trusted mode, and its serialized
    // form (what receipts record) names it.
    let selected: Transport = serde_json::from_value(json!({"kind": "tcp-trusted"})).unwrap();
    assert_eq!(selected, Transport::TcpTrusted);
    assert_eq!(
        serde_json::to_value(&selected).unwrap(),
        json!({"kind": "tcp-trusted"})
    );
    assert_eq!(selected.label(), "tcp-trusted");
    assert!(selected.is_native() && !selected.is_fabric());
    assert_eq!(selected.security_label(), "trusted_network_no_tls");

    // Like-for-like partners carry the same security class.
    assert_eq!(
        Transport::ofi(FabricBackend::Verbs, FabricSecurity::MutualTls).security_label(),
        Transport::Tcp.security_label()
    );
    assert_eq!(
        Transport::ofi(FabricBackend::Verbs, FabricSecurity::Trusted).security_label(),
        Transport::TcpTrusted.security_label()
    );
    assert!(!Transport::Tcp.same_protocol(&Transport::TcpTrusted));
}

#[test]
fn trusted_tcp_requires_empty_credentials_and_mtls_never_degrades() {
    let server = trusted_files().server_config().unwrap();
    assert!(server.config.is_none() && server.verifier.is_none());
    let client = trusted_files().client_config_with_identity().unwrap();
    assert!(client.config.is_none());

    // Credentials next to a trusted transport are a configuration error.
    let mut with_ca = trusted_files();
    with_ca.ca = PathBuf::from("ca.pem");
    assert!(with_ca.server_config().is_err());
    assert!(with_ca.client_config_with_identity().is_err());

    // Standard TCP without credentials fails; it does not become plaintext.
    let mut tcp = trusted_files();
    tcp.transport = Transport::Tcp;
    assert!(tcp.server_config().is_err());
    assert!(tcp.client_config_with_identity().is_err());
}

struct Server {
    config: NodeConfig,
    endpoint: String,
    shutdown: Arc<AtomicBool>,
    worker: Option<thread::JoinHandle<()>>,
    _temp: tempdir::TempDir,
}

impl Server {
    fn start(tls: TlsFiles, temp: tempdir::TempDir) -> Self {
        let config = config(&temp.path().join("node"));
        let node = StorageNode::open(config.clone(), metrics()).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        let shutdown = Arc::new(AtomicBool::new(false));
        let (server, stop) = (node, shutdown.clone());
        let limits = TcpLimits {
            io_timeout: Duration::from_secs(3),
            max_frame_bytes: 2 * 1024 * 1024,
            ..TcpLimits::default()
        };
        let worker = thread::spawn(move || server.serve(listener, tls, limits, stop).unwrap());
        Self {
            config,
            endpoint,
            shutdown,
            worker: Some(worker),
            _temp: temp,
        }
    }
    fn connect(&self, tls: &TlsFiles) -> Result<Client, TcpError> {
        Client::connect(
            &self.endpoint,
            tls,
            Duration::from_secs(2),
            self.config.partition.clone(),
            &shape(),
        )
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            worker.join().unwrap();
        }
    }
}

#[test]
fn trusted_tcp_serves_status_and_is_not_interchangeable_with_mtls() {
    let trusted = Server::start(
        trusted_files(),
        tempdir::TempDir::new("trusted-tcp").unwrap(),
    );
    let mut client = trusted.connect(&trusted_files()).unwrap();
    assert_eq!(client.fence_to(1).unwrap().active_epoch, Some(1));
    assert!(client.status().is_ok());

    // An mTLS client cannot talk to a trusted server: its TLS hello is not the
    // trusted-mode preface.
    let certificates_dir = tempdir::TempDir::new("trusted-vs-mtls-client").unwrap();
    let mtls = certificates(certificates_dir.path());
    assert!(trusted.connect(&mtls).is_err());
    drop(client);

    // And a trusted client cannot talk to an mTLS server: no plaintext fallback
    // exists, so the server rejects the first bytes.
    let temp = tempdir::TempDir::new("mtls-vs-trusted-client").unwrap();
    let mtls_files = certificates(temp.path());
    let strict = Server::start(mtls_files.clone(), temp);
    let outcome = strict
        .connect(&trusted_files())
        .and_then(|mut client| client.status().map(|_| ()));
    assert!(
        outcome.is_err(),
        "a trusted client must not reach an mTLS server"
    );
    // The legitimate mTLS client still works against the same server.
    assert!(strict.connect(&mtls_files).unwrap().status().is_ok());
}
