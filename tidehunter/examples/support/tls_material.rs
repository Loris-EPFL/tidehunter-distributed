//! Short-lived test certificates, generated with the installed OpenSSL tool.
//! No downloaded binaries, shared production credentials or private-key logging.
//!
//! `generate` runs `openssl` only on its first call in a process. Each `openssl`
//! child copies every open descriptor of the process and closes the `O_CLOEXEC`
//! ones only at `exec`. A child that starts while another thread owns a `flock`
//! (the lifecycle namespace guard of a database) therefore holds that lock for a
//! short time, and the owner's next lock attempt fails with "held by another
//! lifecycle operation". One generation per process, then plain file writes,
//! keeps the number of such children small and constant.

use std::fs;
use std::io;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::LazyLock;

/// Largest node count `generate` accepts. The shared master holds this many.
const MAX_NODES: usize = 16;

#[derive(Clone, Debug)]
pub struct Identity {
    pub certificate: PathBuf,
    pub private_key: PathBuf,
    pub server_name: String,
}

#[derive(Clone, Debug)]
pub struct TestCertificates {
    pub ca: PathBuf,
    pub authority: Identity,
    pub nodes: Vec<Identity>,
}

/// One generated file: its name inside the certificate directory, its unix
/// mode and its bytes.
struct Material {
    name: std::ffi::OsString,
    mode: u32,
    bytes: Vec<u8>,
}

/// The process-wide certificate set: the files and the identities, with the
/// directory part of every path still pointing at the temporary master.
struct Master {
    files: Vec<Material>,
    certificates: TestCertificates,
}

/// Generated on first use. The error is kept as text because `io::Error` is not
/// `Clone`; every later caller reports the same failure. Other callers block in
/// the `LazyLock` until the first one has finished.
static MASTER: LazyLock<Result<Master, String>> =
    LazyLock::new(|| generate_master().map_err(|error| error.to_string()));

fn generate_master() -> io::Result<Master> {
    let base = create_master_directory()?;
    let master = read_master(&base.join("tls"));
    // The master lives in memory from here on; the files must not outlive the call.
    let removed = fs::remove_dir_all(&base);
    let master = master?;
    removed?;
    Ok(master)
}

/// Creates a fresh private directory under the system temporary directory. The
/// name holds the process id and a counter, so a leftover of a crashed process
/// with the same id is skipped, never reused.
fn create_master_directory() -> io::Result<PathBuf> {
    for attempt in 0..64 {
        let base = std::env::temp_dir().join(format!(
            "tidehunter-tls-master-{}-{attempt}",
            std::process::id()
        ));
        match fs::create_dir(&base) {
            Ok(()) => {
                fs::set_permissions(&base, fs::Permissions::from_mode(0o700))?;
                return Ok(base);
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error),
        }
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        "no free TLS master directory name",
    ))
}

fn read_master(directory: &Path) -> io::Result<Master> {
    let certificates = TestCertificates::generate_files(directory, MAX_NODES)?;
    let mut files = Vec::new();
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        files.push(Material {
            name: entry.file_name(),
            mode: entry.metadata()?.permissions().mode(),
            bytes: fs::read(entry.path())?,
        });
    }
    Ok(Master {
        files,
        certificates,
    })
}

fn relocate(path: &Path, directory: &Path) -> io::Result<PathBuf> {
    let name = path.file_name().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidData, "certificate path has no name")
    })?;
    Ok(directory.join(name))
}

fn relocate_identity(identity: &Identity, directory: &Path) -> io::Result<Identity> {
    Ok(Identity {
        certificate: relocate(&identity.certificate, directory)?,
        private_key: relocate(&identity.private_key, directory)?,
        server_name: identity.server_name.clone(),
    })
}

impl TestCertificates {
    /// Creates `directory` (it must not exist) with the identities of the
    /// authority and of `nodes` owners. Only the first call of the process
    /// starts `openssl`; later calls write copies of the same files.
    pub fn generate(directory: &Path, nodes: usize) -> io::Result<Self> {
        if nodes == 0 || nodes > MAX_NODES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid test node count",
            ));
        }
        let master = MASTER
            .as_ref()
            .map_err(|error| io::Error::other(error.clone()))?;
        fs::create_dir(directory)?;
        fs::set_permissions(directory, fs::Permissions::from_mode(0o700))?;
        let directory = fs::canonicalize(directory)?;
        for file in &master.files {
            let path = directory.join(&file.name);
            fs::write(&path, &file.bytes)?;
            fs::set_permissions(&path, fs::Permissions::from_mode(file.mode))?;
        }
        Ok(Self {
            ca: relocate(&master.certificates.ca, &directory)?,
            authority: relocate_identity(&master.certificates.authority, &directory)?,
            nodes: master.certificates.nodes[..nodes]
                .iter()
                .map(|identity| relocate_identity(identity, &directory))
                .collect::<io::Result<_>>()?,
        })
    }

    /// Runs `openssl` for the whole set. Only `generate_master` calls it.
    fn generate_files(directory: &Path, nodes: usize) -> io::Result<Self> {
        fs::create_dir(directory)?;
        fs::set_permissions(directory, fs::Permissions::from_mode(0o700))?;
        let directory = fs::canonicalize(directory)?;
        let ca_pem = directory.join("ca.pem");
        let ca = directory.join("ca.der");
        let ca_key = directory.join("ca.key");
        // Do not inherit host x509_extensions: OpenSSL 1.1 appends -addext to
        // defaults, producing duplicate basicConstraints rejected by rustls.
        let req_config = directory.join("ca-req.cnf");
        fs::write(&req_config, "[req]\ndistinguished_name=dn\n[dn]\n")?;
        let mut command = Command::new("openssl");
        command
            .args(["req", "-config"])
            .arg(&req_config)
            .args([
                "-x509",
                "-newkey",
                "ec",
                "-pkeyopt",
                "ec_paramgen_curve:P-256",
                "-nodes",
                "-sha256",
                "-days",
                "1",
                "-subj",
                "/CN=tidehunter-local-test-ca",
                "-addext",
                "basicConstraints=critical,CA:TRUE",
                "-addext",
                "keyUsage=critical,keyCertSign,cRLSign",
                "-keyout",
            ])
            .arg(&ca_key)
            .arg("-out")
            .arg(&ca_pem);
        run(command)?;
        let mut convert_ca = Command::new("openssl");
        convert_ca
            .args(["x509", "-in"])
            .arg(&ca_pem)
            .args(["-outform", "DER", "-out"])
            .arg(&ca);
        run(convert_ca)?;
        let authority = issue(
            &directory,
            &ca_pem,
            &ca_key,
            "tidehunter-authority",
            false,
            2,
        )?;
        let nodes = (0..nodes)
            .map(|node| {
                issue(
                    &directory,
                    &ca_pem,
                    &ca_key,
                    &format!("tidehunter-node-{node}"),
                    true,
                    node + 3,
                )
            })
            .collect::<io::Result<_>>()?;
        Ok(Self {
            ca,
            authority,
            nodes,
        })
    }
}

fn issue(
    directory: &Path,
    ca: &Path,
    ca_key: &Path,
    name: &str,
    server: bool,
    serial: usize,
) -> io::Result<Identity> {
    let key_pem = directory.join(format!("{name}.key.pem"));
    let key = directory.join(format!("{name}.pk8"));
    let certificate_pem = directory.join(format!("{name}.cert.pem"));
    let certificate = directory.join(format!("{name}.der"));
    let csr = directory.join(format!("{name}.csr"));
    let extensions = directory.join(format!("{name}.ext"));
    fs::write(
        &extensions,
        format!(
            "basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature\nextendedKeyUsage={}\nsubjectAltName=DNS:{name},DNS:localhost,IP:127.0.0.1\n",
            if server { "serverAuth" } else { "clientAuth" }
        ),
    )?;
    let mut request = Command::new("openssl");
    request
        .args([
            "req",
            "-new",
            "-newkey",
            "ec",
            "-pkeyopt",
            "ec_paramgen_curve:P-256",
            "-nodes",
            "-sha256",
            "-subj",
        ])
        .arg(format!("/CN={name}"))
        .arg("-keyout")
        .arg(&key_pem)
        .arg("-out")
        .arg(&csr);
    run(request)?;
    let mut sign = Command::new("openssl");
    sign.args(["x509", "-req", "-sha256", "-days", "1", "-in"])
        .arg(&csr)
        .arg("-CA")
        .arg(ca)
        .arg("-CAkey")
        .arg(ca_key)
        .arg("-set_serial")
        .arg(serial.to_string())
        .arg("-extfile")
        .arg(extensions)
        .arg("-out")
        .arg(&certificate_pem);
    run(sign)?;
    let mut cert_der = Command::new("openssl");
    cert_der
        .args(["x509", "-in"])
        .arg(&certificate_pem)
        .args(["-outform", "DER", "-out"])
        .arg(&certificate);
    run(cert_der)?;
    let mut key_der = Command::new("openssl");
    key_der
        .args(["pkcs8", "-topk8", "-nocrypt", "-in"])
        .arg(&key_pem)
        .args(["-outform", "DER", "-out"])
        .arg(&key);
    run(key_der)?;
    Ok(Identity {
        certificate,
        private_key: key,
        server_name: name.to_owned(),
    })
}

fn run(mut command: Command) -> io::Result<()> {
    let output = command.output()?;
    if !output.status.success() {
        return Err(io::Error::other(format!(
            "OpenSSL certificate generation failed ({}): {}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    Ok(())
}
