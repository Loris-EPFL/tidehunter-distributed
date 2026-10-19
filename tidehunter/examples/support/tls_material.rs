//! Short-lived test certificates, generated with the installed OpenSSL tool.
//! No downloaded binaries, shared production credentials or private-key logging.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;

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

impl TestCertificates {
    pub fn generate(directory: &Path, nodes: usize) -> io::Result<Self> {
        if nodes == 0 || nodes > 16 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid test node count",
            ));
        }
        fs::create_dir(directory)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(directory, fs::Permissions::from_mode(0o700))?;
        }
        let directory = fs::canonicalize(directory)?;
        let ca_pem = directory.join("ca.pem");
        let ca = directory.join("ca.der");
        let ca_key = directory.join("ca.key");
        let mut command = Command::new("openssl");
        command
            .args([
                "req",
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
