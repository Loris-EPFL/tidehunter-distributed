//! One process-wide set of test TLS identities for the five-node fixtures.
//!
//! `TestCertificates::generate` runs about thirty `openssl` children per call.
//! A child copies every open descriptor of the test process and closes the
//! `O_CLOEXEC` ones only at `exec`, so it holds the `flock` of a lifecycle
//! namespace guard that a parallel fixture owns at that moment. That fixture's
//! next open then fails with "held by another lifecycle operation". The first
//! caller generates the material once, before any fixture can open an
//! authority (every other caller blocks in the `LazyLock` first); every
//! fixture then receives its own copy of the files and spawns nothing.
use super::tls_material::{Identity, TestCertificates};
use std::ffi::OsString;
use std::io;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

/// Largest owner count of a fixture (`new_with_root` asserts 1..=16).
const MAX_OWNERS: usize = 16;

/// A generated file: its name inside the certificate directory, its unix mode
/// and its bytes.
struct Material {
    name: OsString,
    mode: u32,
    bytes: Vec<u8>,
}

struct Master {
    files: Vec<Material>,
    /// File names of the identities; the directory part is replaced per fixture.
    certificates: TestCertificates,
}

/// Generated on first use. The error is kept as text: `io::Error` is not
/// `Clone`, and every later caller reports the same failure.
static MASTER: LazyLock<Result<Master, String>> =
    LazyLock::new(|| generate_master().map_err(|error| error.to_string()));

fn generate_master() -> io::Result<Master> {
    let root = tempdir::TempDir::new("remote-pipeline-tls-master")?;
    let directory = root.path().join("tls");
    let certificates = TestCertificates::generate(&directory, MAX_OWNERS)?;
    let mut files = Vec::new();
    for entry in std::fs::read_dir(&directory)? {
        let entry = entry?;
        files.push(Material {
            name: entry.file_name(),
            mode: entry.metadata()?.permissions().mode(),
            bytes: std::fs::read(entry.path())?,
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

/// The identities of a fixture with `owners` owners, written into the new
/// directory `directory`. Same result as `TestCertificates::generate` except
/// that only the first call of the process spawns `openssl`.
///
/// # Errors
/// Returns a file-system error, or the generation error of the first call.
pub(super) fn certificates(directory: &Path, owners: usize) -> io::Result<TestCertificates> {
    if owners == 0 || owners > MAX_OWNERS {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid test node count",
        ));
    }
    // `LazyLock` blocks every other caller until the first one has generated.
    let master = MASTER
        .as_ref()
        .map_err(|error| io::Error::other(error.clone()))?;
    std::fs::create_dir(directory)?;
    std::fs::set_permissions(directory, std::fs::Permissions::from_mode(0o700))?;
    let directory = std::fs::canonicalize(directory)?;
    for file in &master.files {
        let path = directory.join(&file.name);
        std::fs::write(&path, &file.bytes)?;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(file.mode))?;
    }
    Ok(TestCertificates {
        ca: relocate(&master.certificates.ca, &directory)?,
        authority: relocate_identity(&master.certificates.authority, &directory)?,
        nodes: master.certificates.nodes[..owners]
            .iter()
            .map(|identity| relocate_identity(identity, &directory))
            .collect::<io::Result<_>>()?,
    })
}
