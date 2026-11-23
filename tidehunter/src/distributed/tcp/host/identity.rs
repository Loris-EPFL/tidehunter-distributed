//! Identities both sides of a database lifecycle derive from the same inputs:
//! the effective live initiator of one (database, partition) pair, the target
//! subsystem scope, and the owner client-trust principal.
//!
//! The coordinator and the owner compute these independently; neither trusts a
//! value the other reports. Configured inputs stay in the provision request so
//! provision equality compares what the operator configured, not a derivation.

use std::fmt::Write as _;

/// Prefix of every effective initiator NQN (`nqn.<date>.<reverse domain>:`).
pub(crate) const NQN_PREFIX: &str = "nqn.2026-01.local.tidehunter:";
const INITIATOR_DOMAIN: &[u8] = b"tidehunter-db-initiator-v1";
const PRINCIPAL_DOMAIN: &[u8] = b"tidehunter-owner-client-trust-v1";

/// 32 lowercase hexadecimal digits of a database identity.
pub fn database_hex(database: &[u8; 16]) -> String {
    let mut text = String::with_capacity(32);
    for byte in database {
        // Writing to a String cannot fail.
        let _ = write!(text, "{byte:02x}");
    }
    text
}

/// Parse exactly 32 lowercase hexadecimal digits; anything else is `None`.
pub fn parse_database_hex(text: &str) -> Option<[u8; 16]> {
    if text.len() != 32
        || !text
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return None;
    }
    let mut database = [0u8; 16];
    for (index, byte) in database.iter_mut().enumerate() {
        *byte = u8::from_str_radix(text.get(index * 2..index * 2 + 2)?, 16).ok()?;
    }
    Some(database)
}

/// `db-<32 hex>-p<8 hex>-`: the DB/partition scope prefixed to the target's
/// subsystem name suffix, so two databases on one host never share a name.
pub fn subsystem_scope(database: &[u8; 16], partition: u32) -> String {
    format!("db-{}-p{partition:08x}-", database_hex(database))
}

/// Effective host NQN of the coordinator initiator for one database partition.
/// At most 79 bytes, inside the 223-byte NQN bound.
pub fn effective_host_nqn(database: &[u8; 16], partition: u32) -> String {
    format!(
        "{NQN_PREFIX}db-{}-p{partition:08x}-host",
        database_hex(database)
    )
}

/// Effective host UUID: the first 16 bytes of BLAKE2b-256 over the domain,
/// NUL, the configured host ID, the database ID and the big-endian partition.
pub fn effective_host_id(configured: &[u8; 16], database: &[u8; 16], partition: u32) -> [u8; 16] {
    let mut hash = blake2b_simd::Params::new().hash_length(32).to_state();
    hash.update(INITIATOR_DOMAIN);
    hash.update(&[0]);
    hash.update(configured);
    hash.update(database);
    hash.update(&partition.to_be_bytes());
    let digest = hash.finalize();
    let mut id = [0u8; 16];
    id.copy_from_slice(&digest.as_bytes()[..16]);
    id
}

/// Owner client-trust principal: BLAKE2b-256 of the domain, NUL, then the
/// sorted, deduplicated client CA anchor DER encodings, each prefixed with its
/// big-endian `u32` length. Rotating a leaf within the trust domain keeps it.
pub fn principal_from_anchors(anchors: &[Vec<u8>]) -> Result<[u8; 32], &'static str> {
    let mut sorted: Vec<&[u8]> = anchors.iter().map(Vec::as_slice).collect();
    sorted.sort_unstable();
    sorted.dedup();
    if sorted.is_empty() || sorted.iter().any(|anchor| anchor.is_empty()) {
        return Err("owner client trust policy has no CA anchor");
    }
    let mut hash = blake2b_simd::Params::new().hash_length(32).to_state();
    hash.update(PRINCIPAL_DOMAIN);
    hash.update(&[0]);
    for anchor in sorted {
        let len = u32::try_from(anchor.len()).map_err(|_| "client CA anchor exceeds u32")?;
        hash.update(&len.to_be_bytes());
        hash.update(anchor);
    }
    let mut principal = [0u8; 32];
    principal.copy_from_slice(hash.finalize().as_bytes());
    Ok(principal)
}
