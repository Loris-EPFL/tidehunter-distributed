//! Frame checks of one live recovery page.

use super::{exact_frame_payload, retryable_recovery_failure};
use crate::crc::CrcFrame;
use std::io;

#[test]
fn recovery_rejects_corrupt_or_nonexact_inner_frames() {
    let frame = CrcFrame::new(&vec![1, 2, 3]);
    let bytes = frame.as_ref();
    assert_eq!(exact_frame_payload(bytes).unwrap(), &[1, 2, 3]);
    let mut corrupt = bytes.to_vec();
    corrupt[8] ^= 1;
    assert!(exact_frame_payload(&corrupt).is_err());
    assert!(exact_frame_payload(&bytes[..bytes.len() - 1]).is_err());
    let mut trailing = bytes.to_vec();
    trailing.push(0);
    assert!(exact_frame_payload(&trailing).is_err());
}

/// A short read or an `EIO`/`EBADF` on a controller the probe says is live is
/// a media or namespace verdict: it must stay fatal. The same class retries
/// only after the error path proved the controller is gone.
#[test]
fn recovery_retries_a_lost_controller_but_never_a_live_read_verdict() {
    let short = io::Error::new(io::ErrorKind::UnexpectedEof, "short namespace read");
    assert!(
        !retryable_recovery_failure(&short, false),
        "a short read on a live controller is fatal: {short}"
    );
    assert!(
        retryable_recovery_failure(&short, true),
        "the same short read after the controller vanished is retryable: {short}"
    );
    let eio = io::Error::from_raw_os_error(libc::EIO);
    assert!(
        !retryable_recovery_failure(&eio, false),
        "an EIO on a live controller is a media verdict: {eio}"
    );
    assert!(
        retryable_recovery_failure(&eio, true),
        "an EIO on a verifiably lost controller is retryable: {eio}"
    );
    let owner_verdict = io::Error::new(io::ErrorKind::InvalidData, "receipt mismatch");
    assert!(
        !retryable_recovery_failure(&owner_verdict, true),
        "an owner verdict is never retried, even after a loss: {owner_verdict}"
    );
    let lost = crate::distributed::nvmeof::live_namespace::controller_lost_error();
    assert!(
        retryable_recovery_failure(&lost, true),
        "a typed controller loss with the sysfs verdict is retryable: {lost}"
    );
}
