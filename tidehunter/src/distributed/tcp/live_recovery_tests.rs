//! Frame checks of one live recovery page.

use super::exact_frame_payload;
use crate::crc::CrcFrame;

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
