use super::Decoder;
use crate::backend::BackendResult;

#[test]
fn decodes_fixed_width_and_sequence_types() -> BackendResult<()> {
    let mut bytes = vec![7, 1];
    bytes.extend_from_slice(&0x0102_0304u32.to_le_bytes());
    bytes.extend_from_slice(&0x0a0b_0c0d_0e0f_1011u64.to_le_bytes());
    bytes.extend_from_slice(&[2, 0xaa, 0xbb]); // bytes
    bytes.extend_from_slice(&[2, b'o', b'k']); // string
    bytes.extend_from_slice(&[0, 1, 1, 0x55]); // None, Some([0x55])
    bytes.extend_from_slice(&[2, 1, 0]); // vec![true, false]
    bytes.extend_from_slice(&[9; 32]);
    let mut d = Decoder::new(&bytes);
    assert_eq!(d.u8()?, 7);
    assert!(d.bool()?);
    assert_eq!(d.u32()?, 0x0102_0304);
    assert_eq!(d.u64()?, 0x0a0b_0c0d_0e0f_1011);
    assert_eq!(d.bytes()?, vec![0xaa, 0xbb]);
    assert_eq!(d.string()?, "ok");
    assert_eq!(d.option(Decoder::bytes)?, None);
    assert_eq!(d.option(Decoder::bytes)?, Some(vec![0x55]));
    assert_eq!(d.vec(Decoder::bool)?, vec![true, false]);
    assert_eq!(d.hash()?, [9; 32]);
    d.finish()
}

#[test]
fn uleb128_is_canonical_and_bounded() -> BackendResult<()> {
    assert_eq!(Decoder::new(&[0x7f]).uleb()?, 127);
    assert_eq!(Decoder::new(&[0x80, 0x01]).uleb()?, 128);
    assert_eq!(
        Decoder::new(&[0xff, 0xff, 0xff, 0xff, 0x07]).uleb()?,
        (1 << 31) - 1
    );
    // A zero final group after the first byte is a redundant encoding.
    assert!(Decoder::new(&[0x80, 0x00]).uleb().is_err());
    assert!(
        Decoder::new(&[0x80, 0x80, 0x80, 0x80, 0x08])
            .uleb()
            .is_err()
    );
    assert!(
        Decoder::new(&[0x80, 0x80, 0x80, 0x80, 0x80, 0x01])
            .uleb()
            .is_err()
    );
    assert!(Decoder::new(&[0x80]).uleb().is_err());
    Ok(())
}

#[test]
fn rejects_invalid_and_truncated_payloads() {
    assert!(Decoder::new(&[2]).bool().is_err());
    assert!(Decoder::new(&[2, 0]).option(Decoder::bytes).is_err());
    assert!(Decoder::new(&[1, 0xff]).string().is_err());
    assert!(Decoder::new(&[1, 2, 3]).u32().is_err());
    // A length beyond the remaining payload fails before allocating.
    assert!(Decoder::new(&[0xff, 0xff, 0x03, 1]).bytes().is_err());
    assert!(Decoder::new(&[1]).finish().is_err());
}
