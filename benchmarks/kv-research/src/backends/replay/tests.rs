use super::*;
#[test]
fn reference_capability_is_atomic_within_one_node_database() {
    let capability = ReplayCommitCapability::node_db();
    assert_eq!(capability.commit_mapping, "native_batch");
    assert_eq!(capability.atomic_scope, "node_db");
    assert_eq!(capability.batch_visibility, "atomic");
    assert_eq!(capability.failure_atomicity, "transaction");
}
#[test]
fn singleton_and_cross_table_keys_are_injective_without_changing_suffixes() {
    let singleton = physical_key(11, &Bytes::new(), false);
    assert_eq!(&*singleton, b"\0\x0b");
    let key = Bytes::from_static(b"\0{binary}\xff");
    let a = physical_key(1, &key, true);
    let b = physical_key(4, &key, true);
    assert_ne!(a, b);
    assert_eq!(&a[4..], key.as_ref());
    assert!(a.starts_with(b"{}"));
}
