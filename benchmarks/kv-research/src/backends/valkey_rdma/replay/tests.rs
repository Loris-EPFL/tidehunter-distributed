use super::*;
#[test]
fn status_replies_and_binary_values_remain_distinct() {
    assert!(
        matches!(value(Response::Bytes(b"QUEUED".to_vec()), true), redis::Value::SimpleString(s) if s == "QUEUED")
    );
    assert!(
        matches!(value(Response::Bytes(b"OK".to_vec()), false), redis::Value::BulkString(b) if b == b"OK")
    );
    assert!(
        matches!(value(Response::Array(vec![Response::Bytes(b"OK".to_vec())]), true), redis::Value::Array(v) if matches!(&v[0], redis::Value::SimpleString(s) if s == "OK"))
    );
}
