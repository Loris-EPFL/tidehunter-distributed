use super::*;
use scylla::errors::WriteType;

#[test]
fn client_request_deadline_is_classified_as_timeout() {
    let original = ExecutionError::RequestTimeout(Duration::from_millis(10));
    let error = normalize_cql_error(original);
    let io_error = error.downcast_ref::<io::Error>().unwrap();
    assert_eq!(io_error.kind(), io::ErrorKind::TimedOut);
}

#[test]
fn server_read_and_write_timeouts_are_normalized() {
    for db_error in [
        DbError::ReadTimeout {
            consistency: Consistency::Quorum,
            received: 1,
            required: 2,
            data_present: false,
        },
        DbError::WriteTimeout {
            consistency: Consistency::Quorum,
            received: 1,
            required: 2,
            write_type: WriteType::Simple,
        },
    ] {
        let original = ExecutionError::LastAttemptError(RequestAttemptError::DbError(
            db_error,
            "server timeout".to_owned(),
        ));
        let error = normalize_cql_error(original);
        assert_eq!(
            error.downcast_ref::<io::Error>().unwrap().kind(),
            io::ErrorKind::TimedOut
        );
    }
}
