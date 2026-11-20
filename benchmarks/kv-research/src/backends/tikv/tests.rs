use super::*;
use tonic::Status;

#[test]
fn validates_client_and_http_endpoint_shapes() {
    assert_eq!(
        validate_endpoint_url("127.0.0.1:2379", true).unwrap(),
        "http://127.0.0.1:2379"
    );
    assert!(validate_endpoint_url("127.0.0.1:2379", false).is_err());
    assert!(validate_endpoint_url("https://pd.example:2379", false).is_ok());
    assert!(validate_endpoint_url("http://user:secret@pd.example:2379", false).is_err());
}

#[test]
fn preflight_rejects_bare_http_api_endpoint() {
    let options = serde_json::json!({
        "pd_endpoints": ["127.0.0.1:2379"],
        "pd_api_endpoints": ["127.0.0.1:2379"],
        "required_replication_factor": 1
    });
    assert!(validate_options(&options).is_err());

    let options = serde_json::json!({
        "pd_endpoints": ["127.0.0.1:2379"],
        "pd_api_endpoints": ["http://127.0.0.1:2379"],
        "required_replication_factor": 1
    });
    assert!(validate_options(&options).is_ok());
}

#[test]
fn tonic_deadline_is_classified_as_timeout() {
    let error = normalize_tikv_error(tikv_client::Error::GrpcAPI(Status::deadline_exceeded(
        "deadline",
    )));
    let io_error = error.downcast_ref::<io::Error>().unwrap();
    assert_eq!(io_error.kind(), io::ErrorKind::TimedOut);
}
