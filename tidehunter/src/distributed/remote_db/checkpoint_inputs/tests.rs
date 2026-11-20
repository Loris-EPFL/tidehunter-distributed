use super::*;

/// A checkpointed WAL position is trusted only when its partition is
/// configured, an owner endpoint serves it, and that endpoint's history holds
/// the epoch. A configured partition without an endpoint is rejected.
#[test]
fn checkpoint_positions_need_a_configured_partition_with_an_owner_endpoint() {
    let endpoints = [RemoteEndpoint {
        partition: 0,
        address: String::new(),
        server_name: String::new(),
    }];
    let histories = [vec![1, 2]];
    let configured = [0, 1];
    assert!(position_has_owner_history(
        &configured,
        &endpoints,
        &histories,
        0,
        2
    ));
    // The epoch is not in the endpoint's history.
    assert!(!position_has_owner_history(
        &configured,
        &endpoints,
        &histories,
        0,
        3
    ));
    // Configured, but no endpoint serves partition 1.
    assert!(!position_has_owner_history(
        &configured,
        &endpoints,
        &histories,
        1,
        1
    ));
    // Served by an endpoint, but absent from the storage configuration.
    assert!(!position_has_owner_history(
        &[1],
        &endpoints,
        &histories,
        0,
        1
    ));
}
