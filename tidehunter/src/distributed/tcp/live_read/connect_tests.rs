//! The connect budget and the typed class of a connect deadline.

use super::*;

#[test]
fn connect_budget_takes_the_earlier_deadline() {
    let since = Instant::now();
    let call = since + Duration::from_secs(5);
    assert_eq!(
        connect_budget(call, since, Duration::from_secs(2)),
        since + Duration::from_secs(2),
        "the connect budget caps a later call deadline"
    );
    assert_eq!(
        connect_budget(
            since + Duration::from_secs(1),
            since,
            Duration::from_secs(2)
        ),
        since + Duration::from_secs(1),
        "an earlier call deadline stays the bound"
    );
    assert_eq!(
        connect_budget(call, since, Duration::from_secs(9)),
        call,
        "a budget later than the call does not extend it"
    );
}

#[test]
fn connect_budget_counts_from_the_first_attempt() {
    let since = Instant::now();
    let first = connect_budget(
        since + Duration::from_secs(9),
        since,
        Duration::from_secs(2),
    );
    // A later call deadline does not move the budget: the elapsed connect
    // time is charged once, by `since`, and never subtracted again.
    let later = connect_budget(
        since + Duration::from_secs(30),
        since,
        Duration::from_secs(2),
    );
    assert_eq!(
        first, later,
        "the connect budget is fixed by the first attempt"
    );
    assert_eq!(
        first,
        since + Duration::from_secs(2),
        "the budget is the first attempt plus the connect timeout"
    );
}

#[test]
fn a_connect_timeout_keeps_its_class() {
    let ConnectError::Failed(deadline) =
        failed_io(std::io::Error::from(std::io::ErrorKind::TimedOut))
    else {
        panic!("a deadline is a failed setup, not a full pool");
    };
    assert_eq!(
        deadline.kind(),
        std::io::ErrorKind::TimedOut,
        "the connect deadline keeps its class: {deadline}"
    );
    let ConnectError::Failed(other) =
        failed_io(std::io::Error::from(std::io::ErrorKind::ConnectionRefused))
    else {
        panic!("a refused connect is a failed setup, not a full pool");
    };
    assert_ne!(
        other.kind(),
        std::io::ErrorKind::TimedOut,
        "only a deadline keeps the timeout class: {other}"
    );
}

#[test]
fn a_spent_pool_budget_is_a_timeout() {
    let error = pool_deadline(std::io::Error::from(std::io::ErrorKind::WouldBlock));
    assert_eq!(
        error.kind(),
        std::io::ErrorKind::TimedOut,
        "a full pool at its connect deadline is a timeout: {error}"
    );
}
