use super::remote_error;
use crate::distributed::storage::LocalDbError;

/// The Exact-era marker reaches the adapter as caller input, not as an
/// undifferentiated I/O failure, and the mapping is specific: an error
/// without the marker keeps its own kind. This covers the adapter half
/// only; format_gate_tests covers the engine open that produces it.
#[test]
fn exact_era_manifest_maps_to_invalid_input_and_others_keep_their_kind() {
    let marked = remote_error(
        LocalDbError::new("unsupported legacy distributed mode exact_index")
            .mark_exact_era_manifest(),
    );
    match marked {
        super::DbError::Io(error) => {
            assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput)
        }
        other => panic!("expected an IO error, got {other:?}"),
    }
    let unmarked = remote_error(LocalDbError::with_kind(
        std::io::ErrorKind::TimedOut,
        "busy",
    ));
    match unmarked {
        super::DbError::Io(error) => assert_eq!(error.kind(), std::io::ErrorKind::TimedOut),
        other => panic!("expected an IO error, got {other:?}"),
    }
}
