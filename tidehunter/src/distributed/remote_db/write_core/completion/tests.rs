use super::*;
use crate::distributed::remote_db::pipeline_tests::Fixture;
use crate::distributed::remote_db::write_core::sequencer::Resolution;
use crate::distributed::types::{BatchId, BatchVersion};
use std::time::Instant;

#[test]
fn uncertain_append_fails_every_slot_without_reclaiming_a_generation() {
    let fixture = Fixture::new();
    let database = fixture.open();
    let core = Arc::clone(&database.core);
    let completion = CoreCompletion::new(&core);
    for sequence in 0..2 {
        assert_eq!(
            core.sequencer.allocate(BatchId {
                database: fixture.config.storage.database,
                operation: fixture.operation(0, sequence),
            }),
            sequence
        );
        core.sequencer
            .transition(sequence, Resolution::Allocated, Resolution::Ready)
            .unwrap();
    }
    let causal = BatchVersion {
        epoch: core.epoch,
        sequence: 1,
    };
    completion.failed(
        0,
        &[0, 1],
        LocalDbError::unknown(causal, "injected uncertain owner append"),
    );
    for sequence in 0..2 {
        let version = BatchVersion {
            epoch: core.epoch,
            sequence,
        };
        let error = core.sequencer.wait(version, Instant::now()).unwrap_err();
        assert_eq!(error.uncertain_version, Some(version));
        assert!(error.message().contains("injected uncertain owner append"));
        assert_eq!(
            core.sequencer.resolution(sequence),
            Some(Resolution::Unknown)
        );
        core.release_sequence(sequence).unwrap();
    }
    assert_eq!(core.sequencer.reuse_next(), 0);
    assert_eq!(core.publisher.lock().published_next, 0);
    assert!(core.ensure_healthy().is_err());
    assert!(core.reads.failed());
    assert!(core.first_failure.get().unwrap().contains("sequence: 1"));
    assert!(core.shutdown().is_err());
}
