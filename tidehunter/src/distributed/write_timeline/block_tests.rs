use super::*;

#[test]
fn block_spans_are_disabled_bounded_and_reset_on_arm() {
    let timeline = WriteTimeline::default();
    assert!(timeline.block_started().is_none());
    timeline.arm(1, 1).unwrap();
    timeline.block_finished(WriteBlockStage::OwnerQueue, timeline.block_started(), 7);
    let export = timeline.finish().unwrap();
    assert_eq!(export.blocking_spans.len(), 1);
    assert_eq!(export.blocking_spans[0].calls, 1);
    assert_eq!(export.blocking_spans[0].max_depth, 7);
    assert!(timeline.block_started().is_none());
    timeline.arm(1, 1).unwrap();
    timeline.blocks.origin_ns.store(0, Ordering::Relaxed);
    timeline.block_finished(WriteBlockStage::Publish, timeline.block_started(), 128);
    let export = timeline.finish().unwrap();
    assert_eq!(export.blocking_spans.len(), 1);
    assert_eq!(export.blocking_spans[0].second, BLOCK_BUCKETS - 1);
    assert_eq!(export.blocking_spans[0].max_depth, 128);
}
