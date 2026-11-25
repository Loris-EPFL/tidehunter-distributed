use super::*;

#[test]
fn byte_budget_counts_queued_groups_and_keeps_control_headroom() {
    let group = [800, 800]
        .into_iter()
        .try_fold(64usize, AppendByteBudget::add_batch_charge)
        .unwrap();
    let reserve = 128;
    let budget = AppendByteBudget::new(group * 2 + reserve, reserve, group).unwrap();
    assert!(budget.admits(group, group));
    assert!(!budget.admits(group * 2, group));
    assert!(!budget.admits(usize::MAX, group));
    assert!(AppendByteBudget::new(group, 1, group).is_err());
    assert!(AppendByteBudget::new(group * 2, 0, group).is_err());
    assert!(AppendByteBudget::add_batch_charge(64, usize::MAX).is_none());
}

#[test]
fn control_slot_is_reserved_for_every_valid_setting() {
    assert!(validate_owner_occupancy(0).is_err());
    assert!(validate_owner_occupancy(APPEND_COMMAND_WINDOW + 1).is_err());
    assert!(validate_owner_occupancy(APPEND_COMMAND_WINDOW).is_ok());
}
