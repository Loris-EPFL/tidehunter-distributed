use super::*;

#[test]
fn wal_export_names_are_stable_and_cover_subsystem_boundaries() {
    for (file, subsystem, nsid) in [(0, 0, 1), (1, 0, 2), (1023, 0, 1024), (1024, 1, 1)] {
        let name = wal_export_name(1024, [1; 16], 2, 3, file);
        assert_eq!((name.subsystem, name.nsid), (subsystem, nsid));
        assert_eq!(name, wal_export_name(1024, [1; 16], 2, 3, file));
        let one = wal_export_name(1, [1; 16], 2, 3, file);
        assert_eq!((one.subsystem, one.nsid), (file, 1));
    }
    let base = wal_export_name(1024, [1; 16], 2, 3, 0);
    assert_eq!(base.nqn, wal_export_name(1024, [1; 16], 2, 3, 1).nqn);
    for changed in [
        wal_export_name(1024, [2; 16], 2, 3, 0),
        wal_export_name(1024, [1; 16], 4, 3, 0),
        wal_export_name(1024, [1; 16], 2, 4, 0),
        wal_export_name(1024, [1; 16], 2, 3, 1),
    ] {
        assert_ne!(base.uuid, changed.uuid);
    }
    assert_ne!(base.nqn, wal_export_name(1024, [1; 16], 2, 3, 1024).nqn);
}
