//! Database drop, `TargetCleanup` phase: tear a database's target down from
//! its persisted journal without commissioning anything again.

use super::*;

impl LiveTargetManager {
    /// Reconcile the persisted journal (deny connections, disable and drain
    /// every registered namespace, reconcile commissioning controllers) and
    /// remove every registered configfs object, without recreating the host,
    /// ports or commissioning objects. Succeeds only with proof that no
    /// registered object, export or pin remains. With `expected` absent the
    /// caller has proved the journal file exists; it must match `config`.
    pub(crate) fn teardown(
        config: LiveTargetConfig,
        expected: Option<LiveTargetIdentity>,
        fixture_configfs: Option<PathBuf>,
    ) -> io::Result<()> {
        let (configfs, fixture) = match fixture_configfs {
            Some(configfs) => (configfs, true),
            None => {
                check_configfs()?;
                check_path(&config.state_dir, true)?;
                (PathBuf::from(CONFIGFS), false)
            }
        };
        let mut manager = Self::open_inner_mode(
            config,
            configfs,
            fixture,
            expected,
            false,
            #[cfg(test)]
            None,
        )?;
        manager.shutdown()?;
        if !manager.state.objects.is_empty()
            || !manager.state.exports.is_empty()
            || !manager.pins.is_empty()
        {
            return Err(invalid(
                "target teardown left registered objects, exports or pins",
            ));
        }
        Ok(())
    }

    /// Name of the journal file inside a target state directory.
    pub(crate) fn journal_name() -> &'static str {
        STATE
    }
}
