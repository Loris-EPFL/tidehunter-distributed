//! Database drop, `TargetCleanup` phase: tear a database's target down from
//! its configfs scope without commissioning anything again.

use super::*;

impl LiveTargetManager {
    /// Reconcile the scoped configfs objects (disable and seal every enabled
    /// namespace, disconnect commissioning controllers) and remove them,
    /// without recreating the host, ports or commissioning objects. Succeeds
    /// only with proof that no export or pin remains. With `expected` absent
    /// the caller has proved that `target-id` exists; it must match `config`.
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
        if !manager.exports.is_empty() || !manager.pins.is_empty() {
            return Err(invalid("target teardown left registered exports or pins"));
        }
        Ok(())
    }

    /// Name of the first-open witness inside a target state directory.
    pub(crate) fn identity_name() -> &'static str {
        TARGET_ID
    }
}
