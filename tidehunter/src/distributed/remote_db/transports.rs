//! Transport lifetime is independent of the durable authority epoch.
//!
//! eRPC §§3.1/4.2.2 retains endpoint/buffer ownership across logical sessions.
//! Here only a successfully drained, sealed database returns its mode-validated
//! connections. Reopen still refreshes status, fences every node and recovers
//! the complete checkpoint/tail. No failed connection is retried in place.
use super::*;

pub(super) type SharedClient = Arc<Mutex<Client>>;

/// A failed open must not leave idle kernel controllers connected. The durable
/// connection journal retains uncertain ownership if the bounded drain fails.
#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
pub(super) struct LivePoolOpenGuard {
    pool: Arc<super::super::nvmeof::live_pool::LiveControllerPool>,
    timeout: Duration,
    armed: bool,
}

#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
impl LivePoolOpenGuard {
    pub(super) fn new(
        pool: Arc<super::super::nvmeof::live_pool::LiveControllerPool>,
        timeout: Duration,
    ) -> Self {
        Self {
            pool,
            timeout,
            armed: true,
        }
    }

    pub(super) fn disarm(&mut self) {
        self.armed = false;
    }
}

#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
impl Drop for LivePoolOpenGuard {
    fn drop(&mut self) {
        if self.armed
            && let Err(error) = self.pool.shutdown(Instant::now() + self.timeout)
        {
            // Drop cannot return a secondary error. The persistent journal
            // prevents reuse; report the unresolved cleanup rather than hide it.
            eprintln!("live NVMe failed-open controller cleanup remains quarantined: {error}");
        }
    }
}

/// Optional process-owned transport lifetime for clean database reopen.
///
/// Holds at most one idle database's bounded connection set. Credentials, shape
/// and configuration must match exactly; changing any of them discards that set.
/// Dropping the pool closes idle connections. It retains no index, read cache,
/// authority epoch or durability acknowledgement.
#[derive(Default)]
pub struct RemoteTransportPool {
    idle: Mutex<Option<IdleTransports>>,
}

pub(super) struct IdleTransports {
    pub key: [u8; 32],
    pub controls: Vec<SharedClient>,
    pub reads: Vec<SharedClient>,
}

impl RemoteTransportPool {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Release idle transports. Does not close a currently checked-out database.
    pub fn clear(&self) {
        let idle = self.idle.lock().take();
        drop(idle);
    }

    pub(super) fn take(&self, key: [u8; 32]) -> Option<IdleTransports> {
        let idle = self.idle.lock().take();
        idle.filter(|idle| idle.key == key)
    }

    pub(super) fn retain(&self, idle: IdleTransports) {
        // Close superseded resources outside the pool lock: provider teardown
        // must not serialize another database's checkout or request progress.
        let previous = self.idle.lock().replace(idle);
        drop(previous);
    }
}

pub(super) fn pool_key(
    config: &RemoteDatabaseConfig,
    shape_yaml: &str,
    credentials: [u8; 32],
) -> Result<[u8; 32], LocalDbError> {
    let encoded =
        bincode::serialize(&(config, shape_yaml, credentials)).map_err(LocalDbError::component)?;
    Ok(Blake2b::<U32>::digest(encoded).into())
}

pub(super) struct Connections {
    pub clients: Vec<Client>,
    pub reused: usize,
    pub elapsed_ns: u64,
}

pub(super) fn connect(
    config: &RemoteDatabaseConfig,
    shape: &KeyShape,
    tls_config: &super::super::tcp::ClientTlsConfig,
    lanes: usize,
    retained: Option<Vec<SharedClient>>,
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))] pool: Option<
        Arc<super::super::nvmeof::live_pool::LiveControllerPool>,
    >,
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))] metrics: Option<
        Arc<super::super::nvmeof::live_metrics::LiveNvmeMetrics>,
    >,
) -> Result<Connections, LocalDbError> {
    let started = Instant::now();
    let count = config.endpoints.len() * lanes;
    let reused = retained.as_ref().map_or(0, Vec::len);
    if retained
        .as_ref()
        .is_some_and(|clients| clients.len() != count)
    {
        return Err(LocalDbError::new("retained transport inventory differs"));
    }
    let mut retained = retained.unwrap_or_default().into_iter();
    let mut jobs: Vec<_> = config
        .endpoints
        .iter()
        .flat_map(|endpoint| (0..lanes).map(move |_| endpoint))
        .map(|endpoint| (endpoint, retained.next()))
        .collect();
    let clients = endpoint_map(&mut jobs, |_, (endpoint, previous)| {
        let client = if let Some(client) = previous.take() {
            let mut client = Arc::try_unwrap(client)
                .map_err(|_| LocalDbError::new("retained transport still has an owner"))?
                .into_inner();
            client
                .refresh_for_reuse()
                .map_err(LocalDbError::component)?;
            client
        } else {
            let mut tls = config.tls.clone();
            tls.server_name = endpoint.server_name.clone();
            Client::connect_shared(
                &endpoint.address,
                &tls,
                tls_config.clone(),
                config.timeout,
                config.partition_config(1, endpoint.partition),
                shape,
            )
            .map_err(LocalDbError::component)?
        };
        if client.connected_status().live_nvmeof != config.live_nvme.is_some() {
            return Err(LocalDbError::new(
                "owner and coordinator data-plane modes differ",
            ));
        }
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        let client = {
            let mut client = client;
            if let Some(live) = &config.live_nvme {
                let pool = pool
                    .as_ref()
                    .ok_or_else(|| LocalDbError::new("missing live controller pool"))?;
                client.set_live_pool(pool.clone());
                let metrics = metrics
                    .as_ref()
                    .ok_or_else(|| LocalDbError::new("missing live metrics"))?;
                client
                    .set_live_metrics(metrics.clone())
                    .map_err(LocalDbError::component)?;
                client
                    .configure_live_nvme(live)
                    .map_err(LocalDbError::component)?;
            }
            client
        };
        Ok(client)
    })?;
    Ok(Connections {
        clients,
        reused,
        elapsed_ns: elapsed(started),
    })
}

/// Starts bounded read-connection preparation while control recovery proceeds.
/// Drop joins even on a control/fencing/replay error, before the authority lock
/// can leave scope. These connections cannot issue reads before activation.
pub(super) struct ReadSetup(Option<JoinHandle<Result<Connections, LocalDbError>>>);

impl ReadSetup {
    pub(super) fn start(
        config: RemoteDatabaseConfig,
        shape: KeyShape,
        tls_config: super::super::tcp::ClientTlsConfig,
        retained: Option<Vec<SharedClient>>,
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))] pool: Option<
            Arc<super::super::nvmeof::live_pool::LiveControllerPool>,
        >,
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))] metrics: Option<
            Arc<super::super::nvmeof::live_metrics::LiveNvmeMetrics>,
        >,
    ) -> Result<Self, LocalDbError> {
        let thread = thread::Builder::new()
            .name("wal-read-setup".to_owned())
            .spawn(move || {
                connect(
                    &config,
                    &shape,
                    &tls_config,
                    config.read_lanes,
                    retained,
                    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
                    pool,
                    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
                    metrics,
                )
            })
            .map_err(LocalDbError::component)?;
        Ok(Self(Some(thread)))
    }

    pub(super) fn finish(mut self) -> Result<Connections, LocalDbError> {
        self.0
            .take()
            .unwrap()
            .join()
            .map_err(|_| LocalDbError::new("read transport setup panicked"))?
    }
}

impl Drop for ReadSetup {
    fn drop(&mut self) {
        if let Some(thread) = self.0.take() {
            let _ = thread.join();
        }
    }
}

#[cfg(test)]
mod tests;
