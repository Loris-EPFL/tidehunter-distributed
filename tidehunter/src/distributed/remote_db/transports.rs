//! Coordinator connection setup. Every open creates fresh owner sessions;
//! live queues and namespace grants are never reused across databases.
use super::*;

/// A failed open must not leave idle kernel controllers connected. The durable
/// connection journal retains uncertain ownership if the bounded drain fails.
pub(super) struct LivePoolOpenGuard {
    pool: Arc<super::super::nvmeof::live_pool::LiveControllerPool>,
    timeout: Duration,
    armed: bool,
}

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

pub(super) struct Connections {
    pub clients: Vec<Client>,
    pub elapsed_ns: u64,
}

pub(super) fn connect(
    config: &RemoteDatabaseConfig,
    shape: &KeyShape,
    tls_config: &super::super::tcp::ClientTlsConfig,
    lanes: usize,
    pool: Option<Arc<super::super::nvmeof::live_pool::LiveControllerPool>>,
    metrics: Option<Arc<super::super::nvmeof::live_metrics::LiveNvmeMetrics>>,
    #[cfg(any(test, feature = "test-utils"))] local_fixture: Option<bool>,
) -> Result<Connections, LocalDbError> {
    let started = Instant::now();
    let mut jobs: Vec<_> = config
        .endpoints
        .iter()
        .flat_map(|endpoint| (0..lanes).map(move |_| endpoint))
        .collect();
    let clients = endpoint_map(&mut jobs, |_, endpoint| {
        let mut tls = config.tls.clone();
        tls.server_name = endpoint.server_name.clone();
        let client = Client::connect_shared(
            &endpoint.address,
            &tls,
            tls_config.clone(),
            config.timeout,
            config.partition_config(1, endpoint.partition),
            shape,
        )
        .map_err(LocalDbError::component)?;
        if !client.connected_status().live_nvmeof {
            return Err(LocalDbError::new(
                "owner and coordinator data-plane modes differ",
            ));
        }
        let client = {
            let mut client = client;
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
            // Select the ordinary-file fixture before any live I/O, including
            // WAL replay during open.
            #[cfg(any(test, feature = "test-utils"))]
            if let Some(buffered) = local_fixture {
                client.bench_local_live_fixture(buffered);
            }
            client
                .configure_live_nvme(&config.live_nvme, config.effective_owner_append_occupancy())
                .map_err(LocalDbError::component)?;
            client
        };
        Ok(client)
    })?;
    Ok(Connections {
        clients,
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
        pool: Option<Arc<super::super::nvmeof::live_pool::LiveControllerPool>>,
        metrics: Option<Arc<super::super::nvmeof::live_metrics::LiveNvmeMetrics>>,
        #[cfg(any(test, feature = "test-utils"))] local_fixture: Option<bool>,
    ) -> Result<Self, LocalDbError> {
        let thread = thread::Builder::new()
            .name("wal-read-setup".to_owned())
            .spawn(move || {
                connect(
                    &config,
                    &shape,
                    &tls_config,
                    config.effective_read_lanes(),
                    pool,
                    metrics,
                    #[cfg(any(test, feature = "test-utils"))]
                    local_fixture,
                )
            })
            .map_err(LocalDbError::component)?;
        Ok(Self(Some(thread)))
    }

    #[allow(
        clippy::expect_used,
        reason = "`finish` consumes the setup once, so the thread is still owned"
    )]
    pub(super) fn finish(mut self) -> Result<Connections, LocalDbError> {
        self.0
            .take()
            .expect("setup thread owned until finish")
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
