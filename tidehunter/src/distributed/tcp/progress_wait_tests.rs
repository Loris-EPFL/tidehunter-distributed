//! Durable-progress lane readiness (design option A): the actual coordinator
//! client and owner session loops over loopback mTLS, instrumented by test-only
//! wait hooks, plus scripted CQ-arm fixtures. Every assertion follows an issued
//! event; the watchdog only detects a hang.
use super::progress_wait::{
    HookEvent, SPIN_LIMIT, ScriptStep, WAIT_UNAVAILABLE, WaitHook, WaitSet, WakeFd, Woke,
};
use super::tests::{batch, certificates, config, metrics};
use super::*;
use std::sync::mpsc;

/// The owner hub never ends a parked poll by time in these tests (except where a
/// test lowers it).
const LONG_CAP: Duration = Duration::from_secs(3600);
/// Only a hang reaches this bound.
const WATCHDOG: Duration = Duration::from_secs(120);
/// Data timeout of both ends; above the watchdog, so a hang is reported by it.
const IO_TIMEOUT: Duration = Duration::from_secs(150);

fn submit(
    node: &StorageNode,
    id: u64,
    request: Request,
    completed: &mpsc::SyncSender<reactor::WorkCompletion>,
) {
    let admitted = node.control.try_submit(control::ControlWork {
        id,
        request,
        limits: TcpLimits::default(),
        measured: false,
        queued: None,
        submitted: None,
        charge: 1,
        completed: completed.clone(),
        progress: None,
    });
    assert!(admitted.is_ok(), "control queue rejected request {id}");
}

fn receive(
    completions: &mpsc::Receiver<reactor::WorkCompletion>,
    id: u64,
) -> Result<Response, TcpError> {
    let completion = completions
        .recv_timeout(WATCHDOG)
        .expect("watchdog: control reply");
    assert_eq!(completion.id, id);
    completion.response
}

/// A separate control path: one fence of the owner, answered after the hub
/// failed (and therefore published and woke) every parked poll.
fn fence(node: &StorageNode, epoch: u64) {
    let (completed, completions) = mpsc::sync_channel(4);
    submit(node, u64::MAX - 1, Request::Fence { epoch }, &completed);
    assert!(matches!(
        receive(&completions, u64::MAX - 1),
        Ok(Response::Status(_))
    ));
}

fn parked_poll(after: u64) -> Request {
    Request::DurableProgress {
        authority_epoch: 1,
        epoch: 1,
        after,
        max_wait_ms: u32::MAX,
        sync: false,
    }
}

type Exchange = thread::JoinHandle<(Client, Result<Response, TcpError>)>;

fn park(mut client: Client, after: u64) -> Exchange {
    thread::spawn(move || {
        let response = client.request(parked_poll(after));
        (client, response)
    })
}

/// Collect hook events until `done` holds for everything collected so far.
fn until(
    events: &mpsc::Receiver<HookEvent>,
    mut done: impl FnMut(&[HookEvent]) -> bool,
) -> Vec<HookEvent> {
    let mut seen = Vec::new();
    while !done(seen.as_slice()) {
        seen.push(events.recv_timeout(WATCHDOG).expect("watchdog: hook event"));
    }
    seen
}

fn waiting(seen: &[HookEvent], entry: u64) -> Option<u64> {
    seen.iter().find_map(|event| match *event {
        HookEvent::Waiting { entry: at, id } if at == entry => Some(id),
        _ => None,
    })
}

fn parked(seen: &[HookEvent]) -> Option<u64> {
    seen.iter().find_map(|event| match *event {
        HookEvent::Parked(id) => Some(id),
        _ => None,
    })
}

fn drain(events: &mpsc::Receiver<HookEvent>, seen: &mut Vec<HookEvent>) {
    seen.extend(events.try_iter());
}

fn report(response: Result<Response, TcpError>) -> DurableProgressReport {
    match response {
        Ok(Response::DurableProgress(report)) => report,
        Ok(response) => panic!("expected DurableProgress, received {}", response.as_ref()),
        Err(error) => panic!("expected DurableProgress, received error {error}"),
    }
}

/// One instrumented progress lane: owner node, listener and owner hook.
struct Lane {
    _temp: tempdir::TempDir,
    config: NodeConfig,
    node: Arc<StorageNode>,
    tls: TlsFiles,
    endpoint: String,
    shutdown: Arc<AtomicBool>,
    server: Option<thread::JoinHandle<()>>,
    owner: Arc<WaitHook>,
    owner_events: mpsc::Receiver<HookEvent>,
    control: (
        mpsc::SyncSender<reactor::WorkCompletion>,
        mpsc::Receiver<reactor::WorkCompletion>,
    ),
    next_control: u64,
    next_sequence: u64,
}

struct ProgressClient {
    client: Client,
    stop: Arc<WakeFd>,
    hook: Arc<WaitHook>,
    events: mpsc::Receiver<HookEvent>,
}

impl Lane {
    /// A Native-tier owner with active stream 1 at a nonzero generation.
    fn start(name: &str) -> Self {
        Self::start_with(name, None)
    }

    /// `start` over `transport` instead of the mutual-TLS TCP default.
    fn start_with(name: &str, transport: Option<Transport>) -> Self {
        let temp = tempdir::TempDir::new(name).unwrap();
        let mut tls = certificates(temp.path());
        if let Some(transport) = transport {
            tls.transport = transport;
        }
        let config = config(&temp.path().join("node"));
        let node = StorageNode::open(config.clone(), metrics()).unwrap();
        node.set_durable_progress_cap_for_test(LONG_CAP);
        node.inventory.lock().fence_to(1).unwrap();
        let (owner, owner_events) = WaitHook::new();
        *node.progress_wait_hook.lock() = Some(owner.clone());
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        let shutdown = Arc::new(AtomicBool::new(false));
        let limits = TcpLimits {
            io_timeout: IO_TIMEOUT,
            max_connections: 64,
            ..TcpLimits::default()
        };
        let server = {
            let node = node.clone();
            let tls = tls.clone();
            let stop = shutdown.clone();
            thread::spawn(move || node.serve(listener, tls, limits, stop).unwrap())
        };
        let mut lane = Self {
            _temp: temp,
            config,
            node,
            tls,
            endpoint,
            shutdown,
            server: Some(server),
            owner,
            owner_events,
            control: mpsc::sync_channel(8),
            next_control: 1_000_000,
            next_sequence: 0,
        };
        lane.advance();
        lane
    }

    fn connect(&self) -> ProgressClient {
        let stop = Arc::new(WakeFd::new().unwrap());
        let shape = KeyShape::from_yaml(&self.config.shape_yaml).unwrap();
        let mut client = Client::connect_progress_shared(
            &self.endpoint,
            &self.tls,
            self.tls.client_config_with_identity().unwrap(),
            IO_TIMEOUT,
            self.config.partition.clone(),
            &shape,
            stop.clone(),
        )
        .unwrap();
        let (hook, events) = WaitHook::new();
        client.instrument_progress_wait(hook.clone());
        ProgressClient {
            client,
            stop,
            hook,
            events,
        }
    }

    fn control(&mut self, request: Request) -> Result<Response, TcpError> {
        self.next_control += 1;
        let id = self.next_control;
        submit(&self.node, id, request, &self.control.0);
        receive(&self.control.1, id)
    }

    /// One durable advance from a separate control path: append a batch, then
    /// nudge its sync. The generation grows by exactly one.
    fn advance(&mut self) {
        let sequence = self.next_sequence;
        self.next_sequence += 1;
        self.node
            .stream_log_for_test(1)
            .append_complete_batch(
                crate::distributed::types::RequestAuthority { epoch: 1 },
                &batch(&self.config, 1, sequence),
            )
            .unwrap();
        let nudge = Request::DurableProgress {
            authority_epoch: 1,
            epoch: 1,
            after: 0,
            max_wait_ms: u32::MAX,
            sync: true,
        };
        assert!(matches!(
            self.control(nudge),
            Ok(Response::DurableProgress(_))
        ));
    }

    fn generation(&self) -> u64 {
        self.node
            .durable_progress_for_test(1, 1)
            .unwrap()
            .generation
    }

    /// Stop the listener; joined under the watchdog so a session that ignores
    /// its shutdown wake fails the test instead of hanging it.
    fn stop_server(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        if let Some(server) = self.server.take() {
            let (joined, done) = mpsc::sync_channel(1);
            thread::spawn(move || {
                let _ = joined.send(server.join().is_ok());
            });
            assert!(
                done.recv_timeout(WATCHDOG)
                    .expect("watchdog: listener join")
            );
        }
    }
}

impl Drop for Lane {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        if let Some(server) = self.server.take() {
            let _ = server.join();
        }
    }
}

/// The two sides parked on request `id`: owner registered and waiting once,
/// client waiting once.
fn both_parked(
    lane: &Lane,
    client: &mpsc::Receiver<HookEvent>,
) -> (Vec<HookEvent>, Vec<HookEvent>, u64) {
    let owner = until(&lane.owner_events, |seen| {
        parked(seen).is_some() && waiting(seen, 1).is_some()
    });
    let side = until(client, |seen| waiting(seen, 1).is_some());
    let id = parked(&owner).unwrap();
    assert_eq!(waiting(&owner, 1), Some(id));
    assert_eq!(
        waiting(&side, 1),
        Some(id),
        "client parked on another request"
    );
    (owner, side, id)
}

#[test]
fn parked_lane_waits_once_per_side_without_repeated_empty_sweeps() {
    let mut lane = Lane::start("progress-wait-parked");
    let ProgressClient {
        client,
        hook,
        events,
        stop: _stop,
    } = lane.connect();
    let g = lane.generation();
    // Scripted readiness: each side blocks in its first wait until released.
    let owner_script = lane.owner.script();
    let client_script = hook.script();
    let exchange = park(client, g);
    let (mut owner_seen, mut client_seen, id) = both_parked(&lane, &events);
    // Parked phase: exactly one wait entry each, no repeated empty sweep, no
    // durable advance.
    assert_eq!(lane.owner.entries(), 1);
    assert_eq!(hook.entries(), 1);
    assert_eq!(lane.owner.repeated_empty_sweeps(), 0);
    assert_eq!(hook.repeated_empty_sweeps(), 0);
    assert_eq!(lane.generation(), g);

    // Real readiness from here on: one empty sweep each, then a real wait.
    owner_script
        .send(ScriptStep::Finish(Woke::Spurious))
        .unwrap();
    client_script
        .send(ScriptStep::Finish(Woke::Spurious))
        .unwrap();
    owner_seen.extend(until(&lane.owner_events, |seen| waiting(seen, 2).is_some()));
    client_seen.extend(until(&events, |seen| waiting(seen, 2).is_some()));
    assert_eq!(lane.generation(), g);

    // One publication from a separate control path. The eventfd wakes the owner,
    // the socket wakes the client, and the reply is the original request's.
    lane.advance();
    let (_progress, response) = exchange.join().unwrap();
    let report = report(response);
    assert_eq!(report.generation, g + 1);
    assert_eq!(
        (
            report.database,
            report.partition,
            report.authority_epoch,
            report.epoch
        ),
        (
            lane.config.partition.inventory.database,
            lane.config.partition.partition,
            1,
            1
        )
    );
    owner_seen.extend(until(&lane.owner_events, |seen| {
        seen.contains(&HookEvent::Completed(id))
    }));
    drain(&lane.owner_events, &mut owner_seen);
    drain(&events, &mut client_seen);
    assert!(owner_seen.contains(&HookEvent::Woke(Woke::Completion)));
    assert!(client_seen.contains(&HookEvent::Woke(Woke::Transport)));
    assert!(!owner_seen.contains(&HookEvent::Spin));
    assert!(!client_seen.contains(&HookEvent::Spin));
    assert_eq!(lane.owner.entries(), 2);
    assert_eq!(hook.entries(), 2);
    assert_eq!(lane.owner.repeated_empty_sweeps(), 0);
    assert_eq!(hook.repeated_empty_sweeps(), 0);
    assert!(!lane.owner.lost_events() && !hook.lost_events());
}

/// Negative control: the same endpoints and lane with only the readiness branch
/// disabled. Both loops reach the fixed empty-sweep count without a wait entry.
#[test]
fn active_loop_control_reaches_the_spin_limit() {
    let mut lane = Lane::start("progress-wait-active");
    let ProgressClient {
        client,
        hook,
        events,
        stop: _stop,
    } = lane.connect();
    let g = lane.generation();
    lane.owner.disable_readiness();
    hook.disable_readiness();
    let exchange = park(client, g);
    until(&lane.owner_events, |seen| seen.contains(&HookEvent::Spin));
    until(&events, |seen| seen.contains(&HookEvent::Spin));
    assert_eq!(lane.owner.entries(), 0);
    assert_eq!(hook.entries(), 0);
    assert!(lane.owner.repeated_empty_sweeps() >= SPIN_LIMIT - 1);
    assert!(hook.repeated_empty_sweeps() >= SPIN_LIMIT - 1);
    // The active loop still answers correctly.
    lane.advance();
    let (_client, response) = exchange.join().unwrap();
    assert_eq!(report(response).generation, g + 1);
}

/// A completion published after the owner's empty-inbox check and before its
/// `poll` (a fence answers the parked poll) leaves the eventfd readable: the
/// wait returns at once and the reply reaches the client.
#[test]
fn completion_before_owner_poll_is_not_lost() {
    let lane = Lane::start("progress-wait-owner-race");
    let ProgressClient {
        client,
        hook,
        events: _events,
        stop: _stop,
    } = lane.connect();
    let g = lane.generation();
    let node = lane.node.clone();
    lane.owner.inject_before_poll(move || fence(&node, 2));
    let exchange = park(client, g);
    let (_client, response) = exchange.join().unwrap();
    let error = response.err().expect("a fence ends the parked poll");
    assert!(error.to_string().contains("fence or seal"), "{error}");
    let mut owner_seen = until(&lane.owner_events, |seen| {
        seen.iter()
            .any(|event| matches!(event, HookEvent::Completed(_)))
    });
    drain(&lane.owner_events, &mut owner_seen);
    let woke = owner_seen
        .iter()
        .position(|event| *event == HookEvent::Woke(Woke::Completion));
    let completed = owner_seen
        .iter()
        .position(|event| matches!(event, HookEvent::Completed(_)));
    assert!(woke.is_some() && woke < completed, "{owner_seen:?}");
    assert_eq!(lane.owner.entries(), 1, "{owner_seen:?}");
    assert_eq!(lane.owner.repeated_empty_sweeps(), 0);
    assert_eq!(hook.repeated_empty_sweeps(), 0);
}

/// Reply bytes that arrive in the client's socket immediately before its
/// `poll` leave readiness pending: one wait entry, woken by transport.
#[test]
fn socket_data_before_client_poll_is_not_lost() {
    let lane = Lane::start("progress-wait-client-race");
    let ProgressClient {
        client,
        hook,
        events,
        stop: _stop,
    } = lane.connect();
    let g = lane.generation();
    let node = lane.node.clone();
    let socket = client.stream.sock.socket.try_clone().unwrap();
    hook.inject_before_poll(move || {
        fence(&node, 2);
        // Block (without consuming) until the owner's reply is in the socket.
        let mut descriptor = libc::pollfd {
            fd: std::os::fd::AsRawFd::as_raw_fd(&socket),
            events: libc::POLLIN,
            revents: 0,
        };
        let timeout = libc::c_int::try_from(WATCHDOG.as_millis()).unwrap();
        // SAFETY: one initialized stack pollfd for the call; `socket` keeps the fd open.
        let ready = unsafe { libc::poll(&raw mut descriptor, 1, timeout) };
        assert_eq!(ready, 1, "watchdog: the owner reply never arrived");
    });
    let exchange = park(client, g);
    let (_client, response) = exchange.join().unwrap();
    assert!(response.is_err());
    let mut seen = until(&events, |seen| {
        seen.iter().any(|event| matches!(event, HookEvent::Woke(_)))
    });
    drain(&events, &mut seen);
    assert_eq!(
        seen.iter()
            .find(|event| matches!(event, HookEvent::Woke(_))),
        Some(&HookEvent::Woke(Woke::Transport))
    );
    assert_eq!(hook.entries(), 1, "{seen:?}");
    assert_eq!(hook.repeated_empty_sweeps(), 0);
}

#[test]
fn fence_answers_a_parked_lane_through_the_completion_wake() {
    let lane = Lane::start("progress-wait-fence");
    let client = lane.connect();
    let g = lane.generation();
    let ProgressClient {
        client: progress,
        hook,
        events,
        stop: _stop,
    } = client;
    let exchange = park(progress, g);
    let owner = until(&lane.owner_events, |seen| {
        parked(seen).is_some() && waiting(seen, 1).is_some()
    });
    until(&events, |seen| waiting(seen, 1).is_some());
    fence(&lane.node, 2);
    let (_client, response) = exchange.join().unwrap();
    let error = response.err().expect("fence error");
    assert!(error.to_string().contains("fence or seal"), "{error}");
    let mut owner_seen = owner;
    owner_seen.extend(until(&lane.owner_events, |seen| {
        seen.contains(&HookEvent::Woke(Woke::Completion))
    }));
    assert_eq!(hook.repeated_empty_sweeps(), 0);
    assert_eq!(lane.owner.repeated_empty_sweeps(), 0);
}

#[test]
fn coordinator_stop_wake_ends_a_parked_client() {
    let lane = Lane::start("progress-wait-stop");
    let ProgressClient {
        client,
        stop,
        hook,
        events,
    } = lane.connect();
    let g = lane.generation();
    let exchange = park(client, g);
    until(&lane.owner_events, |seen| parked(seen).is_some());
    until(&events, |seen| waiting(seen, 1).is_some());
    stop.signal().unwrap();
    let (client, response) = exchange.join().unwrap();
    let error = response.err().expect("stop error");
    assert!(error.to_string().contains("stopped"), "{error}");
    assert!(!error.is_wait_unavailable());
    assert_eq!(
        client.session_present(),
        None,
        "a stopped lane is not reused"
    );
    until(&events, |seen| seen.contains(&HookEvent::Woke(Woke::Stop)));
    assert_eq!(hook.entries(), 1);
    assert_eq!(lane.generation(), g);
    // The owner's disconnected waiter keeps its hub slot until its terminal
    // outcome; the session joins it before exit. Release it for the teardown.
    fence(&lane.node, 2);
}

/// A parked client whose readiness wait fails (the capability is lost) ends the
/// exchange with the named error and drops the session. It never returns to an
/// active loop and never sees a durable advance.
#[test]
fn lost_client_wait_fails_closed_with_the_named_error() {
    let lane = Lane::start("progress-wait-client-lost");
    let ProgressClient {
        client,
        hook,
        events,
        stop: _stop,
    } = lane.connect();
    let g = lane.generation();
    let script = hook.script();
    let exchange = park(client, g);
    until(&lane.owner_events, |seen| parked(seen).is_some());
    let mut seen = until(&events, |seen| waiting(seen, 1).is_some());
    script
        .send(ScriptStep::Fail("poll: completion queue wait fd lost"))
        .unwrap();
    let (client, response) = exchange.join().unwrap();
    let error = response.err().expect("a lost wait ends the exchange");
    assert!(error.is_wait_unavailable(), "{error}");
    assert!(error.to_string().starts_with(WAIT_UNAVAILABLE), "{error}");
    assert_eq!(
        client.session_present(),
        None,
        "a lane that lost readiness is not reused"
    );
    drain(&events, &mut seen);
    assert!(!seen.contains(&HookEvent::Spin), "{seen:?}");
    assert_eq!(hook.entries(), 1);
    assert_eq!(hook.repeated_empty_sweeps(), 0);
    assert_eq!(lane.generation(), g);
    // The owner's disconnected waiter keeps its hub slot until its terminal
    // outcome; release it for the teardown.
    fence(&lane.node, 2);
}

/// A parked owner session whose readiness wait fails ends with the named
/// error in its close notice; the coordinator classifies the close as a route
/// that cannot wait (its poller stops instead of reconnecting).
#[test]
fn lost_owner_wait_fails_closed_and_the_client_classifies_the_close() {
    let lane = Lane::start("progress-wait-owner-lost");
    let ProgressClient {
        client,
        hook,
        events,
        stop: _stop,
    } = lane.connect();
    let g = lane.generation();
    let script = lane.owner.script();
    let exchange = park(client, g);
    let (mut owner_seen, _, _) = both_parked(&lane, &events);
    script
        .send(ScriptStep::Fail("poll: completion wake fd lost"))
        .unwrap();
    let (_client, response) = exchange.join().unwrap();
    let error = response.err().expect("the owner closes the lane");
    assert!(error.is_wait_unavailable(), "{error}");
    assert!(
        error
            .to_string()
            .starts_with(&format!("owner closed the session: {WAIT_UNAVAILABLE}")),
        "{error}"
    );
    drain(&lane.owner_events, &mut owner_seen);
    assert!(!owner_seen.contains(&HookEvent::Spin), "{owner_seen:?}");
    assert_eq!(lane.owner.entries(), 1);
    assert_eq!(lane.owner.repeated_empty_sweeps(), 0);
    assert_eq!(hook.repeated_empty_sweeps(), 0);
    assert_eq!(lane.generation(), g);
    // The closed session joins its accepted poll before it exits.
    fence(&lane.node, 2);
}

/// Owner shutdown wakes the parked owner session at once; the client's socket
/// becomes readable with the close notice, without a durable advance.
#[test]
fn owner_shutdown_wakes_the_session_and_the_client_sees_the_close() {
    let mut lane = Lane::start("progress-wait-shutdown");
    let ProgressClient {
        client,
        hook,
        events,
        stop: _stop,
    } = lane.connect();
    let g = lane.generation();
    let exchange = park(client, g);
    until(&lane.owner_events, |seen| {
        parked(seen).is_some() && waiting(seen, 1).is_some()
    });
    until(&events, |seen| waiting(seen, 1).is_some());
    // The flag first, as the listener does; the listener signals the wake.
    lane.shutdown.store(true, Ordering::Release);
    let (_client, response) = exchange.join().unwrap();
    let error = response.err().expect("owner close");
    assert!(
        error.to_string().contains("owner closed the session"),
        "{error}"
    );
    until(&lane.owner_events, |seen| {
        seen.contains(&HookEvent::Woke(Woke::Stop))
    });
    until(&events, |seen| {
        seen.contains(&HookEvent::Woke(Woke::Transport))
    });
    assert_eq!(hook.entries(), 1);
    assert_eq!(lane.generation(), g);
    // The closed session joins its accepted poll before it exits.
    fence(&lane.node, 2);
    lane.stop_server();
}

/// The hub's own poll deadline answers with the unchanged generation through
/// the completion wake; no extra timer runs in either loop.
#[test]
fn owner_deadline_replies_with_the_unchanged_generation() {
    let lane = Lane::start("progress-wait-deadline");
    let ProgressClient {
        client,
        hook,
        events: _events,
        stop: _stop,
    } = lane.connect();
    let g = lane.generation();
    lane.node
        .set_durable_progress_cap_for_test(Duration::from_millis(20));
    let (_client, response) = park(client, g).join().unwrap();
    let report = report(response);
    assert_eq!(report.generation, g);
    assert_eq!(lane.generation(), g);
    assert_eq!(hook.repeated_empty_sweeps(), 0);
    assert_eq!(lane.owner.repeated_empty_sweeps(), 0);
}

/// The hub's waiter bound: 16 lanes park, one more is answered at once with the
/// current report through its own session's completion wake.
#[test]
fn sixteen_parked_lanes_and_one_immediate_reply() {
    const LANES: usize = 17;
    let lane = Lane::start("progress-wait-sixteen");
    let clients: Vec<_> = (0..LANES).map(|_| lane.connect()).collect();
    let g = lane.generation();
    let (replied, replies) = mpsc::sync_channel(LANES);
    let exchanges: Vec<_> = clients
        .into_iter()
        .map(|ProgressClient { client, .. }| {
            let replied = replied.clone();
            thread::spawn(move || {
                let (client, response) = park(client, g).join().unwrap();
                let _ = replied.send(response.is_ok());
                (client, response)
            })
        })
        .collect();
    until(&lane.owner_events, |seen| {
        seen.iter()
            .filter(|event| matches!(event, HookEvent::Parked(_)))
            .count()
            == LANES - 1
    });
    // Exactly one lane replied, with the current report.
    assert!(
        replies
            .recv_timeout(WATCHDOG)
            .expect("watchdog: immediate reply")
    );
    assert!(replies.try_recv().is_err());
    fence(&lane.node, 2);
    let mut immediate = 0;
    let mut failed = 0;
    for exchange in exchanges {
        let (_client, response) = exchange.join().unwrap();
        match response {
            Ok(Response::DurableProgress(report)) => {
                assert_eq!(report.generation, g);
                immediate += 1;
            }
            Ok(response) => panic!("unexpected {}", response.as_ref()),
            Err(error) => {
                assert!(error.to_string().contains("fence or seal"), "{error}");
                failed += 1;
            }
        }
    }
    assert_eq!((immediate, failed), (1, LANES - 1));
}

/// Hardware qualification before a fleet comparison: the same lane over OFI
/// verbs with mutual TLS. Both ends park on the armed CQ wait fd, the owner's
/// hub completion wakes its session, the reply's CQ notification wakes the
/// client, and a local-only stop wake ends a parked client. A local TCP result
/// proves neither of these.
#[cfg(feature = "experimental-fabric")]
#[test]
#[ignore = "requires an RDMA device with the libfabric verbs provider"]
fn verbs_lane_parks_on_the_cq_wait_fd() {
    let mut lane = Lane::start_with(
        "progress-wait-verbs",
        Some(Transport::ofi(
            FabricBackend::Verbs,
            FabricSecurity::MutualTls,
        )),
    );
    let ProgressClient {
        client,
        stop,
        hook,
        events,
    } = lane.connect();
    let g = lane.generation();
    let owner_script = lane.owner.script();
    let client_script = hook.script();
    let exchange = park(client, g);
    let (_, _, id) = both_parked(&lane, &events);
    assert_eq!(lane.owner.entries(), 1);
    assert_eq!(hook.entries(), 1);
    assert_eq!(lane.owner.repeated_empty_sweeps(), 0);
    assert_eq!(lane.generation(), g);
    // Sweeps before the first client wait can wait for the request's local
    // send completion (conservative CQ quiescence); none follow a wait.
    let client_before = hook.repeated_empty_sweeps();
    owner_script
        .send(ScriptStep::Finish(Woke::Spurious))
        .unwrap();
    client_script
        .send(ScriptStep::Finish(Woke::Spurious))
        .unwrap();
    until(&lane.owner_events, |seen| waiting(seen, 2).is_some());
    until(&events, |seen| waiting(seen, 2).is_some());
    lane.advance();
    let (client, response) = exchange.join().unwrap();
    assert_eq!(report(response).generation, g + 1);
    let owner_seen = until(&lane.owner_events, |seen| {
        seen.contains(&HookEvent::Completed(id))
    });
    assert!(owner_seen.contains(&HookEvent::Woke(Woke::Completion)));
    assert_eq!(lane.owner.repeated_empty_sweeps(), 0);
    assert_eq!(hook.repeated_empty_sweeps(), client_before);
    let entries = hook.entries();
    let exchange = park(client, g + 1);
    until(&events, |seen| waiting(seen, entries + 1).is_some());
    stop.signal().unwrap();
    let (_client, response) = exchange.join().unwrap();
    let error = response.err().expect("stop error");
    assert!(error.to_string().contains("stopped"), "{error}");
    fence(&lane.node, 2);
}

/// An OFI route without a CQ wait fd (software RxM parks in `fi_cq_sread`):
/// the coordinator's progress lane fails at connect, before Status, and an
/// owner refuses a poll from an ordinary client with a correlated named error
/// and keeps the session open, so the refusal is not peer loss.
#[cfg(feature = "experimental-fabric")]
#[test]
fn route_without_a_cq_wait_fd_is_refused_with_a_named_error() {
    let lane = Lane::start_with(
        "progress-wait-software",
        Some(software_transport(FabricSecurity::MutualTls)),
    );
    let shape = KeyShape::from_yaml(&lane.config.shape_yaml).unwrap();
    let error = Client::connect_progress_shared(
        &lane.endpoint,
        &lane.tls,
        lane.tls.client_config_with_identity().unwrap(),
        IO_TIMEOUT,
        lane.config.partition.clone(),
        &shape,
        Arc::new(WakeFd::new().unwrap()),
    )
    .err()
    .expect("software RxM has no CQ wait fd");
    assert!(error.is_wait_unavailable(), "{error}");
    let mut client = Client::connect(
        &lane.endpoint,
        &lane.tls,
        IO_TIMEOUT,
        lane.config.partition.clone(),
        &shape,
    )
    .unwrap();
    let g = lane.generation();
    let error = client
        .request(parked_poll(g))
        .err()
        .expect("owner refuses the poll");
    assert!(error.is_wait_unavailable(), "{error}");
    assert!(error.to_string().starts_with(WAIT_UNAVAILABLE), "{error}");
    // The refusal kept the session: the next request on it succeeds.
    client.status().unwrap();
    assert_eq!(lane.owner.entries(), 0);
}

fn loopback_pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let (server, _) = listener.accept().unwrap();
    (client, server)
}

fn far() -> Instant {
    Instant::now() + WATCHDOG
}

/// Scripted CQ fixture: `fi_trywait` reports queued work (`-FI_EAGAIN`). The
/// caller progresses at once instead of blocking or sleeping.
#[test]
fn scripted_cq_with_queued_work_returns_to_progress() {
    let (liveness, _peer) = loopback_pair();
    let mut set = WaitSet::new(far());
    assert!(!set.armed_cq(Ok(None), &liveness).unwrap());
}

/// Scripted CQ fixture: a completion that arrives after `fi_trywait` armed the
/// wait fd and before `poll` is not lost.
#[test]
fn scripted_cqe_after_arm_before_poll_is_not_lost() {
    let (liveness, _peer) = loopback_pair();
    let cq = WakeFd::new().unwrap();
    let mut set = WaitSet::new(far());
    assert!(set.armed_cq(Ok(Some(cq.raw())), &liveness).unwrap());
    cq.signal().unwrap();
    assert_eq!(set.wait().unwrap(), Woke::Transport);
}

/// With the CQ armed, a local-only wake ends the wait, and stop wins.
#[test]
fn scripted_cq_local_wakes_and_stop_priority() {
    let (liveness, _peer) = loopback_pair();
    let cq = WakeFd::new().unwrap();
    let completion = WakeFd::new().unwrap();
    let stop = WakeFd::new().unwrap();
    let armed = |set: &mut WaitSet| {
        assert!(set.armed_cq(Ok(Some(cq.raw())), &liveness).unwrap());
        set.completion(&completion).unwrap();
        set.stop(&stop).unwrap();
    };
    let mut set = WaitSet::new(far());
    armed(&mut set);
    completion.signal().unwrap();
    assert_eq!(set.wait().unwrap(), Woke::Completion);
    assert!(completion.consume().unwrap());
    let mut set = WaitSet::new(far());
    armed(&mut set);
    completion.signal().unwrap();
    cq.signal().unwrap();
    stop.signal().unwrap();
    assert_eq!(set.wait().unwrap(), Woke::Stop);
}

/// A fatal arm result is a named transport error, never an active loop.
#[test]
fn failed_cq_arm_is_a_named_wait_error() {
    let (liveness, _peer) = loopback_pair();
    let mut set = WaitSet::new(far());
    let error = set
        .armed_cq(
            Err(io::Error::other("arm completion queue wait: EIO")),
            &liveness,
        )
        .unwrap_err();
    assert!(error.is_wait_unavailable());
    assert!(error.to_string().starts_with(WAIT_UNAVAILABLE), "{error}");
}

/// A failed eventfd allocation (EMFILE) of a lane wake (owner completion,
/// listener shutdown, poller stop) is the named wait error, so open and serve
/// fail closed instead of running without the wake.
#[test]
fn failed_wake_allocation_is_a_named_wait_error() {
    let created = Err(io::Error::from_raw_os_error(libc::EMFILE));
    let Err(error) = WakeFd::named(created, "poller stop wake") else {
        panic!("a failed allocation must be an error");
    };
    assert!(error.is_wait_unavailable(), "{error}");
    assert!(
        error
            .to_string()
            .starts_with(&format!("{WAIT_UNAVAILABLE}: poller stop wake eventfd: ")),
        "{error}"
    );
}

/// A descriptor that `poll` rejects ends a bounded wait at once with an error,
/// which the lanes report as the named wait error; it never times out or spins.
#[test]
fn invalid_wait_descriptor_fails_the_wait_at_once() {
    let (liveness, _peer) = loopback_pair();
    let mut set = WaitSet::new(far());
    // No process has this descriptor open, so poll reports POLLNVAL.
    assert!(set.armed_cq(Ok(Some(libc::c_int::MAX)), &liveness).unwrap());
    let error = set.wait().unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput, "{error}");
    assert!(TcpError::wait_unavailable(error).is_wait_unavailable());
}

#[test]
fn deadline_ends_one_wait_and_peer_close_is_transport_readiness() {
    let (socket, peer) = loopback_pair();
    let mut set = WaitSet::new(Instant::now() + Duration::from_millis(2));
    set.socket(&socket).unwrap();
    // The timeout rounds up: one entry ends at or past the deadline.
    assert_eq!(set.wait().unwrap(), Woke::Deadline);
    let mut set = WaitSet::new(Instant::now());
    set.socket(&socket).unwrap();
    assert_eq!(set.wait().unwrap(), Woke::Deadline);
    drop(peer);
    let mut set = WaitSet::new(far());
    set.socket(&socket).unwrap();
    assert_eq!(set.wait().unwrap(), Woke::Transport);
}

#[test]
fn wake_fd_coalesces_and_close_notices_classify_the_route() {
    let wake = WakeFd::new().unwrap();
    assert!(!wake.consume().unwrap());
    wake.signal().unwrap();
    wake.signal().unwrap();
    assert!(wake.consume().unwrap());
    assert!(!wake.consume().unwrap());
    let notice = format!("{WAIT_UNAVAILABLE}: provider verbs has no completion queue wait fd");
    assert!(TcpError::owner_closed(&notice).is_wait_unavailable());
    assert!(!TcpError::owner_closed("owner node is shutting down").is_wait_unavailable());
    assert!(!TcpError::new("peer closed the connection").is_wait_unavailable());
}
