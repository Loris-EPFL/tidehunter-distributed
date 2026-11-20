use super::*;
#[test]
fn client_profile_is_bounded_and_preserves_absence() {
    let backend = ValkeyRdmaBackend {
        nodes: Vec::new(),
        slots: Vec::new(),
        cluster: false,
        timeout: Duration::from_secs(1),
        rdma: false,
        profile: ClientProfile::default(),
        info: BackendInfo {
            name: String::new(),
            server_version: String::new(),
            client_version: String::new(),
            acknowledgement: String::new(),
            consistency: String::new(),
            replication_factor: 1,
            measurement_boundary: String::new(),
            get_many_strategy: String::new(),
            batch_atomicity: BatchAtomicity::Unsupported,
            topology: json!({}),
            configuration: json!({}),
        },
    };
    assert!(backend.arm_client_profile(65_537).is_err());
    backend.arm_client_profile(1).unwrap();
    let first = backend.begin_client_sample(Some(42), "get", 0, 1).unwrap();
    update_sample(&first, |s| s.mutex_wait_ns = Some(0));
    assert!(backend.begin_client_sample(Some(43), "put", 0, 0).is_none());
    let result = backend.finish_client_profile(Instant::now()).unwrap();
    assert_eq!(result["records"].as_array().unwrap().len(), 1);
    assert_eq!(result["records"][0]["caller_sample_id"], 42);
    assert_eq!(result["records"][0]["mutex_wait_ns"], 0);
    assert!(result["records"][0]["cq"].is_null());
    assert_eq!(result["cq_available"], false);
    assert!(backend.begin_client_sample(Some(44), "get", 0, 0).is_none());
}
#[test]
fn disconnected_tcp_pipeline_stays_poisoned() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let peer = std::thread::spawn(move || {
        let (socket, _) = listener.accept().unwrap();
        drop(socket);
    });
    let mut connection = Connection::connect(&address.to_string(), 1000, false).unwrap();
    peer.join().unwrap();
    let command = vec![b"GET".to_vec(), b"key".to_vec()];
    let deadline = Instant::now() + Duration::from_secs(1);
    assert!(connection.command(&command, deadline).is_err());
    assert!(connection.poisoned);
    let error = connection.command(&command, deadline).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("unusable after incomplete pipeline")
    );
}
#[test]
fn replacement_attempts_use_the_remaining_request_time_not_a_full_timeout() {
    let far = Instant::now() + Duration::from_secs(60);
    assert_eq!(attempt_budget_ms(1000, far), Some(1000));
    let near = attempt_budget_ms(1000, Instant::now() + Duration::from_millis(40)).unwrap();
    assert!((1..=41).contains(&near), "{near}");
    assert_eq!(attempt_budget_ms(1000, Instant::now()), None);
    assert_eq!(
        attempt_budget_ms(
            1000,
            Instant::now()
                .checked_sub(Duration::from_millis(1))
                .unwrap()
        ),
        None
    );
}

#[test]
fn profile_sampling_stops_exactly_at_finish_under_concurrent_samplers() {
    let backend = Arc::new(ValkeyRdmaBackend {
        nodes: Vec::new(),
        slots: Vec::new(),
        cluster: false,
        timeout: Duration::from_secs(1),
        rdma: false,
        profile: ClientProfile::default(),
        info: BackendInfo {
            name: String::new(),
            server_version: String::new(),
            client_version: String::new(),
            acknowledgement: String::new(),
            consistency: String::new(),
            replication_factor: 1,
            measurement_boundary: String::new(),
            get_many_strategy: String::new(),
            batch_atomicity: BatchAtomicity::Unsupported,
            topology: json!({}),
            configuration: json!({}),
        },
    });
    for round in 0..50 {
        backend.arm_client_profile(3).unwrap();
        let samplers: Vec<_> = (0..4)
            .map(|thread| {
                let backend = Arc::clone(&backend);
                std::thread::spawn(move || {
                    for id in 0..200 {
                        let _ = backend.begin_client_sample(Some(thread * 1000 + id), "get", 0, 0);
                    }
                })
            })
            .collect();
        let finished = backend.finish_client_profile(Instant::now()).unwrap();
        for sampler in samplers {
            sampler.join().unwrap();
        }
        let recorded = finished["records"].as_array().unwrap().len();
        assert!(
            recorded <= 3,
            "round {round}: {recorded} records over the limit"
        );
        // The counter must stay at zero (a decrement of zero would wrap to usize::MAX) and
        // nothing may be added after the snapshot.
        assert_eq!(
            backend.profile.remaining.load(Ordering::Acquire),
            0,
            "round {round}"
        );
        assert_eq!(
            backend.profile.samples.lock().unwrap().len(),
            recorded,
            "round {round}"
        );
    }
}

fn tcp_ctx(endpoint: &str) -> NodeCtx {
    NodeCtx {
        endpoint: endpoint.to_owned(),
        timeout_ms: 1000,
        rdma: false,
        health: PoolHealth::default(),
    }
}

fn count(counter: &AtomicU64) -> u64 {
    counter.load(Ordering::Relaxed)
}

#[test]
fn request_stage_round_trips_through_progress() {
    let progress = RequestProgress::new();
    assert_eq!(progress.stage(), RequestStage::ConnectionLock);
    for stage in RequestStage::ALL {
        progress.enter(stage);
        assert_eq!(progress.stage(), stage);
    }
}

#[test]
fn scalar_timeout_reports_location_and_stage_and_mutation_uncertainty_only_for_mutations() {
    let after = Duration::from_secs(5);
    let progress = RequestProgress::new();
    progress.assign(2);
    progress.enter(RequestStage::Reply);
    let get = scalar_timeout_error(
        "RDMA",
        ScalarCommand::Get,
        after,
        3,
        "10.0.0.4:6379",
        &progress,
    );
    assert_eq!(get.kind(), io::ErrorKind::TimedOut);
    assert_eq!(
        get.to_string(),
        "Valkey RDMA GET timed out after 5000 ms: owner 3 (10.0.0.4:6379), connection 2, \
             stage reply; read-only command; no mutation"
    );
    for command in [ScalarCommand::Set, ScalarCommand::Del] {
        let error = scalar_timeout_error("RDMA", command, after, 3, "10.0.0.4:6379", &progress);
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert_eq!(
            error.to_string(),
            format!(
                "Valkey RDMA {} timed out after 5000 ms: owner 3 (10.0.0.4:6379), \
                     connection 2, stage reply; mutation outcome may be unknown",
                command.name()
            )
        );
    }
    // A mutation still waiting for its connection lock never had a worker to send it.
    let waiting = RequestProgress::new();
    assert_eq!(
        scalar_timeout_error(
            "TCP",
            ScalarCommand::Set,
            after,
            0,
            "127.0.0.1:6379",
            &waiting
        )
        .to_string(),
        "Valkey TCP SET timed out after 5000 ms: owner 0 (127.0.0.1:6379), connection \
             unassigned, stage connection_lock; mutation not sent: the request never acquired \
             its connection"
    );
    for stage in RequestStage::ALL {
        assert_eq!(
            timeout_outcome(ScalarCommand::Get, stage),
            READ_ONLY_OUTCOME
        );
        for command in [ScalarCommand::Set, ScalarCommand::Del] {
            let unknown = timeout_outcome(command, stage) == "mutation outcome may be unknown";
            assert_eq!(unknown, stage != RequestStage::ConnectionLock, "{stage:?}");
        }
    }
}

#[test]
fn mget_timeout_reports_each_owner_and_no_mutation() {
    let finished = RequestProgress::new();
    finished.assign(1);
    finished.enter(RequestStage::Completed);
    let sending = RequestProgress::new();
    sending.assign(3);
    sending.enter(RequestStage::Send);
    let error = mget_timeout_error(
        "RDMA",
        Duration::from_secs(5),
        9,
        [
            MgetOwner {
                owner: 0,
                endpoint: "10.0.0.1:6379",
                commands: 2,
                keys: 4,
                progress: &finished,
            },
            MgetOwner {
                owner: 4,
                endpoint: "10.0.0.5:6379",
                commands: 3,
                keys: 5,
                progress: &sending,
            },
        ],
    );
    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    assert_eq!(
        error.to_string(),
        "Valkey RDMA MGET timed out after 5000 ms: 9 keys, 5 slot commands; owner 0 \
             (10.0.0.1:6379), connection 1, stage completed, 2 commands, 4 keys; owner 4 \
             (10.0.0.5:6379), connection 3, stage send, 3 commands, 5 keys; read-only command; \
             no mutation"
    );
}

#[test]
fn connection_records_send_and_reply_stages_but_not_after_expiry() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let peer = std::thread::spawn(move || drop(listener.accept().unwrap().0));
    let mut connection = Connection::connect(&address, 1000, false).unwrap();
    peer.join().unwrap();
    let command = vec![b"GET".to_vec(), b"key".to_vec()];
    let progress = RequestProgress::new();
    progress.enter(RequestStage::ConnectionCheck);
    let expired = Instant::now()
        .checked_sub(Duration::from_millis(1))
        .unwrap();
    assert!(
        connection
            .pipeline(&[&command], expired, None, Some(&progress))
            .is_err()
    );
    assert_eq!(progress.stage(), RequestStage::ConnectionCheck);
    // The peer has closed, so the reply read fails at once; the distant deadline is never
    // reached and no timing decides the stage. Appending only buffers the command.
    let deadline = Instant::now() + Duration::from_secs(5);
    assert!(
        connection
            .pipeline(&[&command], deadline, None, Some(&progress))
            .is_err()
    );
    assert_eq!(progress.stage(), RequestStage::Reply);
}

#[test]
fn worker_timeouts_keep_their_error_and_gain_the_request_context() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let backend = tcp_backend(&address, Duration::from_millis(50));
    let expired = Instant::now()
        .checked_sub(Duration::from_millis(1))
        .unwrap();
    let args = ScalarCommand::Get.args(&[&b"secret-key"[..]]);
    // The real worker path: a worker that finds the deadline expired fails before sending.
    let worker_timeout = |progress: &RequestProgress| {
        let mut connection = backend.nodes[0].connections[1].try_lock().unwrap();
        backend.nodes[0]
            .ctx
            .tracked_pipeline(
                &mut connection,
                &[&args],
                expired,
                None,
                None,
                None,
                Some(progress),
            )
            .unwrap_err()
    };
    let progress = RequestProgress::new();
    progress.assign(1);
    let worker_text = "Valkey pipeline timed out";
    for (command, outcome) in [
        (ScalarCommand::Get, READ_ONLY_OUTCOME),
        (ScalarCommand::Set, "mutation outcome may be unknown"),
        (ScalarCommand::Del, "mutation outcome may be unknown"),
    ] {
        let error = backend.scalar_worker_timeout(0, command, &progress, worker_timeout(&progress));
        // A failed worker keeps the stage it failed in.
        assert_eq!(progress.stage(), RequestStage::ConnectionCheck);
        assert!(is_timed_out(error.as_ref()), "{error}");
        assert_eq!(
            error.to_string(),
            format!(
                "Valkey TCP {} worker timed out (request deadline 50 ms): owner 0 \
                     ({address}), connection 1, stage connection_check; {outcome}; worker \
                     error: {worker_text}",
                command.name()
            )
        );
        assert_eq!(error.source().unwrap().to_string(), worker_text);
    }
    let mget = with_worker_timeout_context(worker_timeout(&progress), || {
        mget_worker_context(
            "TCP",
            backend.timeout,
            &MgetOwner {
                owner: 0,
                endpoint: &address,
                commands: 1,
                keys: 2,
                progress: &progress,
            },
        )
    });
    assert!(is_timed_out(mget.as_ref()), "{mget}");
    assert_eq!(
        mget.to_string(),
        format!(
            "Valkey TCP MGET worker timed out (request deadline 50 ms): owner 0 ({address}), \
                 connection 1, stage connection_check, 1 commands, 2 keys; read-only command; no \
                 mutation; worker error: {worker_text}"
        )
    );
    assert_eq!(mget.source().unwrap().to_string(), worker_text);
    assert!(!mget.to_string().contains("secret"), "{mget}");
    // Errors that are not timeouts pass through untouched.
    let other = with_worker_timeout_context(invalid("missing libvalkey reply"), || {
        panic!("context built for a non-timeout error")
    });
    assert!(!is_timed_out(other.as_ref()));
    assert_eq!(other.to_string(), "missing libvalkey reply");
    drop(listener);
}

fn tcp_backend(address: &str, timeout: Duration) -> ValkeyRdmaBackend {
    let connections = (0..POOL_SIZE)
        .map(|_| {
            Arc::new(Mutex::new(
                Connection::connect(address, 1000, false).unwrap(),
            ))
        })
        .collect();
    ValkeyRdmaBackend {
        nodes: vec![Node {
            connections,
            next: AtomicUsize::new(0),
            ctx: Arc::new(tcp_ctx(address)),
        }],
        slots: vec![0; 16384],
        cluster: false,
        timeout,
        rdma: false,
        profile: ClientProfile::default(),
        info: BackendInfo {
            name: String::new(),
            server_version: String::new(),
            client_version: String::new(),
            acknowledgement: String::new(),
            consistency: String::new(),
            replication_factor: 1,
            measurement_boundary: String::new(),
            get_many_strategy: String::new(),
            batch_atomicity: BatchAtomicity::Unsupported,
            topology: json!({}),
            configuration: json!({}),
        },
    }
}

#[tokio::test]
async fn timeouts_waiting_for_a_connection_name_the_request_but_not_its_payload() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let backend = tcp_backend(&address, Duration::from_millis(50));
    // Holding every pooled connection makes each request time out waiting for its lock,
    // whatever the timer granularity: no worker can start.
    let mut held = Vec::new();
    for connection in &backend.nodes[0].connections {
        held.push(connection.lock().await);
    }
    let key = Bytes::from(b"secret-key".to_vec());
    let value = Bytes::from(b"secret-value".to_vec());
    let location = |connection: usize| {
        format!("owner 0 ({address}), connection {connection}, stage connection_lock")
    };
    let not_sent = "mutation not sent: the request never acquired its connection";

    // Connections are taken round robin from 0.
    let get = backend.get(&key).await.unwrap_err();
    let put = backend.put(key.clone(), value.clone()).await.unwrap_err();
    let delete = backend.delete(key.clone()).await.unwrap_err();
    let traced = backend
        .traced_put(key.clone(), value.clone(), 7)
        .await
        .unwrap_err();
    let many = backend
        .get_many(&[key.clone(), Bytes::from(b"secret-other".to_vec())])
        .await
        .unwrap_err();
    let expected = [
        (
            &get,
            format!(
                "Valkey TCP GET timed out after 50 ms: {}; read-only command; no mutation",
                location(0)
            ),
        ),
        (
            &put,
            format!(
                "Valkey TCP SET timed out after 50 ms: {}; {not_sent}",
                location(1)
            ),
        ),
        (
            &delete,
            format!(
                "Valkey TCP DEL timed out after 50 ms: {}; {not_sent}",
                location(2)
            ),
        ),
        (
            &traced,
            format!(
                "Valkey TCP SET timed out after 50 ms: {}; {not_sent}",
                location(3)
            ),
        ),
        (
            &many,
            format!(
                "Valkey TCP MGET timed out after 50 ms: 2 keys, 1 slot commands; {}, \
                     1 commands, 2 keys; read-only command; no mutation",
                location(0)
            ),
        ),
    ];
    for (error, message) in expected {
        assert!(is_timed_out(error.as_ref()), "{error}");
        assert_eq!(error.to_string(), message);
        assert!(!message.contains("secret"), "{message}");
    }
    drop(held);
    drop(listener);
}

#[test]
fn request_expired_before_any_byte_is_sent_does_not_poison_the_connection() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let ctx = tcp_ctx(&address);
    // The kernel completes the handshake from the backlog; the peer never has to accept.
    let mut connection = Connection::connect(&address, 1000, false).unwrap();
    let command = vec![b"GET".to_vec(), b"key".to_vec()];
    let expired = Instant::now()
        .checked_sub(Duration::from_millis(1))
        .unwrap();
    let error = ctx
        .pipeline(&mut connection, &[&command], expired, None, None, None)
        .unwrap_err();
    assert!(is_timed_out(error.as_ref()));
    assert!(!connection.poisoned);
    assert!(!connection.is_broken());
    assert_eq!(count(&ctx.health.timeouts_before_send), 1);
    assert_eq!(count(&ctx.health.connections_poisoned), 0);
    assert_eq!(count(&ctx.health.connections_replaced), 0);
}

#[test]
fn poisoned_connection_is_replaced_before_its_next_request() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let ctx = tcp_ctx(&address);
    let first = listener.try_clone().unwrap();
    let peer = std::thread::spawn(move || drop(first.accept().unwrap().0));
    let mut connection = Connection::connect(&address, 1000, false).unwrap();
    peer.join().unwrap();
    let command = vec![b"GET".to_vec(), b"key".to_vec()];
    let deadline = Instant::now() + Duration::from_secs(1);
    assert!(
        ctx.pipeline(&mut connection, &[&command], deadline, None, None, None)
            .is_err()
    );
    assert!(connection.poisoned);
    assert_eq!(count(&ctx.health.connections_poisoned), 1);
    // The replacement handshake is again served by the backlog; no byte is exchanged.
    ctx.repair(&mut connection, deadline).unwrap();
    assert!(!connection.poisoned);
    assert!(!connection.is_broken());
    assert_eq!(count(&ctx.health.connections_replaced), 1);
}

#[test]
fn failed_replacement_is_bounded_counted_and_keeps_the_slot_broken() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let ctx = tcp_ctx(&address);
    let mut connection = Connection::connect(&address, 1000, false).unwrap();
    connection.poisoned = true;
    // Nothing listens any more, so every connect attempt is refused.
    drop(listener);
    let deadline = Instant::now() + Duration::from_secs(5);
    assert!(ctx.repair(&mut connection, deadline).is_err());
    assert_eq!(
        count(&ctx.health.replacement_attempts_failed),
        REPLACEMENT_ATTEMPTS as u64
    );
    assert_eq!(count(&ctx.health.connections_replaced), 0);
    assert!(connection.is_broken());
}

#[test]
fn hash_tags_and_binary() {
    assert_eq!(slot(b"123456789"), 12739);
    assert_eq!(slot(b"a{same}x"), slot(b"b{same}y"));
    assert_ne!(slot(b"a{}"), slot(b"b{}"));
    assert_eq!(slot(b"a{\0x}y"), slot(b"{\0x}"));
}
#[test]
fn static_topology_rejects_gaps_duplicates_and_migration() {
    let ids = HashMap::from([("a".to_string(), 0), ("b".to_string(), 1)]);
    let good = "a 10.0.0.1:6379@16379 master - 0 0 1 connected 0-8191\nb 10.0.0.2:6379@16379 master - 0 0 2 connected 8192-16383";
    let slots = topology(good, &ids).unwrap();
    assert_eq!(slots[8191], 0);
    assert_eq!(slots[8192], 1);
    assert!(topology(&good.replace("8192-16383", "8193-16383"), &ids).is_err());
    assert!(topology(&good.replace("8192-16383", "8191-16383"), &ids).is_err());
    assert!(topology(&good.replace("8192-16383", "8192-16383 [1->-b]"), &ids).is_err());
}
#[test]
fn options_require_numeric_distinct_endpoints_and_rf1() {
    let mut v = json!({"nodes":["127.0.0.1:16379"],"cluster":false,"required_replication_factor":1,"required_primary_nodes":1,"request_timeout_ms":1000});
    assert!(validate_options(&v).is_ok());
    v["nodes"] = json!(["localhost:16379"]);
    assert!(validate_options(&v).is_err());
    v["nodes"] = json!(["127.0.0.1:16379"]);
    v["required_replication_factor"] = json!(2);
    assert!(validate_options(&v).is_err());
}
fn live_config(fsync: &str) -> Response {
    Response::Array(
        [
            "appendonly",
            "yes",
            "appendfsync",
            fsync,
            "no-appendfsync-on-rewrite",
            "no",
            "maxmemory-policy",
            "noeviction",
        ]
        .into_iter()
        .map(|value| Response::Bytes(value.as_bytes().to_vec()))
        .collect(),
    )
}

#[test]
fn contracts_parse_for_rdma_and_paired_tcp_and_check_live_settings() {
    let mut input = json!({"nodes":["127.0.0.1:16379"],"cluster":false,
            "required_replication_factor":1,"required_primary_nodes":1,"request_timeout_ms":1000});
    assert_eq!(
        Options::parse(&input).unwrap().ack_contract,
        AckContract::Synced
    );
    assert!(config(live_config("everysec"), AckContract::default()).is_err());
    for (contract, fsync) in [
        (AckContract::Synced, "always"),
        (AckContract::OsBuffered, "no"),
        (AckContract::PeriodicSync, "everysec"),
    ] {
        input["ack_contract"] = json!(contract);
        assert!(validate_options(&input).is_ok());
        input["client"] = json!("libvalkey");
        assert!(validate_tcp_options(&input).is_ok());
        input.as_object_mut().unwrap().remove("client");
        for actual in ["always", "no", "everysec"] {
            let result = config(live_config(actual), contract);
            assert_eq!(result.is_ok(), actual == fsync);
            if let Err(error) = result {
                assert!(error.to_string().contains(contract.as_str()));
            }
        }
        for index in [1, 5, 7] {
            let Response::Array(mut fields) = live_config(fsync) else {
                unreachable!()
            };
            fields[index] = Response::Bytes(b"invalid".to_vec());
            assert!(config(Response::Array(fields), contract).is_err());
        }
    }
    for invalid_contract in ["page_cache", "invalid"] {
        input["ack_contract"] = json!(invalid_contract);
        assert!(validate_options(&input).is_err());
        input["client"] = json!("libvalkey");
        assert!(validate_tcp_options(&input).is_err());
        input.as_object_mut().unwrap().remove("client");
    }
}
