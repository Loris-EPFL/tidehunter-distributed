/// Auxiliary primitive-cost check, not a store-throughput benchmark. All five
/// successful-call counter updates and three labelled histogram observations
/// match offered()/record(); raw output and private accumulators are excluded.
#[test]
#[ignore = "explicit local release-mode accounting measurement"]
fn client_metric_handle_cost() {
    for clients in [1, 2, 4] {
        for shared in [false, true] {
            let guard =
                tempfile::TempDir::with_prefix(format!("kv-client-metrics-{clients}-{shared}-"))
                    .expect("test tree");
            let root = guard.path().to_path_buf();
            let mut parents = (0..if shared { 1 } else { clients })
                .map(|index| {
                    Reporter::create(
                        &root.join(index.to_string()),
                        Registry::new(),
                        "test".into(),
                    )
                    .unwrap()
                })
                .collect::<Vec<_>>();
            let clients = (0..clients)
                .map(|index| parents[if shared { 0 } else { index }].fork_client())
                .collect::<Vec<_>>();
            let count = clients.len();
            let barrier = std::sync::Barrier::new(count + 1);
            let elapsed = std::thread::scope(|scope| {
                let mut threads = Vec::with_capacity(count);
                for mut client in clients {
                    let barrier = &barrier;
                    threads.push(scope.spawn(move || {
                        barrier.wait();
                        for _ in 0..200_000 / count {
                            client.offered("measurement", "read", 1);
                            let labels = ["test", "read", "success", "measurement"];
                            client.completed.with_label_values(&labels).inc();
                            client.succeeded.with_label_values(&labels).inc();
                            client.keys_completed.with_label_values(&labels).inc();
                            client
                                .response_latency
                                .with_label_values(&labels)
                                .observe(0.000_002);
                            client
                                .api_latency
                                .with_label_values(&labels)
                                .observe(0.000_001);
                            client
                                .scheduling_delay
                                .with_label_values(&labels)
                                .observe(0.000_001);
                        }
                    }));
                }
                let started = Instant::now();
                barrier.wait();
                for thread in threads {
                    thread.join().unwrap();
                }
                started.elapsed()
            });
            println!(
                "client_metric_handle_cost clients={count} shared={shared} aggregate_ns_per_call={:.3}",
                elapsed.as_nanos() as f64 / 200_000.0
            );
            for parent in &mut parents {
                parent.samples.take();
                parent
                    .sample_writer
                    .take()
                    .unwrap()
                    .join()
                    .unwrap()
                    .unwrap();
            }
        }
    }
}

#[tokio::test]
async fn client_reports_merge_counts_distributions_and_all_raw_rows_once() {
    let guard = tempfile::TempDir::with_prefix("kv-research-client-report-").expect("test tree");
    let path = guard.path().to_path_buf();
    let mut parent = Reporter::create(&path, Registry::new(), "test".into()).unwrap();
    for (index, latency) in [3, 255].into_iter().enumerate() {
        let mut client = parent.fork_client();
        let WriterMessage::Row(mut sample) = row("success") else {
            unreachable!()
        };
        sample.api_ns = latency;
        sample.response_ns = latency + 1;
        client.offered("measurement", "read", 1);
        client.record(sample).await.unwrap();
        assert_eq!(parent.totals().0, index as u64);
        parent.merge_client(client);
    }
    assert_eq!(parent.totals().0, 2);
    assert_eq!(parent.totals().1, 2);
    assert_eq!(parent.totals().6, 2);
    let summary = parent.distribution_summaries();
    assert_eq!(summary["measurement.read.api.success"]["count"], 2);
    assert_eq!(summary["measurement.read.api.success"]["min_ns"], 3);
    assert_eq!(summary["measurement.read.api.success"]["max_ns"], 255);
    assert_eq!(parent.realized_mix()["measurement.read"]["offered"], 2);
    assert_eq!(parent.realized_mix()["measurement.read"]["fraction"], 1.0);
    let receiver = parent.stall_sample_writer_for_test();
    assert_eq!(
        std::fs::read_to_string(path.join("samples.csv.tmp"))
            .unwrap()
            .lines()
            .count(),
        3
    );
    drop(receiver);
    drop(parent);
}

use super::*;

#[test]
fn latency_summary_is_bounded_and_uses_bucket_upper_bounds() {
    let mut summary = LatencyAccumulator::default();
    for value in [100, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10] {
        summary.observe(value);
    }
    let summary = summary.summary();
    assert_eq!(summary.count, 11);
    assert_eq!(summary.p50_upper_bound_ns, Some(7));
    assert_eq!(summary.max_ns, Some(100));
    let mut exact = LatencyAccumulator::default();
    exact.observe(3);
    assert_eq!(exact.quantile_upper_bound(99, 100), Some(3));
}

#[tokio::test]
async fn stream_tails_and_outcomes_are_not_pooled_and_rejections_have_no_api_tail() {
    let guard = tempfile::TempDir::with_prefix("kv-research-tail-").expect("test tree");
    let path = guard.path().to_path_buf();
    let mut reporter = Reporter::create(&path, Registry::new(), "test".into()).unwrap();
    for (stream, operation, outcome, latency) in [
        (Stream::Read, "read", "success", 3),
        (Stream::Put, "put", "success", 255),
        (Stream::Read, "read", "timeout", 1023),
    ] {
        let WriterMessage::Row(mut sample) = row(outcome) else {
            unreachable!()
        };
        sample.stream = Some(stream);
        sample.operation = operation;
        sample.api_ns = latency;
        sample.completed_ns = sample.dispatched_ns + latency;
        sample.response_ns = sample.completed_ns;
        sample.released_ns = sample.completed_ns;
        reporter.offered("measurement", operation, 1);
        reporter.record(sample).await.unwrap();
    }
    let WriterMessage::Row(mut rejected) = row("rejected") else {
        unreachable!()
    };
    rejected.stream = Some(Stream::Read);
    rejected.dispatch_observed = false;
    rejected.dispatched_ns = 0;
    rejected.api_ns = 0;
    rejected.scheduling_ns = 0;
    reporter.offered("measurement", "read", 1);
    reporter.rejected(rejected).await.unwrap();
    let summary = reporter.distribution_summaries();
    for (key, expected) in [
        ("measurement.read.read.api.success", 3),
        ("measurement.put.put.api.success", 255),
        ("measurement.read.read.api.timeout", 1023),
    ] {
        assert_eq!(summary[key]["count"], 1);
        assert_eq!(summary[key]["p99_upper_bound_ns"], expected);
        assert_eq!(summary[key]["p999_upper_bound_ns"], expected);
    }
    assert!(summary.get("measurement.read.read.api.rejected").is_none());
    assert!(summary.get("measurement.read.api.success").is_none());
    // Finish the writer here so no queue or thread outlives the fixture.
    reporter.samples.take();
    reporter
        .sample_writer
        .take()
        .unwrap()
        .join()
        .unwrap()
        .unwrap();
}

#[test]
fn csv_fields_escape_commas_and_quotes() {
    assert_eq!(csv("a,\"b"), "\"a,\"\"b\"");
    assert_eq!(csv("plain"), "plain");
    assert_eq!(csv("a\rb"), "\"a\rb\"");
    assert_eq!(csv("a\nb"), "\"a\nb\"");
    assert!(matches!(csv("plain"), Cow::Borrowed("plain")));
    assert!(matches!(csv("a,b"), Cow::Owned(_)));
}

fn row(outcome: &'static str) -> WriterMessage {
    WriterMessage::Row(Sample {
        stream: None,
        phase: "measurement",
        operation: "read",
        outcome,
        caller_sample_id: None,
        scheduled_ns: 0,
        dispatched_ns: 1,
        dispatch_observed: true,
        completed_ns: 5,
        api_ns: 4,
        response_ns: 5,
        scheduling_ns: 1,
        released_ns: 9,
        logical_keys: 1,
        bytes: 0,
        error_kind: Cow::Borrowed(""),
    })
}

// The runtime has one thread. A blocking send would park it, the drain task below could
// never run, and this test would hang; an awaiting send lets the drain task free capacity.
#[tokio::test(flavor = "current_thread")]
async fn full_queue_suspends_only_the_sender_and_loses_no_sample() {
    let (sender, mut receiver) = mpsc::channel::<WriterMessage>(1);
    assert!(send_bounded(&sender, row("first")).await.delivered);
    let drain = async {
        tokio::task::yield_now().await;
        let mut outcomes = Vec::new();
        for _ in 0..2 {
            match receiver.recv().await {
                Some(WriterMessage::Row(sample)) => outcomes.push(sample.outcome),
                _ => break,
            }
        }
        outcomes
    };
    let (admission, outcomes) = tokio::join!(send_bounded(&sender, row("second")), drain);
    assert!(admission.delivered);
    assert!(admission.stalled_ns.is_some());
    assert_eq!(outcomes, ["first", "second"]);
}

#[tokio::test]
async fn stopped_writer_is_reported_not_delivered() {
    let (sender, receiver) = mpsc::channel::<WriterMessage>(1);
    drop(receiver);
    let admission = send_bounded(&sender, row("lost")).await;
    assert!(!admission.delivered);
    assert!(admission.stalled_ns.is_none());
}

#[test]
fn sample_row_keeps_release_clock_and_empty_replay_identity() {
    let WriterMessage::Row(sample) = row("success") else {
        unreachable!("row builds a sample message");
    };
    let mut line = Vec::new();
    write_sample_row(&mut line, &sample).unwrap();
    assert_eq!(
        String::from_utf8(line).unwrap(),
        "measurement,read,success,0,1,true,5,4,5,1,1,0,,,9,,,,,,,,,\n"
    );
}
