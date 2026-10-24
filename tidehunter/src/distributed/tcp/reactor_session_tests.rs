use super::*;
use crate::distributed::storage::WalMemberReadPolicy;
use crate::distributed::tcp::tests::multiplexed::Running;

fn take_wire_responses(client: &mut Client) -> Vec<Envelope<Response>> {
    let mut responses = Vec::new();
    while let Some((frame, frame_len)) = client.stream.receive(client.frame_limit).unwrap() {
        let response = client
            .stream
            .decode(&frame[..frame_len], client.frame_limit)
            .unwrap();
        client.stream.recycle_input(frame);
        responses.push(response);
    }
    responses
}

#[test]
fn one_session_pipelines_appends_but_keeps_fence_and_seal_as_barriers() {
    let server = Running::new(Transport::Tcp, WalMemberReadPolicy::Positional);
    let mut client = server.client();
    client.fence_to(1).unwrap();
    let log = server.node.inventory.lock().active(1).unwrap();
    let (persist_entered, resume_persist) = log.pause_next_persist();

    let first = crate::distributed::tcp::tests::batch(&server.config, 1, 0);
    let second = crate::distributed::tcp::tests::batch(&server.config, 1, 1);
    let after_fence = crate::distributed::tcp::tests::batch(&server.config, 2, 0);
    let append_id = client.reactor.allocate_id().unwrap();
    client
        .stream
        .queue(
            append_id,
            &Request::Append {
                epoch: 1,
                batches: vec![first.encoded().clone().into()],
            },
            client.frame_limit,
        )
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        client.stream.progress(None).unwrap();
        if persist_entered.try_recv().is_ok() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "first append did not reach persistence"
        );
        thread::yield_now();
    }

    let second_append_id = client.reactor.allocate_id().unwrap();
    let fence_id = client.reactor.allocate_id().unwrap();
    let post_fence_append_id = client.reactor.allocate_id().unwrap();
    let seal_id = client.reactor.allocate_id().unwrap();
    for (id, request) in [
        (
            second_append_id,
            Request::Append {
                epoch: 1,
                batches: vec![second.encoded().clone().into()],
            },
        ),
        (fence_id, Request::Fence { epoch: 2 }),
        (
            post_fence_append_id,
            Request::Append {
                epoch: 2,
                batches: vec![after_fence.encoded().clone().into()],
            },
        ),
        (seal_id, Request::Seal { epoch: 2 }),
    ] {
        client
            .stream
            .queue(id, &request, client.frame_limit)
            .unwrap();
    }

    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        client.stream.progress(None).unwrap();
        assert!(
            take_wire_responses(&mut client).is_empty(),
            "an append reply escaped before its persistence barrier"
        );
        if log.receipt_count() >= 2 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "later same-session append did not reach the FIFO control owner"
        );
        thread::yield_now();
    }
    assert!(
        take_wire_responses(&mut client).is_empty(),
        "fence or seal replied while an earlier append was paused"
    );
    resume_persist.send(()).unwrap();

    let deadline = Instant::now() + Duration::from_secs(3);
    let mut responses = Vec::new();
    while responses.len() != 5 {
        client.stream.progress(None).unwrap();
        responses.extend(take_wire_responses(&mut client));
        assert!(
            Instant::now() < deadline,
            "barrier responses did not complete"
        );
        thread::yield_now();
    }
    let mut ids: Vec<_> = responses.iter().map(|response| response.id).collect();
    ids.sort_unstable();
    let mut expected_ids = vec![
        append_id,
        second_append_id,
        fence_id,
        post_fence_append_id,
        seal_id,
    ];
    expected_ids.sort_unstable();
    assert_eq!(ids, expected_ids, "every request keeps its response ID");
    for (id, expected) in [
        (append_id, "first append"),
        (second_append_id, "second append"),
        (post_fence_append_id, "post-fence append"),
    ] {
        let response = responses.iter().find(|response| response.id == id).unwrap();
        assert!(
            matches!(response.body, Response::Appended(_)),
            "{expected} must return its durable receipt"
        );
    }
    let fence = responses
        .iter()
        .find(|response| response.id == fence_id)
        .unwrap();
    let Response::Status(status) = &fence.body else {
        panic!("fence must return status after prior appends");
    };
    assert_eq!(status.active_epoch, Some(2));
    let seal = responses
        .iter()
        .find(|response| response.id == seal_id)
        .unwrap();
    assert!(matches!(seal.body, Response::Sealed));
}
