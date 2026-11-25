use super::*;
use std::thread;

#[test]
fn full_control_wait_releases_order_and_recaptures_its_fence() {
    let queue = Arc::new(OwnerQueue::new(4));
    let (sender, receiver) = OwnerSender::channel(1, Arc::clone(&queue), Duration::from_secs(5));
    let sender = Arc::new(sender);
    sender.send(Command::QueueReady).unwrap();
    let (waiting, entered) = mpsc::sync_channel(1);
    *sender.waiting.lock() = Some(waiting);
    let (reply, pending) = mpsc::sync_channel(1);
    let sending = Arc::clone(&sender);
    let send = thread::spawn(move || sending.send(Command::Stats(reply)));
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    // Full admission has no effect and must not retain owner.order.
    assert!(matches!(pending.try_recv(), Err(mpsc::TryRecvError::Empty)));
    let (earlier, _) = queue.reserve(|| 1);
    assert_eq!(earlier, 1);
    assert!(matches!(
        receiver.recv().unwrap().command,
        Command::QueueReady
    ));
    send.join().unwrap().unwrap();
    let control = receiver.recv().unwrap();
    assert_eq!(control.before_owner_seq, 2);
    let (later, _) = queue.reserve(|| 2);
    assert_eq!(later, control.before_owner_seq);
    assert!(
        queue
            .take_ready_before(control.before_owner_seq, |_| true)
            .is_none()
    );
    queue.abort(earlier);
    assert!(
        queue
            .take_ready_before(control.before_owner_seq, |_| true)
            .is_none()
    );
    assert_eq!(queue.head(), control.before_owner_seq);
    let Command::Stats(reply) = control.command else {
        panic!("expected ordered stats control");
    };
    drop(reply);
    assert!(matches!(
        pending.try_recv(),
        Err(mpsc::TryRecvError::Disconnected)
    ));
}

#[test]
fn expired_full_control_has_no_effect_and_receiver_drop_wakes_waiters() {
    let queue = Arc::new(OwnerQueue::new(2));
    let (sender, receiver) = OwnerSender::channel(1, queue, Duration::from_secs(5));
    let sender = Arc::new(sender);
    sender.send(Command::QueueReady).unwrap();
    let (reply, pending) = mpsc::sync_channel(1);
    assert!(
        sender
            .send_until(Command::Stats(reply), Instant::now())
            .is_err()
    );
    assert!(matches!(
        pending.try_recv(),
        Err(mpsc::TryRecvError::Disconnected)
    ));
    let (waiting, entered) = mpsc::sync_channel(1);
    *sender.waiting.lock() = Some(waiting);
    let sending = Arc::clone(&sender);
    let send = thread::spawn(move || sending.send(Command::QueueReady));
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    drop(receiver);
    assert!(send.join().unwrap().is_err());
}

#[test]
fn explicit_close_wakes_full_sender_and_retains_only_admitted_commands() {
    let queue = Arc::new(OwnerQueue::new(2));
    let (sender, receiver) = OwnerSender::channel(1, queue, Duration::from_secs(5));
    let sender = Arc::new(sender);
    sender.send(Command::QueueReady).unwrap();
    let (waiting, entered) = mpsc::sync_channel(1);
    *sender.waiting.lock() = Some(waiting);
    let sending = Arc::clone(&sender);
    let (reply, pending) = mpsc::sync_channel(1);
    let send = thread::spawn(move || sending.send(Command::Stats(reply)));
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    sender.close();
    sender.close();
    assert!(send.join().unwrap().is_err());
    assert!(matches!(
        pending.try_recv(),
        Err(mpsc::TryRecvError::Disconnected)
    ));
    assert!(matches!(
        receiver.recv().unwrap().command,
        Command::QueueReady
    ));
    assert!(receiver.recv().is_err());
    assert!(matches!(
        sender.try_send(Command::QueueReady),
        Err(mpsc::TrySendError::Disconnected(_))
    ));
}

#[test]
fn control_handles_preserve_closed_and_deadline_admission_errors() {
    use crate::distributed::remote_db::pipeline_tests::Fixture;
    use std::sync::atomic::{AtomicBool, Ordering};

    let fixture = Fixture::new();
    let database = fixture.open();
    let mut handle = database.core.clients[0].control_handle();
    let queue = Arc::new(OwnerQueue::new(2));
    let (sender, receiver) = OwnerSender::channel(1, queue, Duration::from_secs(5));
    let sender = Arc::new(sender);
    handle.sender = Arc::downgrade(&sender);
    let invoked = Arc::new(AtomicBool::new(false));
    let observed = invoked.clone();
    let error = handle
        .control_until(Instant::now(), move |_| {
            observed.store(true, Ordering::Relaxed);
            Ok(())
        })
        .unwrap_err();
    assert_eq!(
        error.message(),
        "owner control deadline expired before admission"
    );
    sender.close();
    assert_eq!(
        handle.owner_timeline_arm(1).unwrap_err().message(),
        "TCP worker stopped"
    );
    let observed = invoked.clone();
    assert_eq!(
        handle
            .control(move |_| {
                observed.store(true, Ordering::Relaxed);
                Ok(())
            })
            .unwrap_err()
            .message(),
        "TCP worker stopped"
    );
    let observed = invoked.clone();
    assert_eq!(
        handle
            .control_until(Instant::now() + Duration::from_secs(1), move |_| {
                observed.store(true, Ordering::Relaxed);
                Ok(())
            })
            .unwrap_err()
            .message(),
        "TCP worker stopped"
    );
    assert!(!invoked.load(Ordering::Relaxed));
    assert!(receiver.recv().is_err());
    database.shutdown().unwrap();
}
