//! Caller-owned bounded workers sharing the benchmark's one native Db/cache.
use super::{Db, KeySpace, Result, invalid, native_error};
use std::sync::{Arc, mpsc};
use std::thread::{self, JoinHandle};
use tidehunter::db::DbError;
use tidehunter::minibytes::Bytes;

type Completion = std::result::Result<Option<Bytes>, DbError>;

struct Worker {
    requests: Option<mpsc::SyncSender<(KeySpace, [u8; 8])>>,
    completions: mpsc::Receiver<Completion>,
    thread: Option<JoinHandle<()>>,
    busy: bool,
}

impl Drop for Worker {
    fn drop(&mut self) {
        // At most one request/completion per worker, so even abandoned work
        // can publish into its capacity-one channel without a consumer.
        self.requests.take();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

pub(super) struct NativeReadWorkers {
    workers: Vec<Worker>,
}

impl NativeReadWorkers {
    pub(super) fn open(db: &Arc<Db>, workers: usize, window: usize) -> Result<Self> {
        if !(1..=32).contains(&workers) || !(1..=256).contains(&window) {
            return Err(invalid("invalid native read worker/window bound").into());
        }
        let mut pool = Self {
            workers: Vec::with_capacity(workers.min(window)),
        };
        for index in 0..workers.min(window) {
            let (requests, incoming) = mpsc::sync_channel::<(KeySpace, [u8; 8])>(1);
            let (completed, completions) = mpsc::sync_channel(1);
            let (ready, started) = mpsc::sync_channel(1);
            let db = Arc::clone(db);
            let thread = thread::Builder::new()
                .name(format!("bench-native-read-{index}"))
                .spawn(move || {
                    if ready.send(()).is_err() {
                        return;
                    }
                    while let Ok((ks, key)) = incoming.recv() {
                        if completed.send(db.get(ks, &key)).is_err() {
                            break;
                        }
                    }
                })?;
            // Ownership is installed before waiting: startup failure also
            // closes channels and joins all successfully spawned workers.
            pool.workers.push(Worker {
                requests: Some(requests),
                completions,
                thread: Some(thread),
                busy: false,
            });
            started
                .recv()
                .map_err(|_| invalid("native read worker failed during startup"))?;
        }
        Ok(pool)
    }

    pub(super) fn len(&self) -> usize {
        self.workers.len()
    }

    pub(super) fn submit(&mut self, slot: usize, ks: KeySpace, key: [u8; 8]) -> Result<()> {
        let worker = &mut self.workers[slot];
        if worker.busy {
            return Err(invalid("native read worker already has an unretired request").into());
        }
        worker
            .requests
            .as_ref()
            .unwrap()
            .try_send((ks, key))
            .map_err(|_| invalid("native read worker unavailable"))?;
        worker.busy = true;
        Ok(())
    }

    pub(super) fn finish(&mut self, slot: usize) -> Result<Option<Bytes>> {
        let worker = &mut self.workers[slot];
        if !worker.busy {
            return Err(invalid("native read worker has no pending request").into());
        }
        worker.busy = false;
        worker
            .completions
            .recv()
            .map_err(|_| invalid("native read worker exited without completing"))?
            .map_err(native_error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidehunter::{config::Config, key_shape::KeyShape, metrics::Metrics};

    #[test]
    fn bounded_workers_share_values_and_join_with_abandoned_completions() {
        let dir = tempdir::TempDir::new("native-read-workers").unwrap();
        let shape = KeyShape::new_single(8, 1, tidehunter::key_shape::KeyType::uniform(1));
        let ks = shape.iter_ks().next().unwrap().id();
        let db = Db::open(
            dir.path(),
            shape,
            Arc::new(Config::small()),
            Metrics::new_in_enabled(&prometheus::Registry::new(), false),
        )
        .unwrap();
        let key = [3; 8];
        let expected = Bytes::from(vec![7; 8192]);
        let mut batch = db.write_batch();
        batch.write(ks, key.to_vec(), expected.clone());
        batch.commit().unwrap();
        assert!(NativeReadWorkers::open(&db, 0, 32).is_err());
        assert!(NativeReadWorkers::open(&db, 33, 32).is_err());
        assert!(NativeReadWorkers::open(&db, 2, 0).is_err());
        let mut pool = NativeReadWorkers::open(&db, 32, 2).unwrap();
        assert_eq!(pool.len(), 2);
        assert!(pool.finish(0).is_err());
        for _ in 0..3 {
            pool.submit(0, ks, key).unwrap();
            pool.submit(1, ks, key).unwrap();
            assert!(pool.submit(0, ks, key).is_err());
            assert_eq!(pool.finish(0).unwrap(), Some(expected.clone()));
            assert_eq!(pool.finish(1).unwrap(), Some(expected.clone()));
        }
        pool.submit(0, ks, key).unwrap();
        pool.submit(1, ks, key).unwrap();
        drop(pool);
        db.wait_for_background_threads_to_finish();
    }
}
