//! Replay transport over the admitted persistent libvalkey pool, shared by TCP/RDMA.
use super::{Response, ValkeyRdmaBackend, invalid};
use crate::backend::BackendResult;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;

fn value(response: Response, statuses: bool) -> redis::Value {
    match response {
        Response::Nil => redis::Value::Nil,
        Response::Integer(n) => redis::Value::Int(n),
        Response::Bytes(bytes) if statuses => {
            redis::Value::SimpleString(String::from_utf8_lossy(&bytes).into_owned())
        }
        Response::Bytes(bytes) => redis::Value::BulkString(bytes),
        Response::Array(rows) => {
            redis::Value::Array(rows.into_iter().map(|r| value(r, statuses)).collect())
        }
    }
}

/// One checkout remains exclusive for every command and reply in a transaction/run.
/// No retry of a submitted mutation. Incomplete replies retain the pool's poison bit.
/// The caller supplies the single logical-commit deadline, including pool wait time.
pub async fn pipeline(
    backend: &Arc<ValkeyRdmaBackend>,
    owner: usize,
    commands: Vec<Vec<Vec<u8>>>,
    deadline: Instant,
) -> BackendResult<Vec<redis::Value>> {
    let node = backend
        .nodes
        .get(owner)
        .ok_or_else(|| invalid("invalid replay owner"))?;
    let index = node.next.fetch_add(1, Ordering::Relaxed) % node.connections.len();
    let conn = Arc::clone(&node.connections[index]);
    let ctx = Arc::clone(&node.ctx);
    tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), async move {
        let mut guard = conn.lock_owned().await;
        tokio::task::spawn_blocking(move || {
            let refs: Vec<_> = commands.iter().map(Vec::as_slice).collect();
            let statuses = commands
                .first()
                .and_then(|c| c.first())
                .is_some_and(|c| c == b"SET" || c == b"MULTI");
            ctx.pipeline(&mut guard, &refs, deadline, None, None, None)
                .map(|rows| rows.into_iter().map(|r| value(r, statuses)).collect())
        })
        .await
        .map_err(|e| invalid(format!("libvalkey replay worker failed: {e}")))?
    })
    .await?
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn status_replies_and_binary_values_remain_distinct() {
        assert!(
            matches!(value(Response::Bytes(b"QUEUED".to_vec()), true), redis::Value::SimpleString(s) if s == "QUEUED")
        );
        assert!(
            matches!(value(Response::Bytes(b"OK".to_vec()), false), redis::Value::BulkString(b) if b == b"OK")
        );
        assert!(
            matches!(value(Response::Array(vec![Response::Bytes(b"OK".to_vec())]), true), redis::Value::Array(v) if matches!(&v[0], redis::Value::SimpleString(s) if s == "OK"))
        );
    }
}
