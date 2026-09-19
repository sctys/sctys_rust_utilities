# Redis snapshots

`use sctys_rust_utilities::redis::{RedisSnapshotClient, RedisSnapshotConfig, Snapshot};`

This module distributes **latest state**, not an event history. Each topic stores
one snapshot. A watcher initially returns existing state, then returns changed
versions; intermediate versions may be skipped. A snapshot can also represent a
signal using an inline JSON value such as `null` or a small status object.

```rust,no_run
use sctys_rust_utilities::redis::{RedisSnapshotClient, RedisSnapshotConfig, Snapshot};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let config = RedisSnapshotConfig::new(std::env::var("REDIS_URL")?, "my-project");
let client = RedisSnapshotClient::connect(config).await?;
let snapshot = Snapshot::inline("snapshot-123", &vec![1, 2, 3])?;
let version = client.publish("dataset", &snapshot).await?;
let latest = client.latest("dataset").await?.unwrap();
assert_eq!(latest.version, version);
assert_eq!(latest.snapshot.decode::<Vec<i32>>()?, vec![1, 2, 3]);
# Ok(())
# }
```

See [`examples/redis_snapshots.rs`](../examples/redis_snapshots.rs) for a complete
producer, observer, and worker with lease renewal and Ctrl-C shutdown. Run each
mode in a separate process:

```sh
REDIS_URL=redis://127.0.0.1:6379/ cargo run --example redis_snapshots -- watch
REDIS_URL=redis://127.0.0.1:6379/ cargo run --example redis_snapshots -- worker
REDIS_URL=redis://127.0.0.1:6379/ cargo run --example redis_snapshots -- publish
```

## Configuration and payloads

Use one Redis server (Redis 6.2+), Tokio, and an application-specific namespace.
The URL is supplied by the caller, including authentication/database selection.
No credentials are logged. Cluster and Sentinel discovery are not supported.
Connection and request timeouts default to five seconds. Connection-manager
retries concern establishing a connection; publications are not automatically
reissued on ambiguous errors.

The default maximum serialized envelope size is 1 MiB; `max_payload_bytes` is
configurable and includes the ID, metadata, and payload. The JSON envelope is:

```json
{"id":"snapshot-123","metadata":{"source":"collector"},"payload":{"kind":"inline","value":[1,2,3]}}
```

A reference uses `{"kind":"reference","value":"s3://bucket/immutable-object"}`.
References must be immutable and accessible to recipients. Local paths work only
when consumers share the filesystem. The library does not fetch references.
Save the snapshot successfully before publishing its reference. Saving a file and
publishing to Redis are separate operations; producers must reconcile failed or
ambiguous publications. Reusing a snapshot ID still creates a new Redis version;
the ID is available for application-level deduplication.

## Observers and shared workers

`watch(topic).await?` establishes a dedicated subscription. `next()` subscribes
before reading on reconnect, drains queued wake-ups, and returns current state.
It rereads at least every five seconds while waiting, even without notifications.
Errors are returned; call `next()` again to reconnect with 100 ms–5 s exponential
backoff. Dropping the watcher closes the connection. There is no durable observer
cursor: a newly created watcher returns the current snapshot again.

Use separate group names for independent downstream applications and the same
group for replicas sharing work. `try_claim` returns `None` if the topic is absent,
the newest version is already acknowledged, or another worker owns a live lease.
It does not wait. Poll with a delay, or combine it with watcher notifications and
a periodic retry (lease expiry itself does not publish a notification).

A claim contains an immutable copy of the payload. A healthy worker finishes it
and calls `ack`, even if newer versions arrive meanwhile. The next claim takes
only the latest version. `release` permits another attempt without acknowledging.
After a crash/expiry, the replacement takes the latest snapshot, not an obsolete
interrupted snapshot. With no newer publication, it retries the same version.

Leases default to 60 seconds. Renew every 20 seconds, or below one-third of a
custom lease duration. `renew`, `ack`, and `release` validate a private ownership
token. A stale token returns `LeaseLost`. Use the original client or its clones
for claim operations; independently connected clients cannot use its claim object.
Dropping a claim does not perform network I/O; the lease expires naturally.

Processing must be cancellation-safe and tolerate retries. Stop on renewal
failure; expiry cannot forcibly stop old code or undo/fence external effects.
This is not exactly-once processing. An acknowledgement error may mean the
acknowledgement succeeded but its response was lost; do not assume otherwise.

## Server operation and storage

Lua scripts atomically publish state and update claims. The Redis ACL must allow
scripts (`EVALSHA`/`SCRIPT LOAD`) and the string/hash, expiry, and Pub/Sub commands
used by the module. Names are length-prefixed to isolate namespaces, topics, and
groups. Reserve the `snapshots:` key prefix for this module; do not modify its
keys directly while applications run.

Only lease keys expire automatically. Latest payloads, per-topic version
counters, and group acknowledgement checkpoints persist until explicitly removed.
Memory grows with the number of topics/groups and payload sizes, not with event
history. Remove retired namespaces during application maintenance, with producers
and consumers stopped. Do not delete/reset version counters independently.

Configure persistence/backups appropriate to your recovery needs and a
`noeviction` memory policy. Redis data loss or eviction can lose snapshots,
acknowledgements, counters, or leases. Single-server leases do not provide
failover-safe mutual exclusion. Set capacity limits operationally and monitor
Redis memory, publish failures, worker processing errors, and lease loss.

## Tests

Unit tests require no Redis. Integration tests are explicitly ignored by default:

```sh
cargo test io::redis::tests
REDIS_TEST_URL=redis://127.0.0.1:6379/ cargo test io::redis::tests -- --ignored
```

Use a disposable/local instance. Integration cases use random namespaces, bounded
execution time, and scoped cleanup; they never flush the database.
