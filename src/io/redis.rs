//! Latest-snapshot communication over Redis. See `docs/redis.md` for worker examples.
//!
//! Redis retains current state; Pub/Sub only accelerates discovery. Claims provide
//! cooperative leases, not exactly-once processing or fencing of external writes.
#![doc = include_str!("../../docs/redis.md")]

use ::redis::{aio::ConnectionManager, aio::PubSub, AsyncCommands, Client, Script};
use futures::StreamExt;
use serde::de::DeserializeOwned;
use serde_derive::{Deserialize, Serialize};
use serde_json::Value;
use std::{sync::Arc, time::Duration};
use tokio::time::{sleep, timeout, Instant};

/// Failures are returned to the caller; credentials and connection URLs are not logged.
#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    #[error("invalid configuration: {0}")]
    InvalidConfiguration(&'static str),
    #[error("Redis operation failed: {0}")]
    Redis(#[from] ::redis::RedisError),
    #[error("JSON conversion failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("serialized snapshot is {actual} bytes; limit is {limit}")]
    PayloadTooLarge { actual: usize, limit: usize },
    #[error("stored snapshot has missing or invalid fields")]
    InvalidSnapshot,
    #[error("snapshot contains a reference, not inline JSON")]
    NotInline,
    #[error("snapshot lease expired or belongs to another worker")]
    LeaseLost,
    #[error("claim must be used with the client that created it or a clone")]
    ClaimClientMismatch,
    #[error("Redis subscription timed out")]
    SubscriptionTimeout,
    #[error("Redis subscription disconnected; call next again to reconnect")]
    SubscriptionDisconnected,
}

pub type SnapshotResult<T> = Result<T, SnapshotError>;

/// Connection configuration. No Debug implementation, to avoid exposing the secret URL.
#[derive(Clone)]
pub struct RedisSnapshotConfig {
    pub url: String,
    pub namespace: String,
    pub connection_timeout: Duration,
    pub request_timeout: Duration,
    pub max_payload_bytes: usize,
    pub lease_duration: Duration,
}

impl RedisSnapshotConfig {
    pub fn new(url: impl Into<String>, namespace: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            namespace: namespace.into(),
            connection_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(5),
            max_payload_bytes: 1024 * 1024,
            lease_duration: Duration::from_secs(60),
        }
    }

    fn validate(&self) -> SnapshotResult<()> {
        if self.namespace.is_empty() || self.url.is_empty() {
            return Err(SnapshotError::InvalidConfiguration(
                "URL and namespace must be nonempty",
            ));
        }
        if self.connection_timeout.is_zero() || self.request_timeout.is_zero() {
            return Err(SnapshotError::InvalidConfiguration(
                "timeouts must be positive",
            ));
        }
        if self.max_payload_bytes == 0 {
            return Err(SnapshotError::InvalidConfiguration(
                "payload limit must be positive",
            ));
        }
        if self.lease_duration.as_millis() == 0
            || self.lease_duration.as_millis() > i64::MAX as u128
        {
            return Err(SnapshotError::InvalidConfiguration(
                "lease must fit positive Redis milliseconds",
            ));
        }
        Ok(())
    }
}

/// JSON payload or an immutable location accessible to all consumers.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum SnapshotPayload {
    Inline(Value),
    Reference(String),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Snapshot {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
    pub payload: SnapshotPayload,
}

impl Snapshot {
    pub fn inline<T: serde::Serialize>(id: impl Into<String>, value: &T) -> SnapshotResult<Self> {
        Ok(Self {
            id: id.into(),
            metadata: None,
            payload: SnapshotPayload::Inline(serde_json::to_value(value)?),
        })
    }

    pub fn reference(id: impl Into<String>, location: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            metadata: None,
            payload: SnapshotPayload::Reference(location.into()),
        }
    }

    pub fn decode<T: DeserializeOwned>(&self) -> SnapshotResult<T> {
        match &self.payload {
            SnapshotPayload::Inline(value) => Ok(serde_json::from_value(value.clone())?),
            SnapshotPayload::Reference(_) => Err(SnapshotError::NotInline),
        }
    }
}

/// Redis publication order within a topic. Not a timestamp or application snapshot ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SnapshotVersion(pub u64);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VersionedSnapshot {
    pub version: SnapshotVersion,
    pub snapshot: Snapshot,
}

/// An immutable claimed payload. Ownership credentials remain private.
pub struct SnapshotClaim {
    owner: Arc<Inner>,
    keys: GroupKeys,
    token: String,
    snapshot: VersionedSnapshot,
}

impl SnapshotClaim {
    pub fn snapshot(&self) -> &VersionedSnapshot {
        &self.snapshot
    }
}

struct Inner {
    config: RedisSnapshotConfig,
    client: Client,
    connection: ConnectionManager,
}

/// Cheaply cloneable; clones share a multiplexed connection manager.
#[derive(Clone)]
pub struct RedisSnapshotClient {
    inner: Arc<Inner>,
}

struct TopicKeys {
    latest: String,
    sequence: String,
    channel: String,
}
struct GroupKeys {
    lease: String,
    acknowledged: String,
}

fn encode(value: &str) -> String {
    // Length-prefixed components cannot collide even when names contain separators.
    format!("{}:{}", value.len(), value)
}

fn topic_keys(namespace: &str, topic: &str) -> TopicKeys {
    let base = format!("snapshots:{}:{}", encode(namespace), encode(topic));
    TopicKeys {
        latest: format!("{base}:latest"),
        sequence: format!("{base}:sequence"),
        channel: format!("{base}:changed"),
    }
}

fn group_keys(namespace: &str, topic: &str, group: &str) -> GroupKeys {
    let base = format!(
        "snapshots:{}:{}:group:{}",
        encode(namespace),
        encode(topic),
        encode(group)
    );
    GroupKeys {
        lease: format!("{base}:lease"),
        acknowledged: format!("{base}:ack"),
    }
}

fn serialize(snapshot: &Snapshot, limit: usize) -> SnapshotResult<String> {
    let json = serde_json::to_string(snapshot)?;
    if json.len() > limit {
        return Err(SnapshotError::PayloadTooLarge {
            actual: json.len(),
            limit,
        });
    }
    Ok(json)
}

fn deserialize(
    fields: (Option<String>, Option<String>),
) -> SnapshotResult<Option<VersionedSnapshot>> {
    match fields {
        (None, None) => Ok(None),
        (Some(version), Some(payload)) => {
            let version = version
                .parse::<u64>()
                .ok()
                .filter(|v| *v > 0)
                .ok_or(SnapshotError::InvalidSnapshot)?;
            Ok(Some(VersionedSnapshot {
                version: SnapshotVersion(version),
                snapshot: serde_json::from_str(&payload)?,
            }))
        }
        _ => Err(SnapshotError::InvalidSnapshot),
    }
}

const PUBLISH: &str = r#"
local t = redis.call('TYPE', KEYS[2]).ok
if t ~= 'none' and t ~= 'hash' then return redis.error_reply('invalid snapshot key type') end
redis.call('INCR', KEYS[1])
local version = redis.call('GET', KEYS[1])
redis.call('HSET', KEYS[2], 'version', version, 'payload', ARGV[1])
redis.call('PUBLISH', KEYS[3], version)
return version
"#;

const CLAIM: &str = r#"
if redis.call('EXISTS', KEYS[2]) == 1 then return nil end
local latest = redis.call('HMGET', KEYS[1], 'version', 'payload')
if not latest[1] or not latest[2] then return nil end
if redis.call('GET', KEYS[3]) == latest[1] then return nil end
redis.call('SET', KEYS[2], ARGV[1], 'PX', ARGV[2])
return latest
"#;

const UPDATE_CLAIM: &str = r#"
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
if ARGV[2] == 'renew' then
    redis.call('PEXPIRE', KEYS[1], ARGV[3])
else
    if ARGV[2] == 'ack' then redis.call('SET', KEYS[2], ARGV[3]) end
    redis.call('DEL', KEYS[1])
end
return 1
"#;

impl RedisSnapshotClient {
    pub async fn connect(config: RedisSnapshotConfig) -> SnapshotResult<Self> {
        config.validate()?;
        let client = Client::open(config.url.as_str())?;
        let manager_config = ::redis::aio::ConnectionManagerConfig::new()
            .set_connection_timeout(Some(config.connection_timeout))
            .set_response_timeout(Some(config.request_timeout));
        let connection = client
            .get_connection_manager_with_config(manager_config)
            .await?;
        Ok(Self {
            inner: Arc::new(Inner {
                config,
                client,
                connection,
            }),
        })
    }

    /// Publish once. An error may mean Redis committed but the reply was lost.
    pub async fn publish(
        &self,
        topic: &str,
        snapshot: &Snapshot,
    ) -> SnapshotResult<SnapshotVersion> {
        let payload = serialize(snapshot, self.inner.config.max_payload_bytes)?;
        let keys = self.keys(topic)?;
        let version: u64 = Script::new(PUBLISH)
            .key(keys.sequence)
            .key(keys.latest)
            .key(keys.channel)
            .arg(payload)
            .invoke_async(&mut self.inner.connection.clone())
            .await?;
        Ok(SnapshotVersion(version))
    }

    pub async fn latest(&self, topic: &str) -> SnapshotResult<Option<VersionedSnapshot>> {
        let keys = self.keys(topic)?;
        let fields = self
            .inner
            .connection
            .clone()
            .hmget(keys.latest, &["version", "payload"])
            .await?;
        deserialize(fields)
    }

    /// Subscribe before reading state, closing the subscribe/read race.
    pub async fn watch(&self, topic: &str) -> SnapshotResult<SnapshotWatcher> {
        self.keys(topic)?;
        let pubsub = self.subscribe(topic).await?;
        Ok(SnapshotWatcher {
            client: self.clone(),
            topic: topic.into(),
            pubsub: Some(pubsub),
            last: None,
            reconnect_at: Instant::now(),
            backoff: Duration::from_millis(100),
        })
    }

    /// Claim newest state for a group. None means busy, absent, or already acknowledged.
    pub async fn try_claim(
        &self,
        topic: &str,
        group: &str,
    ) -> SnapshotResult<Option<SnapshotClaim>> {
        let topic_keys = self.keys(topic)?;
        if group.is_empty() {
            return Err(SnapshotError::InvalidConfiguration(
                "group must be nonempty",
            ));
        }
        let keys = group_keys(&self.inner.config.namespace, topic, group);
        let token = format!("{:032x}", rand::random::<u128>());
        let fields: Option<(String, String)> = Script::new(CLAIM)
            .key(topic_keys.latest)
            .key(&keys.lease)
            .key(&keys.acknowledged)
            .arg(&token)
            .arg(self.inner.config.lease_duration.as_millis() as u64)
            .invoke_async(&mut self.inner.connection.clone())
            .await?;
        match fields {
            None => Ok(None),
            Some((version, payload)) => {
                let snapshot = deserialize((Some(version), Some(payload)))?
                    .ok_or(SnapshotError::InvalidSnapshot)?;
                Ok(Some(SnapshotClaim {
                    owner: self.inner.clone(),
                    keys,
                    token,
                    snapshot,
                }))
            }
        }
    }

    /// Extend the lease. Call regularly while processing (20 seconds for the default lease).
    pub async fn renew(&self, claim: &SnapshotClaim) -> SnapshotResult<()> {
        self.update_claim(
            claim,
            "renew",
            self.inner.config.lease_duration.as_millis() as u64,
        )
        .await
    }

    /// Record successful processing and release ownership. Newer state remains claimable.
    pub async fn ack(&self, claim: &SnapshotClaim) -> SnapshotResult<()> {
        self.update_claim(claim, "ack", claim.snapshot.version.0)
            .await
    }

    /// Release without acknowledging; a subsequent worker takes the newest snapshot.
    pub async fn release(&self, claim: &SnapshotClaim) -> SnapshotResult<()> {
        self.update_claim(claim, "release", 0).await
    }

    async fn update_claim(
        &self,
        claim: &SnapshotClaim,
        operation: &str,
        value: u64,
    ) -> SnapshotResult<()> {
        if !Arc::ptr_eq(&self.inner, &claim.owner) {
            return Err(SnapshotError::ClaimClientMismatch);
        }
        let updated: u8 = Script::new(UPDATE_CLAIM)
            .key(&claim.keys.lease)
            .key(&claim.keys.acknowledged)
            .arg(&claim.token)
            .arg(operation)
            .arg(value)
            .invoke_async(&mut self.inner.connection.clone())
            .await?;
        if updated == 0 {
            return Err(SnapshotError::LeaseLost);
        }
        Ok(())
    }

    fn keys(&self, topic: &str) -> SnapshotResult<TopicKeys> {
        if topic.is_empty() {
            return Err(SnapshotError::InvalidConfiguration(
                "topic must be nonempty",
            ));
        }
        Ok(topic_keys(&self.inner.config.namespace, topic))
    }

    async fn subscribe(&self, topic: &str) -> SnapshotResult<PubSub> {
        let channel = self.keys(topic)?.channel;
        let mut pubsub = timeout(
            self.inner.config.connection_timeout,
            self.inner.client.get_async_pubsub(),
        )
        .await
        .map_err(|_| SnapshotError::SubscriptionTimeout)??;
        timeout(self.inner.config.request_timeout, pubsub.subscribe(channel))
            .await
            .map_err(|_| SnapshotError::SubscriptionTimeout)??;
        Ok(pubsub)
    }
}

/// Pull-based observer. Dropping it closes its dedicated connection; no worker task is spawned.
/// After errors, call `next` again to retry. Intermediate versions can be skipped.
pub struct SnapshotWatcher {
    client: RedisSnapshotClient,
    topic: String,
    pubsub: Option<PubSub>,
    last: Option<SnapshotVersion>,
    reconnect_at: Instant,
    backoff: Duration,
}

impl SnapshotWatcher {
    pub async fn next(&mut self) -> SnapshotResult<VersionedSnapshot> {
        loop {
            if self.pubsub.is_none() {
                tokio::time::sleep_until(self.reconnect_at).await;
                match self.client.subscribe(&self.topic).await {
                    Ok(pubsub) => {
                        self.pubsub = Some(pubsub);
                        self.backoff = Duration::from_millis(100);
                    }
                    Err(error) => {
                        self.reconnect_at = Instant::now() + self.backoff;
                        self.backoff = (self.backoff * 2).min(Duration::from_secs(5));
                        // State remains usable even if the subscription cannot reconnect.
                        if let Some(snapshot) = self.read_changed().await? {
                            return Ok(snapshot);
                        }
                        return Err(error);
                    }
                }
            }
            // Drain queued wake-ups before reading latest; never deliver notification payloads.
            if let Some(pubsub) = &mut self.pubsub {
                let mut stream = pubsub.on_message();
                // Bound draining so a continuously publishing producer cannot starve reads.
                for _ in 0..256 {
                    use futures::FutureExt;
                    match stream.next().now_or_never() {
                        Some(Some(_)) => (),
                        _ => break,
                    }
                }
            }
            if let Some(snapshot) = self.read_changed().await? {
                return Ok(snapshot);
            }
            if let Some(pubsub) = &mut self.pubsub {
                let disconnected = {
                    let mut stream = pubsub.on_message();
                    tokio::select! {
                        message = stream.next() => message.is_none(),
                        _ = sleep(Duration::from_secs(5)) => false,
                    }
                };
                if disconnected {
                    self.pubsub = None;
                    self.reconnect_at = Instant::now() + self.backoff;
                    return Err(SnapshotError::SubscriptionDisconnected);
                }
            }
        }
    }

    async fn read_changed(&mut self) -> SnapshotResult<Option<VersionedSnapshot>> {
        let snapshot = self.client.latest(&self.topic).await?;
        if let Some(snapshot) = snapshot {
            if self.last != Some(snapshot.version) {
                self.last = Some(snapshot.version);
                return Ok(Some(snapshot));
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;

    #[test]
    fn test_snapshot_json_and_reference() {
        let mut snapshot = Snapshot::inline("id", &vec![1, 2, 3]).unwrap();
        snapshot.metadata = Some(serde_json::json!({"source": "collector"}));
        let encoded = serialize(&snapshot, 1024).unwrap();
        let decoded = deserialize((Some("1".into()), Some(encoded)))
            .unwrap()
            .unwrap();
        assert_eq!(decoded.snapshot, snapshot);
        assert_eq!(
            decoded.snapshot.decode::<Vec<i32>>().unwrap(),
            vec![1, 2, 3]
        );
        let reference = Snapshot::reference("ref", "s3://bucket/immutable-id");
        assert!(matches!(
            reference.decode::<Value>(),
            Err(SnapshotError::NotInline)
        ));
        let encoded = serialize(&reference, 1024).unwrap();
        assert_eq!(
            serde_json::from_str::<Snapshot>(&encoded).unwrap(),
            reference
        );
    }

    #[test]
    fn test_payload_limit_and_invalid_fields() {
        let snapshot = Snapshot::inline("id", &"é").unwrap();
        let size = serialize(&snapshot, usize::MAX).unwrap().len();
        assert!(serialize(&snapshot, size).is_ok());
        assert!(matches!(
            serialize(&snapshot, size - 1),
            Err(SnapshotError::PayloadTooLarge { .. })
        ));
        assert_eq!(deserialize((None, None)).unwrap(), None);
        for version in ["0", "no", "-1"] {
            assert!(matches!(
                deserialize((Some(version.into()), Some("{}".into()))),
                Err(SnapshotError::InvalidSnapshot)
            ));
        }
        assert!(matches!(
            deserialize((Some("1".into()), None)),
            Err(SnapshotError::InvalidSnapshot)
        ));
        assert!(matches!(
            deserialize((Some("1".into()), Some("bad JSON".into()))),
            Err(SnapshotError::Json(_))
        ));
    }

    #[test]
    fn test_config_validation_and_key_isolation() {
        let valid = RedisSnapshotConfig::new("redis://localhost", "test");
        assert!(valid.validate().is_ok());
        let mut invalid = valid.clone();
        invalid.namespace.clear();
        assert!(invalid.validate().is_err());
        invalid = valid.clone();
        invalid.request_timeout = Duration::ZERO;
        assert!(invalid.validate().is_err());
        invalid = valid.clone();
        invalid.connection_timeout = Duration::ZERO;
        assert!(invalid.validate().is_err());
        invalid = valid.clone();
        invalid.max_payload_bytes = 0;
        assert!(invalid.validate().is_err());
        invalid = valid;
        invalid.lease_duration = Duration::from_nanos(1);
        assert!(invalid.validate().is_err());
        assert_ne!(topic_keys("a:b", "c").latest, topic_keys("a", "b:c").latest);
        assert_ne!(
            group_keys("a", "b:c", "d").lease,
            group_keys("a", "b", "c:d").lease
        );
        assert_ne!(
            topic_keys("a", "b:group:c").latest,
            group_keys("a", "b", "c").lease
        );
    }

    // Explicit opt-in: REDIS_TEST_URL must point to a disposable/local Redis instance.
    // Each case owns a random namespace; cleanup never flushes the database.
    async fn with_redis<F, Fut>(test: F)
    where
        F: FnOnce(RedisSnapshotClient) -> Fut,
        Fut: Future<Output = ()>,
    {
        use futures::FutureExt;
        let url =
            std::env::var("REDIS_TEST_URL").expect("set REDIS_TEST_URL to run ignored Redis tests");
        let namespace = format!("sctys-test-{:032x}", rand::random::<u128>());
        let mut config = RedisSnapshotConfig::new(url, &namespace);
        config.lease_duration = Duration::from_millis(800);
        let client = RedisSnapshotClient::connect(config).await.unwrap();
        let outcome =
            std::panic::AssertUnwindSafe(timeout(Duration::from_secs(20), test(client.clone())))
                .catch_unwind()
                .await;
        let mut connection = client.inner.connection.clone();
        let pattern = format!("snapshots:{}:*", encode(&namespace));
        let mut cursor = 0u64;
        loop {
            let (next, keys): (u64, Vec<String>) = ::redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&pattern)
                .query_async(&mut connection)
                .await
                .unwrap();
            if !keys.is_empty() {
                connection.del::<_, usize>(keys).await.unwrap();
            }
            cursor = next;
            if cursor == 0 {
                break;
            }
        }
        match outcome {
            Ok(result) => result.expect("Redis test timed out"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
    }

    #[tokio::test]
    #[ignore = "requires REDIS_TEST_URL"]
    async fn test_redis_roundtrip_and_concurrent_publish() {
        with_redis(|client| async move {
            assert_eq!(client.latest("data").await.unwrap(), None);
            let snapshot = Snapshot::reference("saved", "s3://bucket/version-1");
            client.publish("ref", &snapshot).await.unwrap();
            assert_eq!(
                client.latest("ref").await.unwrap().unwrap().snapshot,
                snapshot
            );
            let versions = futures::future::join_all((0..20).map(|i| {
                let client = client.clone();
                async move {
                    client
                        .publish("data", &Snapshot::inline(i.to_string(), &i).unwrap())
                        .await
                        .unwrap()
                }
            }))
            .await;
            let mut sorted = versions.clone();
            sorted.sort();
            sorted.dedup();
            assert_eq!(sorted.len(), 20);
            let latest = client.latest("data").await.unwrap().unwrap();
            assert_eq!(latest.version, *sorted.last().unwrap());
            let i = versions.iter().position(|v| *v == latest.version).unwrap();
            assert_eq!(latest.snapshot.decode::<usize>().unwrap(), i);
            let mut limited_config = client.inner.config.clone();
            limited_config.max_payload_bytes = 1;
            let limited = RedisSnapshotClient::connect(limited_config).await.unwrap();
            assert!(matches!(
                limited.publish("data", &snapshot).await,
                Err(SnapshotError::PayloadTooLarge { .. })
            ));
            assert_eq!(client.latest("data").await.unwrap(), Some(latest));
            let mut connection = client.inner.connection.clone();
            connection
                .hset::<_, _, _, ()>(client.keys("data").unwrap().latest, "payload", "broken")
                .await
                .unwrap();
            assert!(matches!(
                client.latest("data").await,
                Err(SnapshotError::Json(_))
            ));
        })
        .await;
    }

    #[tokio::test]
    #[ignore = "requires REDIS_TEST_URL"]
    async fn test_redis_watcher_initial_coalescing_and_reconnect() {
        with_redis(|client| async move {
            client
                .publish("data", &Snapshot::inline("1", &1).unwrap())
                .await
                .unwrap();
            let mut watcher = client.watch("data").await.unwrap();
            assert_eq!(watcher.next().await.unwrap().snapshot.id, "1");
            for id in ["2", "3"] {
                client
                    .publish("data", &Snapshot::inline(id, &id).unwrap())
                    .await
                    .unwrap();
            }
            assert_eq!(watcher.next().await.unwrap().snapshot.id, "3");
            assert!(timeout(Duration::from_millis(50), watcher.next())
                .await
                .is_err());
            // Force an actual subscription disconnection, targeting only this connection.
            drop(watcher.pubsub.take().unwrap());
            client
                .publish("data", &Snapshot::inline("offline", &0).unwrap())
                .await
                .unwrap();
            assert_eq!(watcher.next().await.unwrap().snapshot.id, "offline");
            // Unsubscribe to simulate missed notifications while state remains available.
            watcher
                .pubsub
                .as_mut()
                .unwrap()
                .unsubscribe(client.keys("data").unwrap().channel)
                .await
                .unwrap();
            let publisher = async {
                sleep(Duration::from_millis(50)).await;
                client
                    .publish("data", &Snapshot::inline("polled", &0).unwrap())
                    .await
                    .unwrap();
            };
            let (next, ()) = tokio::join!(watcher.next(), publisher);
            assert_eq!(next.unwrap().snapshot.id, "polled");
            let mut empty = client.watch("empty").await.unwrap();
            let publisher = async {
                sleep(Duration::from_millis(50)).await;
                client
                    .publish("empty", &Snapshot::inline("first", &0).unwrap())
                    .await
                    .unwrap();
            };
            let (next, ()) = tokio::join!(empty.next(), publisher);
            assert_eq!(next.unwrap().snapshot.id, "first");
        })
        .await;
    }

    #[tokio::test]
    #[ignore = "requires REDIS_TEST_URL"]
    async fn test_redis_competing_workers_and_independent_groups() {
        with_redis(|client| async move {
            let replica = RedisSnapshotClient::connect(client.inner.config.clone())
                .await
                .unwrap();
            client
                .publish("data", &Snapshot::inline("1", &1).unwrap())
                .await
                .unwrap();
            let (a, b) = tokio::join!(
                client.try_claim("data", "workers"),
                replica.try_claim("data", "workers")
            );
            let (a, b) = (a.unwrap(), b.unwrap());
            assert_ne!(a.is_some(), b.is_some());
            let (active, owner, other) = match (a, b) {
                (Some(claim), None) => (claim, &client, &replica),
                (None, Some(claim)) => (claim, &replica, &client),
                _ => unreachable!("exactly one replica must acquire the claim"),
            };
            assert!(matches!(
                other.ack(&active).await,
                Err(SnapshotError::ClaimClientMismatch)
            ));
            let independent = client.try_claim("data", "other").await.unwrap().unwrap();
            client.ack(&independent).await.unwrap();
            client
                .publish("data", &Snapshot::inline("2", &2).unwrap())
                .await
                .unwrap();
            client
                .publish("data", &Snapshot::inline("3", &3).unwrap())
                .await
                .unwrap();
            assert_eq!(active.snapshot().snapshot.id, "1");
            assert!(client.try_claim("data", "workers").await.unwrap().is_none());
            owner.ack(&active).await.unwrap();
            let next = client.try_claim("data", "workers").await.unwrap().unwrap();
            assert_eq!(next.snapshot().snapshot.id, "3");
            client.ack(&next).await.unwrap();
            assert!(client.try_claim("data", "workers").await.unwrap().is_none());
            assert!(client.try_claim("data", "other").await.unwrap().is_some());
        })
        .await;
    }

    #[tokio::test]
    #[ignore = "requires REDIS_TEST_URL"]
    async fn test_redis_lease_expiry_renewal_release_and_stale_owners() {
        with_redis(|client| async move {
            client
                .publish("data", &Snapshot::inline("1", &1).unwrap())
                .await
                .unwrap();
            let old = client.try_claim("data", "workers").await.unwrap().unwrap();
            sleep(Duration::from_millis(450)).await;
            client.renew(&old).await.unwrap();
            sleep(Duration::from_millis(450)).await;
            assert!(client.try_claim("data", "workers").await.unwrap().is_none());
            client
                .publish("data", &Snapshot::inline("2", &2).unwrap())
                .await
                .unwrap();
            sleep(Duration::from_millis(450)).await;
            let replacement = client.try_claim("data", "workers").await.unwrap().unwrap();
            assert_eq!(replacement.snapshot().snapshot.id, "2");
            assert!(matches!(
                client.renew(&old).await,
                Err(SnapshotError::LeaseLost)
            ));
            assert!(matches!(
                client.ack(&old).await,
                Err(SnapshotError::LeaseLost)
            ));
            assert!(matches!(
                client.release(&old).await,
                Err(SnapshotError::LeaseLost)
            ));
            client.release(&replacement).await.unwrap();
            let retry = client.try_claim("data", "workers").await.unwrap().unwrap();
            assert_eq!(retry.snapshot(), replacement.snapshot());
            client.clone().ack(&retry).await.unwrap();
            assert!(matches!(
                client.ack(&retry).await,
                Err(SnapshotError::LeaseLost)
            ));
        })
        .await;
    }
}
