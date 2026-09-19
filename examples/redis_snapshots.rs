//! REDIS_URL=redis://127.0.0.1/ cargo run --example redis_snapshots -- publish|watch|worker
use sctys_rust_utilities::redis::{
    RedisSnapshotClient, RedisSnapshotConfig, Snapshot, SnapshotResult, VersionedSnapshot,
};
use std::time::Duration;
use tokio::time::{interval, sleep, MissedTickBehavior};

const TOPIC: &str = "prices";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = RedisSnapshotConfig::new(std::env::var("REDIS_URL")?, "snapshot-example");
    let client = RedisSnapshotClient::connect(config).await?;
    match std::env::args().nth(1).as_deref() {
        Some("publish") => {
            // Save large snapshots to immutable storage first, then use Snapshot::reference.
            let snapshot =
                Snapshot::inline("collector-snapshot-1", &serde_json::json!({"price": 42}))?;
            println!("Published {:?}", client.publish(TOPIC, &snapshot).await?);
        }
        Some("watch") => {
            let mut watcher = client.watch(TOPIC).await?;
            let shutdown = tokio::signal::ctrl_c();
            tokio::pin!(shutdown);
            loop {
                tokio::select! {
                    result = &mut shutdown => { result?; return Ok(()); }
                    result = watcher.next() => match result {
                        Ok(snapshot) => println!("Latest: {snapshot:?}"),
                        Err(error) => {
                            eprintln!("Watcher error: {error}");
                            tokio::select! {
                                result = &mut shutdown => { result?; return Ok(()); }
                                _ = sleep(Duration::from_secs(1)) => {}
                            }
                        }
                    }
                }
            }
        }
        Some("worker") => worker(&client).await?,
        _ => return Err("usage: redis_snapshots publish|watch|worker".into()),
    }
    Ok(())
}

async fn worker(client: &RedisSnapshotClient) -> Result<(), Box<dyn std::error::Error>> {
    // Replicas share this group. A separate downstream application uses a different group.
    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);
    loop {
        let claim = tokio::select! {
            result = &mut shutdown => { result?; return Ok(()); }
            claim = client.try_claim(TOPIC, "analytics") => claim?,
        };
        let Some(claim) = claim else {
            tokio::select! {
                result = &mut shutdown => { result?; return Ok(()); }
                _ = sleep(Duration::from_secs(1)) => {}
            }
            continue;
        };
        let processing = process(claim.snapshot());
        tokio::pin!(processing);
        let mut renewal = interval(Duration::from_secs(20));
        renewal.set_missed_tick_behavior(MissedTickBehavior::Delay);
        renewal.tick().await; // Consume the immediate first tick.
        loop {
            tokio::select! {
                result = &mut shutdown => {
                    result?;
                    client.release(&claim).await?;
                    return Ok(()); // Drops the processing future.
                }
                _ = renewal.tick() => {
                    // On lease loss or uncertain renewal, stop processing; never acknowledge.
                    client.renew(&claim).await?;
                }
                result = &mut processing => {
                    match result {
                        Ok(()) => client.ack(&claim).await?,
                        Err(error) => { client.release(&claim).await?; return Err(error.into()); }
                    }
                    break;
                }
            }
        }
    }
}

async fn process(snapshot: &VersionedSnapshot) -> SnapshotResult<()> {
    // Replace with cancellation-safe, idempotent application work. Lease expiry cannot
    // cancel remote requests or fence external writes; use snapshot IDs for deduplication.
    println!("Processing: {snapshot:?}");
    sleep(Duration::from_millis(100)).await;
    Ok(())
}
