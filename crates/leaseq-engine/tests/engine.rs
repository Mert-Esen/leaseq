//! End-to-end behaviour of the engine against a real directory.

use std::collections::HashSet;
use std::fs;
use std::sync::Arc;
use std::time::Duration;

use leaseq_engine::core::{JobSpec, Priority, QueueName, RetryPolicy, Status};
use leaseq_engine::{Config, Engine, EngineError, JobFilter, ManualClock, OpenError, SyncPolicy};

const TTL: Duration = Duration::from_secs(30);
const NOW: Duration = Duration::ZERO;

fn queue() -> QueueName {
    "work".parse().unwrap()
}

fn spec(payload: impl Into<String>) -> JobSpec {
    JobSpec {
        queue: queue(),
        payload: payload.into(),
        priority: Priority::default(),
        max_attempts: 2,
    }
}

fn config(dir: &tempfile::TempDir) -> Config {
    let mut config = Config::new(dir.path());
    config.sync = SyncPolicy::Never;
    config.retry = RetryPolicy {
        base: Duration::from_secs(1),
        max: Duration::from_secs(1),
    };
    config
}

#[tokio::test]
async fn state_survives_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(config(&dir)).unwrap();
    let done = engine.enqueue(spec("done"), NOW).await.unwrap();
    let kept = engine.enqueue(spec("kept"), NOW).await.unwrap();
    let leased = engine.lease(queue(), 1, TTL, NOW).await.unwrap();
    assert_eq!(leased[0].id, done);
    engine.ack(done, leased[0].token).await.unwrap();
    engine.shutdown().await;

    let engine = Engine::open(config(&dir)).unwrap();
    let jobs = engine
        .list(JobFilter {
            limit: 10,
            ..JobFilter::default()
        })
        .await
        .unwrap();
    assert_eq!(jobs.len(), 1);
    assert_eq!((jobs[0].id, jobs[0].payload.as_str()), (kept, "kept"));
    assert_eq!(engine.stats()[0].completed, 1);
    // Identifiers are never reused.
    assert!(engine.enqueue(spec("new"), NOW).await.unwrap() > kept);
}

#[tokio::test]
async fn compaction_keeps_state_and_removes_old_logs() {
    let dir = tempfile::tempdir().unwrap();
    let mut small = config(&dir);
    small.compact_after_bytes = 512;
    let engine = Engine::open(small.clone()).unwrap();
    for n in 0..100 {
        engine.enqueue(spec(n.to_string()), NOW).await.unwrap();
    }
    let before = engine.stats();
    engine.shutdown().await;

    let logs: Vec<_> = fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .filter(|name| name.starts_with("wal-"))
        .collect();
    assert_eq!(logs.len(), 1, "{logs:?}");
    assert_ne!(logs[0], "wal-0.log");

    let engine = Engine::open(small).unwrap();
    assert_eq!(engine.stats(), before);
}

#[tokio::test]
async fn a_deleted_snapshot_is_reported_instead_of_losing_data() {
    let dir = tempfile::tempdir().unwrap();
    let mut small = config(&dir);
    small.compact_after_bytes = 1;
    let engine = Engine::open(small.clone()).unwrap();
    engine.enqueue(spec("x"), NOW).await.unwrap();
    engine.shutdown().await;

    fs::remove_file(dir.path().join("snapshot.bin")).unwrap();
    assert!(matches!(
        Engine::open(small),
        Err(OpenError::Inconsistent(_))
    ));
}

#[tokio::test]
async fn the_directory_is_exclusive_until_shutdown() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(config(&dir)).unwrap();
    assert!(matches!(
        Engine::open(config(&dir)),
        Err(OpenError::InUse(_))
    ));
    let clone = engine.clone();
    engine.shutdown().await;
    assert!(matches!(clone.tick().await, Err(EngineError::Closed)));
    Engine::open(config(&dir)).unwrap();
}

#[tokio::test]
async fn lapsed_leases_are_retried_then_dead_lettered() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(ManualClock::default());
    let engine = Engine::open_with_clock(config(&dir), clock.clone()).unwrap();
    let id = engine.enqueue(spec("flaky"), NOW).await.unwrap();

    let first = engine.lease(queue(), 1, TTL, NOW).await.unwrap().remove(0);
    clock.advance(TTL);
    assert!(matches!(
        engine.ack(id, first.token).await,
        Err(EngineError::Queue(_))
    ));

    clock.advance(Duration::from_secs(1));
    let second = engine.lease(queue(), 1, TTL, NOW).await.unwrap().remove(0);
    assert_eq!(second.attempt, 2);
    engine.nack(id, second.token, "boom".into()).await.unwrap();

    let job = engine.get(id).await.unwrap().unwrap();
    assert_eq!(job.status, Status::Dead);
    assert_eq!(job.last_error.as_deref(), Some("boom"));

    engine.revive(id).await.unwrap();
    assert_eq!(engine.stats()[0].ready, 1);
}

#[tokio::test]
async fn long_polling_wakes_for_new_and_delayed_jobs() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(config(&dir)).unwrap();
    for delay in [NOW, Duration::from_millis(100)] {
        let waiter = {
            let engine = engine.clone();
            tokio::spawn(async move { engine.lease(queue(), 1, TTL, TTL).await })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        let id = engine.enqueue(spec("late"), delay).await.unwrap();
        let jobs = tokio::time::timeout(Duration::from_secs(5), waiter)
            .await
            .expect("waiter was not woken")
            .unwrap()
            .unwrap();
        assert_eq!(jobs[0].id, id);
    }
    let empty = engine
        .lease(queue(), 1, TTL, Duration::from_millis(20))
        .await
        .unwrap();
    assert!(empty.is_empty());
}

#[tokio::test]
async fn an_unbounded_wait_does_not_overflow() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(config(&dir)).unwrap();
    engine.enqueue(spec("x"), NOW).await.unwrap();
    let jobs = engine.lease(queue(), 1, TTL, Duration::MAX).await.unwrap();
    assert_eq!(jobs.len(), 1);
}

#[tokio::test]
async fn limits_are_enforced() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(config(&dir)).unwrap();
    let huge = "x".repeat(256 * 1024 + 1);
    assert!(matches!(
        engine.enqueue(spec(huge), NOW).await,
        Err(EngineError::Invalid(_))
    ));
    for (max, ttl) in [(0, TTL), (101, TTL), (1, NOW), (1, Duration::MAX)] {
        assert!(matches!(
            engine.lease(queue(), max, ttl, NOW).await,
            Err(EngineError::Invalid(_))
        ));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_producers_and_consumers_process_every_job_once() {
    const PRODUCERS: usize = 8;
    const PER_PRODUCER: usize = 100;
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(config(&dir)).unwrap();

    let producers: Vec<_> = (0..PRODUCERS)
        .map(|p| {
            let engine = engine.clone();
            tokio::spawn(async move {
                for n in 0..PER_PRODUCER {
                    engine.enqueue(spec(format!("{p}-{n}")), NOW).await.unwrap();
                }
            })
        })
        .collect();
    let consumers: Vec<_> = (0..8)
        .map(|_| {
            let engine = engine.clone();
            tokio::spawn(async move {
                let mut seen = Vec::new();
                loop {
                    let jobs = engine
                        .lease(queue(), 7, TTL, Duration::from_millis(500))
                        .await
                        .unwrap();
                    if jobs.is_empty() {
                        return seen;
                    }
                    for job in jobs {
                        engine.ack(job.id, job.token).await.unwrap();
                        seen.push(job.payload);
                    }
                }
            })
        })
        .collect();

    for producer in producers {
        producer.await.unwrap();
    }
    let mut all = Vec::new();
    for consumer in consumers {
        all.extend(consumer.await.unwrap());
    }
    let unique: HashSet<_> = all.iter().collect();
    assert_eq!(all.len(), PRODUCERS * PER_PRODUCER);
    assert_eq!(unique.len(), all.len());
    assert_eq!(engine.stats()[0].completed, all.len() as u64);
}
