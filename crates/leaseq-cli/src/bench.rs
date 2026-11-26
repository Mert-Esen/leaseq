//! A small throughput benchmark that drives the engine directly.

use std::time::{Duration, Instant};

use clap::Args;
use leaseq_engine::core::{JobSpec, Priority, QueueName};
use leaseq_engine::{Config, Engine, SyncPolicy};
use tokio::task::JoinSet;

#[derive(Debug, Args)]
pub(crate) struct BenchArgs {
    /// Jobs to push through the queue.
    #[arg(long, default_value_t = 20_000)]
    jobs: usize,
    /// Concurrent producers.
    #[arg(long, default_value_t = 64)]
    producers: usize,
    /// Concurrent consumers.
    #[arg(long, default_value_t = 64)]
    consumers: usize,
    /// Skip fsync.
    #[arg(long)]
    no_fsync: bool,
}

pub(crate) async fn run(args: BenchArgs) -> anyhow::Result<()> {
    let producers = args.producers.max(1);
    let consumers = args.consumers.max(1);
    let directory = tempfile::tempdir()?;
    let mut config = Config::new(directory.path());
    if args.no_fsync {
        config.sync = SyncPolicy::Never;
    }
    let engine = Engine::open(config)?;
    let queue: QueueName = "bench".parse()?;
    let sync = if args.no_fsync { "off" } else { "on" };
    println!(
        "{} jobs, {producers} producers, {consumers} consumers, fsync {sync}",
        args.jobs
    );

    let start = Instant::now();
    let mut tasks = JoinSet::new();
    for producer in 0..producers {
        let engine = engine.clone();
        let queue = queue.clone();
        let count = args.jobs / producers + usize::from(producer < args.jobs % producers);
        tasks.spawn(async move {
            let mut latencies = Vec::with_capacity(count);
            for _ in 0..count {
                let spec = JobSpec {
                    queue: queue.clone(),
                    payload: r#"{"to":"someone","template":"welcome"}"#.to_owned(),
                    priority: Priority::default(),
                    max_attempts: 3,
                };
                let sent = Instant::now();
                engine.enqueue(spec, Duration::ZERO).await?;
                latencies.push(sent.elapsed());
            }
            anyhow::Ok(latencies)
        });
    }
    let mut latencies = Vec::with_capacity(args.jobs);
    while let Some(result) = tasks.join_next().await {
        latencies.extend(result??);
    }
    report("enqueue", args.jobs, start.elapsed());
    latencies.sort_unstable();
    for percent in [50, 99] {
        if let Some(value) = latencies.get(latencies.len().saturating_sub(1) * percent / 100) {
            println!("  p{percent} latency  {value:.2?}");
        }
    }

    let start = Instant::now();
    let mut tasks = JoinSet::new();
    for _ in 0..consumers {
        let engine = engine.clone();
        let queue = queue.clone();
        tasks.spawn(async move {
            let ttl = Duration::from_secs(60);
            loop {
                let jobs = engine.lease(queue.clone(), 1, ttl, Duration::ZERO).await?;
                let Some(job) = jobs.first() else {
                    return anyhow::Ok(());
                };
                engine.ack(job.id, job.token).await?;
            }
        });
    }
    while let Some(result) = tasks.join_next().await {
        result??;
    }
    report("lease + ack", args.jobs, start.elapsed());

    engine.shutdown().await;
    Ok(())
}

fn report(name: &str, jobs: usize, elapsed: Duration) {
    #[allow(clippy::cast_precision_loss)] // job counts are far below 2^52
    let rate = jobs as f64 / elapsed.as_secs_f64();
    println!("{name:<12} {rate:>10.0} jobs/s  ({elapsed:.2?})");
}
